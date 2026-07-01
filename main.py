import os
import io
import re
import csv
import json
import hashlib
import logging
import asyncio
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# ── Auto-install missing dependencies ──────────────────────────────────────────
def _ensure_dependencies():
    """
    Attempts to import every third-party package used by this project.
    If any import fails, runs `pip install -r requirements.txt` once and exits
    so the process manager / user can restart with all deps in place.
    """
    _required = [
        "telethon", "motor", "apscheduler",
        "dotenv", "flask", "googleapiclient", "google.auth",
        "google_auth_oauthlib",
    ]
    missing = []
    for pkg in _required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(
            f"[startup] Missing packages detected: {missing}\n"
            f"[startup] Running: pip install -r requirements.txt"
        )
        req_file = os.path.join(os.path.dirname(__file__), "requirements.txt")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", req_file],
            check=False
        )
        if result.returncode != 0:
            print("[startup] pip install failed. Please install dependencies manually.")
            sys.exit(1)
        print("[startup] Dependencies installed. Please restart the bot.")
        sys.exit(0)

_ensure_dependencies()
# ───────────────────────────────────────────────────────────────────────────────

# Indian Standard Time: UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

# Custom logging formatter to use IST time
class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, IST)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S, %f")[:-3]

# Telethon
from telethon import TelegramClient, events, Button
from telethon.tl.types import Channel, Chat, User, DocumentAttributeFilename, PeerUser
from telethon.errors import MessageTooLongError

# Scheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Local modules
from config import API_ID, API_HASH, BOT_TOKEN, OWNERS, MONITORED_CHANNELS, db
from logs import channel_logger, bot_activity_logger, ocr_logger
from ocr import image_to_text
from progress import ProgressManager

# Setup logging
class TelethonWarningFilter(logging.Filter):
    """Filter out redundant Telethon warnings about persistent timestamp and history errors."""
    def filter(self, record):
        msg = record.getMessage()
        if "PersistentTimestampOutdatedError" in msg or "HistoryGetFailedError" in msg or "Persistent timestamp outdated" in msg:
            return False  # Filter out these warnings
        return True

root_handler = logging.StreamHandler()
root_handler.setFormatter(ISTFormatter("%(asctime)s - %(levelname)s - %(message)s"))
root_handler.addFilter(TelethonWarningFilter())  # Add our custom filter
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(root_handler)
logger = logging.getLogger(__name__)

# Also filter Telethon's own logger
telethon_logger = logging.getLogger("telethon")
telethon_logger.addFilter(TelethonWarningFilter())

# State Machine for conversational interactions
# Keys: user_id -> Dict containing 'action' and arbitrary metadata
USER_STATES = {}

# Helper function to clean text for Telegram (prevent invalid entity bounds errors)
def clean_text_for_telegram(text: str, max_length: int = 1024) -> str:
    """Clean text to avoid Telegram entity errors and truncate if too long"""
    if not text:
        return ""
    
    # Remove problematic Markdown characters that can cause entity errors
    # First, escape any remaining Markdown
    cleaned = text.replace("**", "").replace("*", "").replace("__", "").replace("_", "")
    cleaned = cleaned.replace("`", "").replace("```", "")
    
    # Truncate if too long
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length - 3] + "..."
    
    return cleaned


# =====================================================================
# SECURITY GUARD: AUTHORIZATION MIDDLEWARE
# =====================================================================
def is_authorized(sender_id: int) -> bool:
    if not sender_id:
        return False
    is_auth = sender_id in OWNERS
    if not is_auth:
        logger.warning(f"Unauthorized access attempt by user ID: {sender_id}")
    return is_auth

# =====================================================================
# INITIALIZATION & APP CREATION
# =====================================================================

# Bot client: handles commands, buttons, and sending alerts to owners
bot = TelegramClient(
    "market_intel_bot",
    api_id=API_ID,
    api_hash=API_HASH
)

# User client will be initialized in main() after loading session from DB
user = None  # type: TelegramClient

# =====================================================================
# HELPER DATA LAYER OPERATIONS
# =====================================================================
async def init_db_defaults():
    """Seeds foundational settings configuration components if absent."""
    doc = await db["config"].find_one({"_id": "bot_settings"})
    if not doc:
        await db["config"].insert_one({
            "_id": "bot_settings",
            "monitored_channels": MONITORED_CHANNELS,
            "ocr_channels": [],
            "user_session": ""
        })
    else:
        # Ensure user_session field exists
        if "user_session" not in doc:
            await db["config"].update_one({"_id": "bot_settings"}, {"$set": {"user_session": ""}})
        # Ensure ocr_channels field exists
        if "ocr_channels" not in doc:
            await db["config"].update_one({"_id": "bot_settings"}, {"$set": {"ocr_channels": []}})

    macro_doc = await db["config"].find_one({"_id": "macro_settings"})
    if not macro_doc:
        await db["config"].insert_one({
            "_id": "macro_settings",
            "macro_keywords": ["RBI", "Nifty", "Bank Nifty", "Budget", "Inflation", "Fed", "Interest Rate"]
        })

    universal_exclusions_doc = await db["config"].find_one({"_id": "universal_exclusions"})
    if not universal_exclusions_doc:
        await db["config"].insert_one({
            "_id": "universal_exclusions",
            "exclusions": []
        })

    # TTL index: hashes auto-expire after 12 hours so identical reposts within 12 hours are dropped
    try:
        # Drop existing index if it exists
        await db["recent_news_hashes"].drop_index("ts_1")
    except Exception:
        # Index doesn't exist, that's okay
        pass
    await db["recent_news_hashes"].create_index("ts", expireAfterSeconds=43200)

    # TTL index: OCR results auto-expire after 24 hours
    try:
        await db["ocr_results"].drop_index("ts_1")
    except Exception:
        pass
    await db["ocr_results"].create_index("ts", expireAfterSeconds=86400)

    # TTL index: Processed messages auto-expire after 24 hours
    try:
        await db["processed_messages"].drop_index("ts_1")
    except Exception:
        pass
    await db["processed_messages"].create_index("ts", expireAfterSeconds=86400)
    # Compound index for faster lookups by channel ID and message ID
    try:
        await db["processed_messages"].drop_index("channel_id_1_message_id_1")
    except Exception:
        pass
    await db["processed_messages"].create_index(["channel_id", "message_id"], unique=True)


async def is_message_processed(channel_id: int, message_id: int):
    """Check if a message has already been processed in a scan."""
    normalized_channel_id = normalize_channel_id(channel_id)
    return await db["processed_messages"].find_one({
        "channel_id": normalized_channel_id,
        "message_id": message_id
    }) is not None


async def mark_message_processed(channel_id: int, message_id: int):
    """Mark a message as processed so it's skipped in future scans."""
    normalized_channel_id = normalize_channel_id(channel_id)
    await db["processed_messages"].update_one(
        {"channel_id": normalized_channel_id, "message_id": message_id},
        {"$set": {"ts": datetime.now(IST)}},
        upsert=True
    )

async def get_cached_ocr_result(image_hash: str):
    """Get cached OCR result from DB if it exists and is less than 24h old."""
    result = await db["ocr_results"].find_one({"image_hash": image_hash})
    if result:
        return result
    return None

async def save_ocr_result_to_db(
    image_hash: str,
    extracted_text: str,
    deep_link: str,
    match_type: str = None,
    matched_entities: list = None,
    is_matched: bool = False
):
    """Save OCR result to DB with detailed info."""
    ocr_doc = {
        "image_hash": image_hash,
        "extracted_text": extracted_text,
        "deep_link": deep_link,
        "match_type": match_type,
        "matched_entities": matched_entities or [],
        "is_matched": is_matched,
        "ts": datetime.now(IST)
    }
    await db["ocr_results"].update_one(
        {"image_hash": image_hash},
        {"$set": ocr_doc},
        upsert=True
    )
    ocr_logger.info(f"Saved OCR result for image {deep_link} to DB (hash={image_hash[:10]}...)")

def extract_real_filename(original_message, fallback_entity_name: str) -> str:
    """
    Robustly extracts the true filename from a Telegram document message.
    Falls back gracefully if no structural name attribute is found.
    """
    logger.debug(f"[FILENAME EXTRACT] Starting extraction for entity: {fallback_entity_name}")
    
    filename = None
    
    # 1. Inspect Telegram Document Attributes securely
    if original_message and getattr(original_message, 'document', None):
        logger.debug(f"[FILENAME EXTRACT] Found document object!")
        if getattr(original_message.document, 'attributes', None):
            logger.debug(f"[FILENAME EXTRACT] Found {len(original_message.document.attributes)} attributes!")
            for i, attr in enumerate(original_message.document.attributes):
                logger.debug(f"[FILENAME EXTRACT] Attribute {i} type: {type(attr).__name__}, value: {attr}")
                # Explicitly match the filename attribute class
                if isinstance(attr, DocumentAttributeFilename):
                    filename = attr.file_name
                    logger.debug(f"[FILENAME EXTRACT] FOUND DOCUMENTATTRIBUTEFILENAME! Filename: {filename}")
                    break
        else:
            logger.debug(f"[FILENAME EXTRACT] Document has NO attributes!")
    else:
        logger.debug(f"[FILENAME EXTRACT] Original message has NO document!")
                    
    # 2. Cleanup extracted filename to remove any accidental path injections
    if filename:
        logger.debug(f"[FILENAME EXTRACT] Cleaning filename: {filename}")
        filename = os.path.basename(filename).strip()
        logger.debug(f"[FILENAME EXTRACT] Cleaned filename: {filename}")
        
    # 3. Apply your strict fallback strategy if the filename is missing or empty
    if not filename:
        logger.debug(f"[FILENAME EXTRACT] No filename found, using fallback!")
        timestamp = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
        # Clean entity name to make it a safe filesystem string
        safe_entity = "".join([c for c in fallback_entity_name if c.isalnum() or c in (' ', '_', '-')]).strip()
        filename = f"{safe_entity.upper()}_{timestamp}.pdf"
        logger.debug(f"[FILENAME EXTRACT] Fallback filename: {filename}")
        
    # Ensure it always terminates with a .pdf extension
    if not filename.lower().endswith('.pdf'):
        logger.debug(f"[FILENAME EXTRACT] Adding .pdf extension!")
        filename += '.pdf'
    
    logger.debug(f"[FILENAME EXTRACT] Final filename to use: {filename}")
    return filename

async def get_system_config():
    return await db["config"].find_one({"_id": "bot_settings"})

# =====================================================================
# TWO-TIER LOCAL FILTERING (ZERO TOKEN WASTE)
# =====================================================================
def extract_domains_from_text(text: str):
    """Extract all domain names from URLs in the given text."""
    # Regex pattern to find URLs and extract domain names
    url_pattern = re.compile(r'https?://(?:www\.)?([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+)', re.IGNORECASE)
    domains = set()
    
    for match in url_pattern.finditer(text):
        domain = match.group(1).lower()
        domains.add(domain)
        # Also add parent domain if there are subdomains (e.g., m.youtube.com → youtube.com)
        parts = domain.split('.')
        if len(parts) > 2:
            parent_domain = '.'.join(parts[-2:])
            domains.add(parent_domain)
    
    return domains

async def is_text_universally_excluded(text: str):
    """Check if text matches any universal exclusion (keyword or domain)."""
    universal_exclusions_doc = await db["config"].find_one({"_id": "universal_exclusions"})
    if not universal_exclusions_doc:
        return False, []
    
    exclusions = universal_exclusions_doc.get("exclusions", [])
    if not exclusions:
        return False, []
    
    normalized_text = text.lower()
    domains_in_text = extract_domains_from_text(text)
    matched_exclusions = []
    
    for exclusion in exclusions:
        exclusion_lower = exclusion.lower()
        # Check if it's a keyword match (word boundary)
        keyword_pattern = rf"\b{re.escape(exclusion_lower)}\b"
        if re.search(keyword_pattern, normalized_text):
            matched_exclusions.append(exclusion)
        # Check if it's a domain match
        elif exclusion_lower in domains_in_text:
            matched_exclusions.append(exclusion)
    
    return len(matched_exclusions) > 0, matched_exclusions

async def execute_two_tier_filter(text: str):
    """
    Evaluates incoming raw context payloads against local caches using word-boundary regex.
    Returns: (is_matched, match_type, match_details)
    match_details is a list of dicts, each with:
      - type: "stock" or "macro"
      - name: stock name or macro keyword
      - matched_positive: list of matched positive variants for stock
      - matched_exclusions: list of matched exclusion variants for stock (or empty for macro)
      - excluded: whether this match was excluded (True/False)
    match_type is "Portfolio Stock" if any portfolio stocks are matched and not excluded, else "Macro Economy" if any macro keywords matched
    """
    if not text:
        return False, "", []
    
    # First check universal exclusions
    is_excluded_by_universal, universal_matches = await is_text_universally_excluded(text)
    
    normalized_text = text.lower()
    
    # Tier 1: Check Specific Stock Portfolio Rules - Sort by longest positive variant first
    stocks = await db["portfolio"].find({}).to_list(length=None)
    stocks_sorted = sorted(
        stocks,
        key=lambda s: len(max(s.get("positive_variants", [s.get("stock_name", "")]), key=len)),
        reverse=True
    )
    
    all_match_details = []
    for stock in stocks_sorted:
        stock_name = stock.get("stock_name")
        positives = stock.get("positive_variants", [])
        exclusions = stock.get("exclusion_variants", [])
        
        # Look for positive matches using word-boundary regex
        matched_positive_list = []
        for variant in positives:
            pattern = rf"\b{re.escape(variant.lower())}\b"
            if re.search(pattern, normalized_text):
                matched_positive_list.append(variant)
        
        if matched_positive_list:
            # Check if any exclusions are present in the text (word boundaries)
            matched_exclusion_list = []
            for exc in exclusions:
                exc_pattern = rf"\b{re.escape(exc.lower())}\b"
                if re.search(exc_pattern, normalized_text):
                    matched_exclusion_list.append(exc)
            
            # Exclude if either stock-specific exclusions match OR universal exclusions match
            is_excluded = len(matched_exclusion_list) > 0 or is_excluded_by_universal
            if is_excluded_by_universal:
                matched_exclusion_list.extend(universal_matches)
            
            all_match_details.append({
                "type": "stock",
                "name": stock_name,
                "matched_positive": matched_positive_list,
                "matched_exclusions": matched_exclusion_list,
                "excluded": is_excluded
            })
    
    # Get non-excluded portfolio stocks
    matched_portfolio_stocks = [d["name"] for d in all_match_details if d["type"] == "stock" and not d["excluded"]]
    if matched_portfolio_stocks:
        return True, "Portfolio Stock", all_match_details
    
    # Tier 2: Check Global Macro Economy Keywords (also with word boundaries)
    macro_doc = await db["config"].find_one({"_id": "macro_settings"})
    if macro_doc:
        keywords = macro_doc.get("macro_keywords", [])
        for kw in keywords:
            kw_pattern = rf"\b{re.escape(kw.lower())}\b"
            if re.search(kw_pattern, normalized_text):
                all_match_details.append({
                    "type": "macro",
                    "name": kw.upper(),
                    "matched_positive": [kw.upper()],
                    "matched_exclusions": universal_matches if is_excluded_by_universal else [],
                    "excluded": is_excluded_by_universal
                })
    
    matched_macro_keywords = [d["name"] for d in all_match_details if d["type"] == "macro" and not d["excluded"]]
    if matched_macro_keywords:
        return True, "Macro Economy", all_match_details
                
    return False, "", all_match_details

# =====================================================================
# INTERACTIVE SETTINGS MECHANICS (UI GENERATOR)
# =====================================================================
def build_settings_keyboard() -> list:
    keyboard = [
        [
            Button.inline("📊 View Portfolio", data="view_portfolio"),
            Button.inline("➕ Add Stock", data="add_stock")
        ],
        [
            Button.inline("📡 Monitored Channels", data="view_channels"),
            Button.inline("🔍 OCR Channels", data="view_ocr_channels")
        ],
        [
            Button.inline("📥 Download CSV Portfolio", data="export_csv"),
        ],
        [
            Button.inline("📤 Upload CSV Bulk", data="prompt_upload_csv")
        ],
        [
            Button.inline("🌐 Macro Keywords", data="view_macro_keywords"),
            Button.inline("⛔ Universal Exclusions", data="view_universal_exclusions")
        ],
        [
            Button.inline("🔐 Manage User Session", data="manage_user_session")
        ],
        [
            Button.inline("📄 Google Drive Credentials", data="manage_google_creds")
        ]
    ]
    return keyboard

async def _settings_keyboard(config: dict) -> list:
    """Builds the settings keyboard."""
    return build_settings_keyboard()
async def start_command_handler(event: events.NewMessage.Event):
    await event.respond(
        "👋 **Welcome to Market Intelligence Bot!**\n\n"
        "This bot monitors Telegram channels for financial news and forwards them against your portfolio.\n\n"
        "Use /settings to configure the bot _(owners only)_."
    )

@bot.on(events.NewMessage(pattern="/help"))
async def help_command(event):
    user_id = event.sender_id
    if not is_authorized(user_id):
        await event.respond("Access Denied.")
        return
    
    help_text = (
        "📚 **Market Intelligence Terminal - Command List**\n\n"
        "**Core Commands:**\n"
        "/settings - Open settings menu (portfolio, channels, modes)\n"
        "/add_channel [link/id] - Add a new channel to monitor\n"
        "/remove_channel [link/id] - Stop monitoring a channel\n"
        "/add_ocr_channel [link/id] - Add a new channel to OCR list\n"
        "/remove_ocr_channel [link/id] - Stop OCR on a channel\n"
        "/scan_old_messages - Scan last 24h of monitored channels for missed messages\n"
        "/logs - Send today's activity and channel logs\n"
        "/help - Show this command list\n\n"
        "**Settings Menu Features:**\n"
        "• View/Add/Remove Portfolio Stocks\n"
        "• Import/Export Portfolio CSV\n"
        "• Add/Remove Macro Keywords\n"
        "• View/Manage Monitored Channels\n"
        "• View/Manage OCR Channels\n\n"
        "**How It Works:**\n"
        "1. Add portfolio stocks (or import CSV) and macro keywords\n"
        "2. Add channels to monitor\n"
        "3. The bot scans messages for matches and forwards them\n\n"
        "**CSV Format:**\n"
        "Columns: stock_name, positive_variants (comma-separated), exclusion_variants (comma-separated)\n"
    )
    await event.respond(help_text, link_preview=False)

@bot.on(events.NewMessage(pattern="/logs"))
async def logs_command_handler(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    
    # Get log file paths
    bot_activity_log_path = os.path.join("bot_activity_logs", "bot_activity.log")
    channel_log_path = os.path.join("channel_logs", "channel_activity.log")
    ocr_log_path = os.path.join("ocr_logs", "ocr_activity.log")
    
    files_sent = 0
    
    # Check and send bot activity log (and backups if any)
    log_files = [
        (bot_activity_log_path, "bot_activity.log", "🤖 Bot Activity Log"),
        (channel_log_path, "channel_activity.log", "📡 Channel Activity Log"),
        (ocr_log_path, "ocr_activity.log", "🖼️ OCR Activity Log")
    ]
    
    for log_path, log_name, caption in log_files:
        if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
            try:
                log_bytes = open(log_path, 'rb').read()
                log_bytesio = io.BytesIO(log_bytes)
                log_bytesio.name = log_name.replace('.log', '.txt')
                await bot.send_file(
                    event.sender_id,
                    log_bytesio,
                    caption=caption
                )
                files_sent += 1
            except Exception as e:
                logger.error(f"Failed to send {log_name}: {e}")
        
        # Also check for backup files (e.g., bot_activity.log.1)
        for i in range(1, 6):  # Check up to 5 backups
            backup_path = f"{log_path}.{i}"
            if os.path.exists(backup_path) and os.path.getsize(backup_path) > 0:
                try:
                    backup_bytes = open(backup_path, 'rb').read()
                    backup_bytesio = io.BytesIO(backup_bytes)
                    backup_bytesio.name = f"{log_name.replace('.log', '')}.{i}.txt"
                    await bot.send_file(
                        event.sender_id,
                        backup_bytesio,
                        caption=f"{caption} (Backup {i})"
                    )
                    files_sent += 1
                except Exception as e:
                    logger.error(f"Failed to send backup {i} for {log_name}: {e}")
    
    if files_sent == 0:
        await event.respond("📭 No log files available yet!")
    else:
        await event.respond(f"✅ Sent {files_sent} log file(s)!")

@bot.on(events.NewMessage(pattern="/settings"))
async def settings_command_handler(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
        
    config = await get_system_config()
    mode = config.get("input_mode", "Text + Images")
    
    await event.respond(
        "⚡️ **Market Intelligence Terminal Settings**\nConfigure metrics, balance keys, or change monitoring constraints below:",
        buttons=await _settings_keyboard(config)
    )

# =====================================================================
# UI CALLBACK DISPATCHER HANDLER
# =====================================================================
@bot.on(events.CallbackQuery())
async def callback_dispatcher(event: events.CallbackQuery.Event):
    data = event.data.decode('utf-8')
    user_id = event.sender_id

    # Global verification check for incoming interactions
    if not is_authorized(user_id):
        await event.answer("Access Denied.", alert=True)
        return

    config = await get_system_config()

    if data == "view_portfolio" or data.startswith("view_portfolio_page:"):
        page = int(data.split(":")[1]) if ":" in data else 0
        PAGE_SIZE = 10

        total = await db["portfolio"].count_documents({})
        stocks = await db["portfolio"].find({}).skip(page * PAGE_SIZE).limit(PAGE_SIZE).to_list(length=PAGE_SIZE)

        if not stocks:
            await event.edit("Portfolio is currently empty.", buttons=[[Button.inline("⬅️ Back", data="back_to_settings")]])
            return

        txt = f"📋 **Portfolio** (page {page + 1}/{-(-total // PAGE_SIZE)}) — {total} stocks\n\n"
        kbd = []
        
        for idx, s in enumerate(stocks):
            pos = ', '.join(s.get('positive_variants', []))
            exc = ', '.join(s.get('exclusion_variants', []))
            stock_button_label = f"• {s['stock_name']}"
            txt += f"• **{s['stock_name']}**\n"
            txt += f"  ✅ `{pos}`\n"
            if exc:
                txt += f"  ❌ `{exc}`\n"
            txt += "\n"
            # Add a button to manage this stock
            kbd.append([Button.inline(f"📝 Manage {s['stock_name']}", data=f"manage_stock:{s['_id']}")])

        nav = []
        if page > 0:
            nav.append(Button.inline("◀️ Prev", data=f"view_portfolio_page:{page - 1}"))
        if (page + 1) * PAGE_SIZE < total:
            nav.append(Button.inline("Next ▶️", data=f"view_portfolio_page:{page + 1}"))

        if nav:
            kbd.append(nav)
        kbd.append([Button.inline("⬅️ Back", data="back_to_settings")])
        try:
            await event.edit(txt, buttons=kbd)
        except MessageTooLongError:
            await event.answer("⚠️ Page content too long, try exporting CSV instead.", alert=True)
    
    elif data.startswith("manage_stock:"):
        stock_id = data.split(":")[1]
        # Import ObjectId from bson
        from bson import ObjectId
        stock = await db["portfolio"].find_one({"_id": ObjectId(stock_id)})
        if not stock:
            await event.answer("Stock not found.", alert=True)
            await event.edit("Portfolio is currently empty.", buttons=[[Button.inline("⬅️ Back", data="view_portfolio")]])
            return
        
        pos = ', '.join(stock.get('positive_variants', []))
        exc = ', '.join(stock.get('exclusion_variants', []))
        txt = f"📝 **Managing {stock['stock_name']}**\n\n"
        txt += f"✅ Positive variants: `{pos}`\n"
        txt += f"❌ Exclusion variants: `{exc}`\n\n"
        
        kbd = [
            [Button.inline("➕ Add Positive Variant", data=f"add_positive:{stock_id}")],
            [Button.inline("🗑️ Remove Positive Variant", data=f"remove_positive:{stock_id}")],
            [Button.inline("➕ Add Exclusion Variant", data=f"add_exclusion:{stock_id}")],
            [Button.inline("🗑️ Remove Exclusion Variant", data=f"remove_exclusion:{stock_id}")],
            [Button.inline("⚠️ Delete Stock", data=f"delete_stock:{stock_id}")],
            [Button.inline("⬅️ Back to Portfolio", data="view_portfolio")]
        ]
        await event.edit(txt, buttons=kbd)
    
    elif data.startswith("delete_stock:"):
        stock_id = data.split(":")[1]
        from bson import ObjectId
        result = await db["portfolio"].delete_one({"_id": ObjectId(stock_id)})
        if result.deleted_count:
            await event.answer("Stock deleted successfully!", alert=True)
        else:
            await event.answer("Stock not found.", alert=True)
        # Go back to portfolio view directly
        config = await get_system_config()
        page = 0
        PAGE_SIZE = 10
        total = await db["portfolio"].count_documents({})
        stocks = await db["portfolio"].find({}).skip(page * PAGE_SIZE).limit(PAGE_SIZE).to_list(length=PAGE_SIZE)

        if not stocks:
            await event.edit("Portfolio is currently empty.", buttons=[[Button.inline("⬅️ Back", data="back_to_settings")]])
            return

        txt = f"📋 **Portfolio** (page {page + 1}/{-(-total // PAGE_SIZE)}) — {total} stocks\n\n"
        kbd = []
        
        for idx, s in enumerate(stocks):
            pos = ', '.join(s.get('positive_variants', []))
            exc = ', '.join(s.get('exclusion_variants', []))
            txt += f"• **{s['stock_name']}**\n"
            txt += f"  ✅ `{pos}`\n"
            if exc:
                txt += f"  ❌ `{exc}`\n"
            txt += "\n"
            kbd.append([Button.inline(f"📝 Manage {s['stock_name']}", data=f"manage_stock:{s['_id']}")])

        nav = []
        if page > 0:
            nav.append(Button.inline("◀️ Prev", data=f"view_portfolio_page:{page - 1}"))
        if (page + 1) * PAGE_SIZE < total:
            nav.append(Button.inline("Next ▶️", data=f"view_portfolio_page:{page + 1}"))

        if nav:
            kbd.append(nav)
        kbd.append([Button.inline("⬅️ Back", data="back_to_settings")])
        try:
            await event.edit(txt, buttons=kbd)
        except MessageTooLongError:
            await event.answer("⚠️ Page content too long, try exporting CSV instead.", alert=True)
    
    elif data.startswith("add_positive:"):
        stock_id = data.split(":")[1]
        USER_STATES[user_id] = {"action": "AWAITING_POSITIVE_VARIANT", "stock_id": stock_id}
        await event.edit("📝 Enter the positive variant to add:", buttons=[[Button.inline("❌ Cancel", data="view_portfolio")]])
    
    elif data.startswith("remove_positive:"):
        stock_id = data.split(":")[1]
        from bson import ObjectId
        stock = await db["portfolio"].find_one({"_id": ObjectId(stock_id)})
        if not stock or not stock.get('positive_variants'):
            await event.answer("No positive variants to remove.", alert=True)
            # Go back to manage stock view
            pos = ', '.join(stock.get('positive_variants', [])) if stock else ''
            exc = ', '.join(stock.get('exclusion_variants', [])) if stock else ''
            txt = f"📝 **Managing {stock['stock_name']}**\n\n"
            txt += f"✅ Positive variants: `{pos}`\n"
            txt += f"❌ Exclusion variants: `{exc}`\n\n"
            
            kbd = [
                [Button.inline("➕ Add Positive Variant", data=f"add_positive:{stock_id}")],
                [Button.inline("🗑️ Remove Positive Variant", data=f"remove_positive:{stock_id}")],
                [Button.inline("➕ Add Exclusion Variant", data=f"add_exclusion:{stock_id}")],
                [Button.inline("🗑️ Remove Exclusion Variant", data=f"remove_exclusion:{stock_id}")],
                [Button.inline("⚠️ Delete Stock", data=f"delete_stock:{stock_id}")],
                [Button.inline("⬅️ Back to Portfolio", data="view_portfolio")]
            ]
            await event.edit(txt, buttons=kbd)
            return
        # Show buttons to select which variant to remove
        kbd = []
        for variant in stock['positive_variants']:
            kbd.append([Button.inline(f"🗑️ {variant}", data=f"confirm_remove_positive:{stock_id}:{variant}")])
        kbd.append([Button.inline("⬅️ Back", data=f"manage_stock:{stock_id}")])
        await event.edit(f"Select a positive variant to remove for **{stock['stock_name']}**:", buttons=kbd)
    
    elif data.startswith("confirm_remove_positive:"):
        stock_id, variant = data.split(":")[1], data.split(":")[2]
        from bson import ObjectId
        result = await db["portfolio"].update_one(
            {"_id": ObjectId(stock_id)},
            {"$pull": {"positive_variants": variant}}
        )
        if result.modified_count:
            await event.answer(f"Removed positive variant: {variant}", alert=True)
        else:
            await event.answer("Variant not found.", alert=True)
        # Go back to manage stock view
        stock = await db["portfolio"].find_one({"_id": ObjectId(stock_id)})
        if not stock:
            await event.answer("Stock not found.", alert=True)
            await event.edit("Portfolio is currently empty.", buttons=[[Button.inline("⬅️ Back", data="view_portfolio")]])
            return
        
        pos = ', '.join(stock.get('positive_variants', []))
        exc = ', '.join(stock.get('exclusion_variants', []))
        txt = f"📝 **Managing {stock['stock_name']}**\n\n"
        txt += f"✅ Positive variants: `{pos}`\n"
        txt += f"❌ Exclusion variants: `{exc}`\n\n"
        
        kbd = [
            [Button.inline("➕ Add Positive Variant", data=f"add_positive:{stock_id}")],
            [Button.inline("🗑️ Remove Positive Variant", data=f"remove_positive:{stock_id}")],
            [Button.inline("➕ Add Exclusion Variant", data=f"add_exclusion:{stock_id}")],
            [Button.inline("🗑️ Remove Exclusion Variant", data=f"remove_exclusion:{stock_id}")],
            [Button.inline("⚠️ Delete Stock", data=f"delete_stock:{stock_id}")],
            [Button.inline("⬅️ Back to Portfolio", data="view_portfolio")]
        ]
        await event.edit(txt, buttons=kbd)
    
    elif data.startswith("add_exclusion:"):
        stock_id = data.split(":")[1]
        USER_STATES[user_id] = {"action": "AWAITING_EXCLUSION_VARIANT", "stock_id": stock_id}
        await event.edit("📝 Enter the exclusion variant to add:", buttons=[[Button.inline("❌ Cancel", data="view_portfolio")]])
    
    elif data.startswith("remove_exclusion:"):
        stock_id = data.split(":")[1]
        from bson import ObjectId
        stock = await db["portfolio"].find_one({"_id": ObjectId(stock_id)})
        if not stock or not stock.get('exclusion_variants'):
            await event.answer("No exclusion variants to remove.", alert=True)
            # Go back to manage stock view
            pos = ', '.join(stock.get('positive_variants', [])) if stock else ''
            exc = ', '.join(stock.get('exclusion_variants', [])) if stock else ''
            txt = f"📝 **Managing {stock['stock_name']}**\n\n"
            txt += f"✅ Positive variants: `{pos}`\n"
            txt += f"❌ Exclusion variants: `{exc}`\n\n"
            
            kbd = [
                [Button.inline("➕ Add Positive Variant", data=f"add_positive:{stock_id}")],
                [Button.inline("🗑️ Remove Positive Variant", data=f"remove_positive:{stock_id}")],
                [Button.inline("➕ Add Exclusion Variant", data=f"add_exclusion:{stock_id}")],
                [Button.inline("🗑️ Remove Exclusion Variant", data=f"remove_exclusion:{stock_id}")],
                [Button.inline("⚠️ Delete Stock", data=f"delete_stock:{stock_id}")],
                [Button.inline("⬅️ Back to Portfolio", data="view_portfolio")]
            ]
            await event.edit(txt, buttons=kbd)
            return
        # Show buttons to select which variant to remove
        kbd = []
        for variant in stock['exclusion_variants']:
            kbd.append([Button.inline(f"🗑️ {variant}", data=f"confirm_remove_exclusion:{stock_id}:{variant}")])
        kbd.append([Button.inline("⬅️ Back", data=f"manage_stock:{stock_id}")])
        await event.edit(f"Select an exclusion variant to remove for **{stock['stock_name']}**:", buttons=kbd)
    
    elif data.startswith("confirm_remove_exclusion:"):
        stock_id, variant = data.split(":")[1], data.split(":")[2]
        from bson import ObjectId
        result = await db["portfolio"].update_one(
            {"_id": ObjectId(stock_id)},
            {"$pull": {"exclusion_variants": variant}}
        )
        if result.modified_count:
            await event.answer(f"Removed exclusion variant: {variant}", alert=True)
        else:
            await event.answer("Variant not found.", alert=True)
        # Go back to manage stock view
        stock = await db["portfolio"].find_one({"_id": ObjectId(stock_id)})
        if not stock:
            await event.answer("Stock not found.", alert=True)
            await event.edit("Portfolio is currently empty.", buttons=[[Button.inline("⬅️ Back", data="view_portfolio")]])
            return
        
        pos = ', '.join(stock.get('positive_variants', []))
        exc = ', '.join(stock.get('exclusion_variants', []))
        txt = f"📝 **Managing {stock['stock_name']}**\n\n"
        txt += f"✅ Positive variants: `{pos}`\n"
        txt += f"❌ Exclusion variants: `{exc}`\n\n"
        
        kbd = [
            [Button.inline("➕ Add Positive Variant", data=f"add_positive:{stock_id}")],
            [Button.inline("🗑️ Remove Positive Variant", data=f"remove_positive:{stock_id}")],
            [Button.inline("➕ Add Exclusion Variant", data=f"add_exclusion:{stock_id}")],
            [Button.inline("🗑️ Remove Exclusion Variant", data=f"remove_exclusion:{stock_id}")],
            [Button.inline("⚠️ Delete Stock", data=f"delete_stock:{stock_id}")],
            [Button.inline("⬅️ Back to Portfolio", data="view_portfolio")]
        ]
        await event.edit(txt, buttons=kbd)

    elif data == "add_stock":
        USER_STATES[user_id] = {"action": "AWAITING_STOCK_NAME"}
        await event.edit(
            "📝 Enter the **Stock Name** you wish to onboard (e.g., `Reliance Industries`):",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )



    elif data == "manage_user_session":
        user_session = config.get("user_session", "")
        if user_session:
            # Mask the session string for security
            masked_session = user_session[:20] + "..." + user_session[-10:]
            txt = f"🔐 **Manage User Session**\n\nCurrent session (masked):\n```\n{masked_session}\n```"
        else:
            txt = "🔐 **Manage User Session**\n\nNo user session set yet."
        
        kbd = [
            [Button.inline("➕ Set User Session", data="set_user_session_prompt")],
            [Button.inline("🗑️ Clear User Session", data="clear_user_session")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)
    
    elif data == "set_user_session_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_USER_SESSION"}
        await event.edit(
            "🔐 Send your **Telethon user string session** (just the session string, nothing else):",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )
    
    elif data == "clear_user_session":
        await db["config"].update_one({"_id": "bot_settings"}, {"$set": {"user_session": ""}})
        await event.answer("User session cleared. Please restart the bot for changes to take effect.", alert=True)
        await event.edit("Cleared.", buttons=await _settings_keyboard(config))
    
    elif data == "manage_google_creds":
        # Get current Google Drive credentials from DB
        google_creds = await db["config"].find_one({"_id": "google_drive_creds"})
        txt = "📄 **Google Drive Credentials**\n\n"
        if google_creds:
            if google_creds.get("service_account"):
                txt += "✅ Service account credentials found in database (recommended, no token refresh needed!)\n"
            elif google_creds.get("token"):
                txt += "✅ OAuth credentials and token found in database.\n"
            elif google_creds.get("credentials"):
                txt += "✅ OAuth credentials found in database, but no token.\n"
            else:
                txt += "❌ No valid credentials found in database.\n"
        else:
            txt += "❌ No credentials found in database.\n"
        txt += "\n💡 **Tip**: Using a Service Account is **strongly recommended** - it doesn't require token refreshes and doesn't need any redirect URI setup! Just upload the service account JSON key file.\n"
        kbd = []
        if google_creds and google_creds.get("service_account"):
            kbd.append([Button.inline("📥 Download service account key", data="download_service_account")])
        if google_creds and google_creds.get("credentials"):
            kbd.append([Button.inline("📥 Download OAuth credentials.json", data="download_google_creds")])
        if google_creds and google_creds.get("token"):
            kbd.append([Button.inline("📥 Download OAuth token.json", data="download_google_token")])
        kbd.extend([
            [Button.inline("📤 Upload Service Account Key (Recommended)", data="upload_service_account")],
            [Button.inline("📤 Upload OAuth credentials.json", data="upload_google_creds")],
            [Button.inline("📤 Upload OAuth token.json", data="upload_google_token")],
        ])
        # Add option to start OAuth flow if credentials are present
        if google_creds and google_creds.get("credentials"):
            kbd.append([Button.inline("🔗 Start OAuth Authentication Flow (Phone)", data="start_oauth_flow")])
        kbd.append([Button.inline("⬅️ Back", data="back_to_settings")])
        await event.edit(txt, buttons=kbd)
    
    elif data == "download_google_creds":
        # Get current Google Drive credentials from DB
        google_creds = await db["config"].find_one({"_id": "google_drive_creds"})
        if not google_creds or not google_creds.get("credentials"):
            await event.answer("No credentials found to download.", alert=True)
            return
        
        import io
        import json
        creds_bytes = json.dumps(google_creds["credentials"], indent=2).encode("utf-8")
        creds_bytesio = io.BytesIO(creds_bytes)
        creds_bytesio.name = "credentials.json"
        
        await bot.send_file(event.sender_id, creds_bytesio, caption="📄 Your Google Drive credentials.json")
        await event.answer("Download sent!", alert=True)
    
    elif data == "download_service_account":
        # Get current Google Drive service account from DB
        google_creds = await db["config"].find_one({"_id": "google_drive_creds"})
        if not google_creds or not google_creds.get("service_account"):
            await event.answer("No service account key found to download.", alert=True)
            return
        
        import io
        import json
        sa_bytes = json.dumps(google_creds["service_account"], indent=2).encode("utf-8")
        sa_bytesio = io.BytesIO(sa_bytes)
        sa_bytesio.name = "service_account.json"
        
        await bot.send_file(event.sender_id, sa_bytesio, caption="📄 Your Google Drive service account key")
        await event.answer("Download sent!", alert=True)
    
    elif data == "download_google_token":
        # Get current Google Drive token from DB
        google_creds = await db["config"].find_one({"_id": "google_drive_creds"})
        if not google_creds or not google_creds.get("token"):
            await event.answer("No token found to download.", alert=True)
            return
        
        import io
        import json
        token_bytes = json.dumps(google_creds["token"], indent=2).encode("utf-8")
        token_bytesio = io.BytesIO(token_bytes)
        token_bytesio.name = "token.json"
        
        await bot.send_file(event.sender_id, token_bytesio, caption="📄 Your Google Drive token.json")
        await event.answer("Download sent!", alert=True)
    
    elif data == "upload_google_creds":
        USER_STATES[user_id] = {"action": "AWAITING_GOOGLE_CREDS"}
        await event.edit("📤 Please upload your credentials.json file (from Google Cloud Console):", buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]])
    
    elif data == "start_oauth_flow":
        # Get Google OAuth credentials from DB
        google_creds = await db["config"].find_one({"_id": "google_drive_creds"})
        if not google_creds or not google_creds.get("credentials"):
            await event.answer("Please upload OAuth credentials.json first!", alert=True)
            return
        
        # Import necessary modules
        from google_auth_oauthlib.flow import InstalledAppFlow
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        
        try:
            # Create flow with the special out-of-band redirect URI
            flow = InstalledAppFlow.from_client_config(google_creds["credentials"], SCOPES)
            flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"  # This is the special redirect URI for manual code flow
            
            # Generate authorization URL
            auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline", include_granted_scopes="true")
            
            # Store the flow in user state (we'll need to recreate it later with the code, but let's at least store that we're in the flow)
            USER_STATES[user_id] = {"action": "AWAITING_OAUTH_CODE"}
            
            # Send the auth URL to the user
            await event.edit(
                f"🔗 **OAuth Authentication Flow Started!**\n\n"
                f"⚠️ **Important**: First, make sure you've added `urn:ietf:wg:oauth:2.0:oob` as an authorized redirect URI in your Google Cloud Console!\n\n"
                f"Please click the link below to authenticate with your Google account on your phone browser:\n"
                f"{auth_url}\n\n"
                f"After authenticating, you will get an authorization code. Please send that code here to complete the process.",
                buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
            )
        except Exception as e:
            await event.answer(f"Failed to start OAuth flow: {e}", alert=True)
    
    elif data == "upload_service_account":
        USER_STATES[user_id] = {"action": "AWAITING_SERVICE_ACCOUNT"}
        await event.edit("📤 Please upload your service account JSON key file (from Google Cloud Console - recommended, no token refresh needed!):", buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]])
    
    elif data == "upload_google_token":
        USER_STATES[user_id] = {"action": "AWAITING_GOOGLE_TOKEN"}
        await event.edit("📤 Please upload your token.json file (authenticated Google Drive token):", buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]])



    elif data == "view_channels":
        ch_list = config.get("monitored_channels", [])
        txt = "📡 **Monitored Channels:**\n\n"
        if not ch_list:
            txt += "No channels monitored yet."
        else:
            for idx, ch in enumerate(ch_list):
                label = ch.get("label", str(ch.get("id", ch)))
                cid   = ch.get("id", ch)
                txt  += f"{idx+1}. `{label}` — ID: `{cid}`\n"
        txt += "\nSend `/add_channel <link or @username>` to add, or `/remove_channel <id>` to remove."
        kbd = [
            [Button.inline("➕ Add Channel", data="add_channel_prompt")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)
    elif data == "view_ocr_channels":
        ocr_ch_list = config.get("ocr_channels", [])
        txt = "🔍 **OCR Channels:**\n\n"
        if not ocr_ch_list:
            txt += "No OCR channels configured yet."
        else:
            for idx, ch in enumerate(ocr_ch_list):
                label = ch.get("label", str(ch.get("id", ch)))
                cid   = ch.get("id", ch)
                txt  += f"{idx+1}. `{label}` — ID: `{cid}`\n"
        txt += "\nSend `/add_ocr_channel <link or @username>` to add, or `/remove_ocr_channel <id>` to remove."
        kbd = [
            [Button.inline("➕ Add OCR Channel", data="add_ocr_channel_prompt")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)
    elif data == "add_ocr_channel_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_OCR_CHANNEL_INPUT"}
        await event.edit(
            "🔍 Send the channel **@username**, **invite link** (`https://t.me/...`), or **numeric ID** to add as OCR channel:",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "add_channel_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_CHANNEL_INPUT"}
        await event.edit(
            "📡 Send the channel **@username**, **invite link** (`https://t.me/...`), or **numeric ID**:",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "export_csv":
        await event.answer("Generating CSV dataset...")
        stocks = await db["portfolio"].find({}).to_list(length=1000)
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["stock_name", "positive_variants", "exclusion_variants"])
        
        for s in stocks:
            writer.writerow([
                s.get("stock_name"),
                ",".join(s.get("positive_variants", [])),
                ",".join(s.get("exclusion_variants", []))
            ])
            
        output.seek(0)
        bytes_io = io.BytesIO(output.getvalue().encode('utf-8'))
        bytes_io.name = f"portfolio_export_{datetime.now(IST).strftime('%Y%m%d')}.csv"
        
        await bot.send_file(event.chat_id, file=bytes_io, caption="📊 Current system structural portfolio rules metadata mapping.")

    elif data == "prompt_upload_csv":
        USER_STATES[user_id] = {"action": "AWAITING_CSV_FILE"}
        await event.edit(
            "📤 Please upload/send a clean `.csv` file containing target entities names column headers (`stock_name`).",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "back_to_settings":
        USER_STATES.pop(user_id, None)
        await event.edit(
            "⚡️ **Market Intelligence Terminal Settings**\nConfigure metrics, balance keys, or change monitoring constraints below:",
            buttons=await _settings_keyboard(config)
        )

    elif data == "view_macro_keywords":
        macro_doc = await db["config"].find_one({"_id": "macro_settings"})
        keywords = macro_doc.get("macro_keywords", []) if macro_doc else []
        txt = "🌐 **Macro Keywords**\n\n"
        if not keywords:
            txt += "No macro keywords added yet.\n"
        else:
            for i, kw in enumerate(keywords, 1):
                txt += f"{i}. `{kw}`\n"
        txt += "\n"
        kbd = [
            [Button.inline("➕ Add Keyword", data="add_macro_keyword")],
            [Button.inline("🗑️ Delete Keyword", data="delete_macro_keyword_prompt")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)

    elif data == "add_macro_keyword":
        USER_STATES[user_id] = {"action": "AWAITING_MACRO_KEYWORD"}
        await event.edit(
            "🌐 Send the macro keyword you want to add (e.g., 'Crude Oil', 'Fed Rate'):",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "delete_macro_keyword_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_MACRO_KEYWORD_DELETE"}
        await event.edit(
            "🗑️ Send the macro keyword you want to delete:",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "view_universal_exclusions":
        universal_exclusions_doc = await db["config"].find_one({"_id": "universal_exclusions"})
        exclusions = universal_exclusions_doc.get("exclusions", []) if universal_exclusions_doc else []
        txt = "⛔ **Universal Exclusions**\n\n"
        if not exclusions:
            txt += "No universal exclusions added yet.\n"
        else:
            for i, ex in enumerate(exclusions, 1):
                txt += f"{i}. `{ex}`\n"
        txt += "\n"
        kbd = [
            [Button.inline("➕ Add Exclusion", data="add_universal_exclusion")],
            [Button.inline("🗑️ Delete Exclusion", data="delete_universal_exclusion_prompt")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)

    elif data == "add_universal_exclusion":
        USER_STATES[user_id] = {"action": "AWAITING_UNIVERSAL_EXCLUSION"}
        await event.edit(
            "⛔ Send the universal exclusion you want to add (keyword or domain name, e.g., 'spam', 'youtube.com'):",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "delete_universal_exclusion_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_UNIVERSAL_EXCLUSION_DELETE"}
        await event.edit(
            "🗑️ Send the universal exclusion you want to delete:",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

# =====================================================================
# CONVERSATIONAL STATE INPUT PROCESSING
# =====================================================================
@bot.on(events.NewMessage(func=lambda e: e.is_private))
async def functional_input_processor(event: events.NewMessage.Event):
    user_id = event.sender_id
    if not is_authorized(user_id):
        return

    # Skip if it is a standard menu command
    if event.text.startswith(("/settings", "/start", "/add_channel", "/remove_channel")):
        return

    state = USER_STATES.get(user_id)
    if not state:
        return

    if state["action"] == "AWAITING_STOCK_NAME":
        stock_name = event.text.strip()
        if not stock_name:
            return
            
        await db["portfolio"].update_one(
            {"stock_name": stock_name},
            {"$set": {
                "stock_name": stock_name,
                "positive_variants": [stock_name],
                "exclusion_variants": []
            }},
            upsert=True
        )
        
        USER_STATES.pop(user_id, None)
        await event.respond(
            f"✅ **Added Stock!** Monitoring for `{stock_name}`."
        )

    elif state["action"] == "AWAITING_USER_SESSION":
        text = event.text.strip()
        try:
            user_session = text.strip()
            if not user_session:
                await event.respond("❌ User session cannot be empty.")
                return

            await db["config"].update_one(
                {"_id": "bot_settings"},
                {"$set": {"user_session": user_session}}
            )
            USER_STATES.pop(user_id, None)
            await event.respond(f"✅ User session saved successfully (masked: `{user_session[:20]}...{user_session[-10:]}`). Please restart the bot for changes to take effect.")
        except Exception as e:
            await event.respond(f"❌ Failed to save user session: {e}")

    elif state["action"] == "AWAITING_CHANNEL_INPUT":
        if not user:
            USER_STATES.pop(user_id, None)
            await event.respond("❌ User session not configured. Please set user session via /settings first.")
            return
        raw = event.text.strip()
        processing_msg = await event.respond("🔍 Resolving channel...")
        try:
            channel_id, label = await _resolve_channel_id(raw)

            # Check if already monitored
            existing = await db["config"].find_one({"_id": "bot_settings", "monitored_channels.id": channel_id})
            if existing:
                await bot.delete_messages(event.chat_id, processing_msg.id)
                USER_STATES.pop(user_id, None)
                await event.respond(f"⚠️ `{label}` is already in the monitored list.")
                return

            await db["config"].update_one(
                {"_id": "bot_settings"},
                {"$push": {"monitored_channels": {"id": channel_id, "label": label}}}
            )
            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond(f"✅ Now monitoring **{label}** (`{channel_id}`)")

        except Exception as e:
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond(f"❌ Could not resolve channel: `{e}`\n\nMake sure the bot is a member of the channel, or try the numeric ID.")
    elif state["action"] == "AWAITING_OCR_CHANNEL_INPUT":
        if not user:
            USER_STATES.pop(user_id, None)
            await event.respond("❌ User session not configured. Please set user session via /settings first.")
            return
        raw = event.text.strip()
        processing_msg = await event.respond("🔍 Resolving channel...")
        try:
            channel_id, label = await _resolve_channel_id(raw)

            # Check if already in OCR channels
            existing = await db["config"].find_one({"_id": "bot_settings", "ocr_channels.id": channel_id})
            if existing:
                await bot.delete_messages(event.chat_id, processing_msg.id)
                USER_STATES.pop(user_id, None)
                await event.respond(f"⚠️ `{label}` is already in the OCR channels list.")
                return

            await db["config"].update_one(
                {"_id": "bot_settings"},
                {"$push": {"ocr_channels": {"id": channel_id, "label": label}}}
            )
            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond(f"✅ Now performing OCR on **{label}** (`{channel_id}`)")

        except Exception as e:
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond(f"❌ Could not resolve channel: `{e}`\n\nMake sure the bot is a member of the channel, or try the numeric ID.")

    elif state["action"] == "AWAITING_CSV_FILE":
        if not event.document:
            await event.respond("Please provide a real structured file asset document.")
            return

        processing_msg = await event.respond("📥 Parsing CSV...")
        file_path = await bot.download_media(event.document)

        added = 0
        updated = 0
        skipped = 0
        try:
            # Fetch all existing stocks in one query (including their variants)
            existing_docs = await db["portfolio"].find({}).to_list(length=10000)
            existing_lookup = {d["stock_name"].lower(): d for d in existing_docs}

            rows_to_insert = []
            updates_to_apply = []
            # Try multiple encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'utf-16']
            csv_content = None
            for enc in encodings:
                try:
                    with open(file_path, mode='r', encoding=enc) as f:
                        csv_content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if csv_content is None:
                raise Exception("Could not decode CSV file with any of the supported encodings (utf-8, latin-1, cp1252, utf-16)")
            
            # Now read the CSV content with the successful encoding
            import io
            f = io.StringIO(csv_content)
            reader = csv.DictReader(f)
            for row in reader:
                s_name = row.get("stock_name", "").strip()
                if not s_name:
                    continue

                # Use variants from CSV if the columns exist (re-import of an export),
                # otherwise fall back to stock_name as the only positive variant.
                raw_pos = row.get("positive_variants", "").strip()
                raw_exc = row.get("exclusion_variants", "").strip()

                positive_variants = [v.strip() for v in raw_pos.split(",") if v.strip()] if raw_pos else [s_name]
                exclusion_variants = [v.strip() for v in raw_exc.split(",") if v.strip()] if raw_exc else []

                # Ensure stock_name itself is always in positive_variants
                if s_name not in positive_variants:
                    positive_variants.insert(0, s_name)

                if s_name.lower() in existing_lookup:
                    # Check if variants are different before updating
                    existing = existing_lookup[s_name.lower()]
                    existing_pos = sorted([v.lower() for v in existing.get("positive_variants", [])])
                    existing_exc = sorted([v.lower() for v in existing.get("exclusion_variants", [])])
                    new_pos = sorted([v.lower() for v in positive_variants])
                    new_exc = sorted([v.lower() for v in exclusion_variants])
                    
                    if existing_pos != new_pos or existing_exc != new_exc:
                        # Variants changed, prepare update
                        updates_to_apply.append({
                            "filter": {"_id": existing["_id"]},
                            "update": {
                                "$set": {
                                    "positive_variants": positive_variants,
                                    "exclusion_variants": exclusion_variants
                                }
                            }
                        })
                        updated += 1
                    else:
                        # No changes, skip
                        skipped += 1
                else:
                    # New stock, add to insert list
                    rows_to_insert.append({
                        "stock_name": s_name,
                        "positive_variants": positive_variants,
                        "exclusion_variants": exclusion_variants
                    })
                    added += 1

            # Perform bulk operations
            if rows_to_insert:
                await db["portfolio"].insert_many(rows_to_insert)
            if updates_to_apply:
                for update in updates_to_apply:
                    await db["portfolio"].update_one(update["filter"], update["update"])

            await event.respond(
                f"✅ CSV import complete.\n"
                f"• Added: **{added}** stocks\n"
                f"• Updated: **{updated}** stocks\n"
                f"• Skipped (no changes): **{skipped}** stocks\n"
            )
        except Exception as err:
            await event.respond(f"❌ Error processing CSV: `{err}`")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)

    elif state["action"] == "AWAITING_POSITIVE_VARIANT":
        variant = event.text.strip()
        if not variant:
            return
        from bson import ObjectId
        stock_id = state["stock_id"]
        # Add the variant, ensuring it's not duplicate
        result = await db["portfolio"].update_one(
            {"_id": ObjectId(stock_id)},
            {"$addToSet": {"positive_variants": variant}}
        )
        USER_STATES.pop(user_id, None)
        if result.modified_count:
            await event.respond(f"✅ Added positive variant: `{variant}`")
        else:
            await event.respond(f"⚠️ Positive variant `{variant}` already exists for this stock.")
        
        # Navigate back to stock management
        await callback_dispatcher(event)
    
    elif state["action"] == "AWAITING_EXCLUSION_VARIANT":
        variant = event.text.strip()
        if not variant:
            return
        from bson import ObjectId
        stock_id = state["stock_id"]
        # Add the variant, ensuring it's not duplicate
        result = await db["portfolio"].update_one(
            {"_id": ObjectId(stock_id)},
            {"$addToSet": {"exclusion_variants": variant}}
        )
        USER_STATES.pop(user_id, None)
        if result.modified_count:
            await event.respond(f"✅ Added exclusion variant: `{variant}`")
        else:
            await event.respond(f"⚠️ Exclusion variant `{variant}` already exists for this stock.")
        
        # Navigate back to stock management
        await callback_dispatcher(event)
    
    elif state["action"] == "AWAITING_MACRO_KEYWORD":
        keyword = event.text.strip()
        if not keyword:
            return
        
        # Ensure macro settings document exists
        await db["config"].update_one(
            {"_id": "macro_settings"},
            {"$setOnInsert": {"macro_keywords": []}},
            upsert=True
        )
        
        # Check if keyword already exists (case-insensitive)
        macro_doc = await db["config"].find_one({"_id": "macro_settings"})
        existing_keywords = macro_doc.get("macro_keywords", []) if macro_doc else []
        
        if keyword.lower() in [kw.lower() for kw in existing_keywords]:
            await event.respond(f"⚠️ `{keyword}` is already in the macro keywords list.")
            USER_STATES.pop(user_id, None)
            return
        
        # Add the new keyword
        await db["config"].update_one(
            {"_id": "macro_settings"},
            {"$push": {"macro_keywords": keyword}}
        )
        
        USER_STATES.pop(user_id, None)
        await event.respond(f"✅ Added macro keyword: `{keyword}`")

    elif state["action"] == "AWAITING_MACRO_KEYWORD_DELETE":
        keyword = event.text.strip()
        if not keyword:
            return
        
        # Try to remove the keyword (case-insensitive)
        macro_doc = await db["config"].find_one({"_id": "macro_settings"})
        existing_keywords = macro_doc.get("macro_keywords", []) if macro_doc else []
        
        # Find the exact keyword that matches (case-insensitive)
        keyword_to_remove = None
        for kw in existing_keywords:
            if kw.lower() == keyword.lower():
                keyword_to_remove = kw
                break
        
        if not keyword_to_remove:
            await event.respond(f"⚠️ `{keyword}` not found in macro keywords list.")
            USER_STATES.pop(user_id, None)
            return
        
        await db["config"].update_one(
            {"_id": "macro_settings"},
            {"$pull": {"macro_keywords": keyword_to_remove}}
        )
        
        USER_STATES.pop(user_id, None)
        await event.respond(f"🗑️ Removed macro keyword: `{keyword_to_remove}`")
    
    elif state["action"] == "AWAITING_UNIVERSAL_EXCLUSION":
        exclusion = event.text.strip()
        if not exclusion:
            return
        
        # Ensure universal exclusions document exists
        await db["config"].update_one(
            {"_id": "universal_exclusions"},
            {"$setOnInsert": {"exclusions": []}},
            upsert=True
        )
        
        # Check if exclusion already exists (case-insensitive)
        universal_exclusions_doc = await db["config"].find_one({"_id": "universal_exclusions"})
        existing_exclusions = universal_exclusions_doc.get("exclusions", []) if universal_exclusions_doc else []
        
        if exclusion.lower() in [ex.lower() for ex in existing_exclusions]:
            await event.respond(f"⚠️ `{exclusion}` is already in the universal exclusions list.")
            USER_STATES.pop(user_id, None)
            return
        
        # Add the new exclusion
        await db["config"].update_one(
            {"_id": "universal_exclusions"},
            {"$push": {"exclusions": exclusion}}
        )
        
        USER_STATES.pop(user_id, None)
        await event.respond(f"✅ Added universal exclusion: `{exclusion}`")
    
    elif state["action"] == "AWAITING_UNIVERSAL_EXCLUSION_DELETE":
        exclusion = event.text.strip()
        if not exclusion:
            return
        
        # Try to remove the exclusion (case-insensitive)
        universal_exclusions_doc = await db["config"].find_one({"_id": "universal_exclusions"})
        existing_exclusions = universal_exclusions_doc.get("exclusions", []) if universal_exclusions_doc else []
        
        # Find the exact exclusion that matches (case-insensitive)
        exclusion_to_remove = None
        for ex in existing_exclusions:
            if ex.lower() == exclusion.lower():
                exclusion_to_remove = ex
                break
        
        if not exclusion_to_remove:
            await event.respond(f"⚠️ `{exclusion}` not found in universal exclusions list.")
            USER_STATES.pop(user_id, None)
            return
        
        await db["config"].update_one(
            {"_id": "universal_exclusions"},
            {"$pull": {"exclusions": exclusion_to_remove}}
        )
        
        USER_STATES.pop(user_id, None)
        await event.respond(f"🗑️ Removed universal exclusion: `{exclusion_to_remove}`")
    
    elif state["action"] == "AWAITING_GOOGLE_CREDS":
        if not event.document:
            await event.respond("Please upload a valid JSON file.")
            return
        
        processing_msg = await event.respond("📥 Parsing credentials.json...")
        file_path = await bot.download_media(event.document)
        
        try:
            import json
            with open(file_path, "r", encoding="utf-8") as f:
                creds_data = json.load(f)
            
            # Validate that it's a valid Google Cloud credentials file
            if "web" not in creds_data and "installed" not in creds_data:
                raise Exception("Invalid credentials.json file. Please provide a valid Google Cloud OAuth 2.0 client ID file.")
            
            # Save to database
            await db["config"].update_one(
                {"_id": "google_drive_creds"},
                {"$set": {
                    "credentials": creds_data,
                    "already_notified_auth_issue": False  # Reset notification flag
                }},
                upsert=True
            )

            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond("✅ Google Drive credentials saved successfully!")
            
        except Exception as e:
            await event.respond(f"❌ Failed to parse credentials: {e}")
        finally:
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
    
    elif state["action"] == "AWAITING_OAUTH_CODE":
        auth_code = event.text.strip()
        if not auth_code:
            return
        
        processing_msg = await event.respond("🔄 Exchanging authorization code for tokens...")
        
        try:
            # Get Google OAuth credentials from DB
            google_creds = await db["config"].find_one({"_id": "google_drive_creds"})
            if not google_creds or not google_creds.get("credentials"):
                raise Exception("OAuth credentials not found in database.")
            
            # Import necessary modules
            from google_auth_oauthlib.flow import InstalledAppFlow
            SCOPES = ["https://www.googleapis.com/auth/drive"]
            import json
            
            # Recreate the flow and exchange the code for tokens
            flow = InstalledAppFlow.from_client_config(google_creds["credentials"], SCOPES)
            flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"  # Same redirect URI as before
            creds = flow.fetch_token(code=auth_code)
            
            # Convert credentials to a serializable dict
            from google.oauth2.credentials import Credentials
            credentials_obj = Credentials(
                token=creds["access_token"],
                refresh_token=creds.get("refresh_token"),
                token_uri=flow.client_config["token_uri"],
                client_id=flow.client_config["client_id"],
                client_secret=flow.client_config["client_secret"],
                scopes=SCOPES
            )
            token_data = json.loads(credentials_obj.to_json())
            
            # Save to database
            await db["config"].update_one(
                {"_id": "google_drive_creds"},
                {"$set": {
                    "token": token_data,
                    "already_notified_auth_issue": False  # Reset notification flag
                }},
                upsert=True
            )

            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond("✅ Google Drive OAuth token obtained and saved successfully!")
            
        except Exception as e:
            await event.respond(f"❌ Failed to exchange authorization code: {e}\n\nPlease make sure you entered the correct code from the Google authentication page.")
    
    elif state["action"] == "AWAITING_SERVICE_ACCOUNT":
        if not event.document:
            await event.respond("Please upload a valid JSON file.")
            return
        
        processing_msg = await event.respond("📥 Parsing service account key...")
        file_path = await bot.download_media(event.document)
        
        try:
            import json
            with open(file_path, "r", encoding="utf-8") as f:
                service_account_data = json.load(f)
            
            # Validate that it's a valid service account file
            required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url"]
            for field in required_fields:
                if field not in service_account_data:
                    raise Exception(f"Invalid service account file: missing required field '{field}'. Please provide a valid Google Cloud service account JSON key file.")
            if service_account_data["type"] != "service_account":
                raise Exception("Invalid file type. This doesn't look like a service account key file.")
            
            # Save to database
            await db["config"].update_one(
                {"_id": "google_drive_creds"},
                {"$set": {
                    "service_account": service_account_data,
                    "already_notified_auth_issue": False  # Reset notification flag
                }},
                upsert=True
            )

            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond("✅ Google Drive service account key saved successfully! This is the recommended method and won't require token refreshes!\n\n💡 Important: Make sure you share your Google Drive (or the specific folder) with the service account's email address: " + service_account_data["client_email"])
            
        except Exception as e:
            await event.respond(f"❌ Failed to parse service account key: {e}")
        finally:
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
    
    elif state["action"] == "AWAITING_GOOGLE_TOKEN":
        if not event.document:
            await event.respond("Please upload a valid JSON file.")
            return
        
        processing_msg = await event.respond("📥 Parsing token.json...")
        file_path = await bot.download_media(event.document)
        
        try:
            import json
            with open(file_path, "r", encoding="utf-8") as f:
                token_data = json.load(f)
            
            # Validate that it's a valid Google Drive token file
            if "token" not in token_data and "access_token" not in token_data:
                raise Exception("Invalid token.json file. Please provide a valid Google Drive OAuth 2.0 token file.")
            
            # Save to database
            await db["config"].update_one(
                {"_id": "google_drive_creds"},
                {"$set": {
                    "token": token_data,
                    "already_notified_auth_issue": False  # Reset notification flag
                }},
                upsert=True
            )

            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond("✅ Google Drive token saved successfully!")
            
        except Exception as e:
            await event.respond(f"❌ Failed to parse token: {e}")
        finally:
            # Clean up downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)

# =====================================================================
# AD-HOC CHANNEL MANAGEMENT COMMANDS
# =====================================================================
def normalize_channel_id(identifier):
    """Normalize any channel ID input to the standard -100xxxxxx format."""
    raw = str(identifier).strip()
    if raw.replace("-", "").isdigit():
        num_id = int(raw)
        if num_id > 0:
            return int(f"-100{num_id}")
        elif not str(num_id).startswith("-100"):
            return int(f"-100{abs(num_id)}")
        else:
            return num_id
    return None

async def _resolve_channel_id(identifier) -> tuple[int, str]:
    """Resolve a username, t.me link, or numeric ID to (channel_id, label) using user client."""
    if not user:
        raise RuntimeError("User session not configured. Please set user session via /settings first.")
    raw = str(identifier).strip()
    
    normalized_id = normalize_channel_id(raw)
    
    if normalized_id is not None:
        identifier = normalized_id
    elif "t.me/c/" in raw:
        part = raw.split("t.me/c/")[1].split("/")[0]
        identifier = int(f"-100{part}")
    elif "t.me/" in raw:
        identifier = raw.split("t.me/")[1].split("/")[0]

    entity = await user.get_entity(identifier)
    
    # Validate that it's a Channel
    from telethon.tl.types import Channel
    if not isinstance(entity, Channel):
        raise ValueError(f"Not a valid channel. Got {type(entity).__name__} instead.")
    
    channel_id = entity.id
    if not str(channel_id).startswith("-"):
        channel_id = int(f"-100{channel_id}")
    label = getattr(entity, "username", None) or getattr(entity, "title", str(channel_id))
    return channel_id, label

@bot.on(events.NewMessage(pattern=r"/add_channel"))
async def add_channel_command(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    parts = event.text.split(maxsplit=1)
    if len(parts) < 2:
        await event.respond("Usage: `/add_channel @username` or `/add_channel https://t.me/...`")
        return
    try:
        channel_id, label = await _resolve_channel_id(parts[1].strip())
        await db["config"].update_one(
            {"_id": "bot_settings"},
            {"$addToSet": {"monitored_channels": {"id": channel_id, "label": label}}}
        )
        await event.respond(f"✅ Now monitoring **{label}** (`{channel_id}`)")
    except Exception as ex:
        await event.respond(f"❌ Could not resolve channel: `{ex}`")

@bot.on(events.NewMessage(pattern=r"/add_ocr_channel"))
async def add_ocr_channel_command(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    parts = event.text.split(maxsplit=1)
    if len(parts) < 2:
        await event.respond("Usage: `/add_ocr_channel @username` or `/add_ocr_channel https://t.me/...`")
        return
    try:
        channel_id, label = await _resolve_channel_id(parts[1].strip())
        await db["config"].update_one(
            {"_id": "bot_settings"},
            {"$addToSet": {"ocr_channels": {"id": channel_id, "label": label}}}
        )
        await event.respond(f"✅ Now performing OCR on **{label}** (`{channel_id}`)")
    except Exception as ex:
        await event.respond(f"❌ Could not resolve channel: `{ex}`")

async def remove_channel_by_id_or_resolve(identifier, channel_list_field, description):
    """Helper to remove a channel, either by direct ID lookup or by resolving."""
    config = await db["config"].find_one({"_id": "bot_settings"})
    channels = config.get(channel_list_field, []) if config else []
    
    # First try to normalize as ID and look up in database
    normalized_id = normalize_channel_id(identifier)
    if normalized_id is not None:
        # Look for the channel in our stored channels
        for channel in channels:
            if channel.get("id") == normalized_id:
                channel_id = channel["id"]
                label = channel.get("label", str(channel_id))
                await db["config"].update_one(
                    {"_id": "bot_settings"},
                    {"$pull": {channel_list_field: {"id": channel_id}}}
                )
                return True, label, channel_id
    
    # If not found by ID, try to resolve normally
    channel_id, label = await _resolve_channel_id(identifier)
    await db["config"].update_one(
        {"_id": "bot_settings"},
        {"$pull": {channel_list_field: {"id": channel_id}}}
    )
    return True, label, channel_id

@bot.on(events.NewMessage(pattern=r"/remove_ocr_channel"))
async def remove_ocr_channel_command(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    parts = event.text.split(maxsplit=1)
    if len(parts) < 2:
        await event.respond("Usage: `/remove_ocr_channel @username`, link, or numeric ID")
        return
    try:
        success, label, channel_id = await remove_channel_by_id_or_resolve(
            parts[1].strip(), "ocr_channels", "OCR channels"
        )
        await event.respond(f"🗑️ Removed **{label}** (`{channel_id}`) from OCR channels.")
    except Exception as ex:
        await event.respond(f"❌ Could not resolve channel: `{ex}`")

@bot.on(events.NewMessage(pattern=r"/remove_channel"))
async def remove_channel_command(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    parts = event.text.split(maxsplit=1)
    if len(parts) < 2:
        await event.respond("Usage: `/remove_channel @username`, link, or numeric ID")
        return
    try:
        success, label, channel_id = await remove_channel_by_id_or_resolve(
            parts[1].strip(), "monitored_channels", "monitored channels"
        )
        await event.respond(f"🗑️ Removed **{label}** (`{channel_id}`) from monitoring.")
    except Exception as ex:
        await event.respond(f"❌ Could not resolve channel: `{ex}`")

@bot.on(events.NewMessage(pattern=r"/scan_old_messages"))
async def scan_old_messages_command(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    await event.respond("🔍 Starting scan of monitored channels for last 24 hour messages...")
    logger.info("Manual scan triggered via /scan_old_messages command.")
    bot_activity_logger.info("="*80)
    bot_activity_logger.info("MANUAL SCAN TRIGGERED VIA /scan_old_messages COMMAND")
    bot_activity_logger.info("="*80)
    try:
        await scan_channels_for_last_24h_portfolio_messages()
        await event.respond("✅ Scan complete! Check your messages for forwarded last 24 hour data and summary files.")
    except Exception as ex:
        logger.error(f"Error during manual scan: {ex}")
        bot_activity_logger.error(f"Error during manual scan: {ex}")
        await event.respond(f"❌ Error during scan: `{ex}`")


# =====================================================================
# DUPLICATE DETECTION: FINGERPRINT + SEMANTIC SIMILARITY GUARD
# =====================================================================

def _normalise(text: str) -> str:
    return re.sub(r'\s+', ' ', text.lower().strip())

async def is_duplicate_news(text_content: str) -> bool:
    """
    Hash-based deduplication check.
    """
    normalised = _normalise(text_content)

    # ── Gate 1: exact hash ──────────────────────────────────────────
    msg_hash = hashlib.sha256(normalised.encode()).hexdigest()
    existing = await db["recent_news_hashes"].find_one({"_id": msg_hash})
    if existing:
        logger.info(f"[Dedup-Gate1] Exact fingerprint match ({msg_hash[:12]}…). Dropping.")
        return True

    # Store hash now; TTL index will purge it after 1 hour automatically
    await db["recent_news_hashes"].insert_one({
        "_id": msg_hash,
        "ts": datetime.now(IST)
    })

    return False

# =====================================================================
# BATCH BUFFER: holds unique messages, waits 30s, flushes all at once
# =====================================================================

# Structure: { msg_hash -> {"message": {...}, "entities": list[str], "match_type": str} }
_portfolio_buffer: dict = {}
_macro_buffer: dict = {}
_portfolio_timer_task: asyncio.Task = None
_macro_timer_task: asyncio.Task = None
_BATCH_WINDOW = 30  # seconds to wait before flushing buffer


async def _flush_buffer(buffer_name: str):
    """
    Called after BATCH_WINDOW seconds of silence for a specific buffer.
    Forwards all unique messages (text and PDF) to owners.
    """
    if buffer_name == "portfolio":
        global _portfolio_buffer, _portfolio_timer_task
        buffer = _portfolio_buffer
        _portfolio_buffer = {}
        _portfolio_timer_task = None
    else:  # macro
        global _macro_buffer, _macro_timer_task
        buffer = _macro_buffer
        _macro_buffer = {}
        _macro_timer_task = None
    
    messages = list(buffer.values())
    if not messages:
        return

    logger.info(f"[Batch] Flushing {len(messages)} unique {buffer_name} message(s).")
    bot_activity_logger.info(f"="*80)
    bot_activity_logger.info(f"PROCESSING BATCH OF {len(messages)} UNIQUE {buffer_name.upper()} MESSAGES")
    bot_activity_logger.info(f"="*80)
    
    for i, msg_bucket in enumerate(messages, 1):
        item = msg_bucket["message"]
        entities = msg_bucket["entities"]
        bot_activity_logger.info(f"  [{i}] {item['deep_link']} | Entities: {', '.join(entities)}")

    # Collect all unique entities/stock names
    all_entities = set()
    for msg_bucket in messages:
        all_entities.update(msg_bucket["entities"])
    
    # Build notification message
    notification_parts = []
    if buffer_name == "portfolio":
        notification_parts.append(f"📈 **Latest Portfolio News Update**: {', '.join(sorted(all_entities))}")
    else:
        notification_parts.append(f"🌐 **Latest Macro News**: {', '.join(sorted(all_entities))}")
    notification_parts.append(f"Unique Messages: {len(messages)}")
    notification_text = "\n".join(notification_parts)

    # ── FIRST: Forward all unique messages (text and PDF) to owners ──────────
    bot_activity_logger.info(f"\nSTEP 1: Sending {buffer_name} messages to owners first")
    for owner in OWNERS:
        try:
            # Resolve owner entity first to prevent errors
            owner_entity = PeerUser(int(owner))
            
            await bot.send_message(
                owner_entity,
                f"📨 **New {buffer_name.capitalize()} Updates**\n"
                f"{notification_text}",
                link_preview=False
            )
            
            for msg_bucket in messages:
                item = msg_bucket["message"]
                entities = msg_bucket["entities"]
                match_type = msg_bucket["match_type"]
                try:
                    match_label = "📊 Portfolio Stock" if match_type == "Portfolio Stock" else "🌐 Macro Economy"
                    if match_type == "Macro Economy":
                        macro_msg = f"🌐 **Macro Economy Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}"
                        await bot.send_message(
                            owner_entity,
                            macro_msg,
                            link_preview=True
                        )
                        bot_activity_logger.info(f"  ✓ Macro news link sent to {owner}: {macro_msg}")
                    else:
                        # Handle photo if present
                        if item.get("has_photo") and user:
                            try:
                                bot_activity_logger.info(f"  Processing photo from message {item['message_id']}...")
                                original_message = await user.get_messages(item['chat_id'], ids=item['message_id'])
                                bot_activity_logger.info(f"  Downloading photo from message {item['message_id']}...")
                                photo_file = await user.download_media(original_message, file=bytes)
                                if photo_file:
                                    photo_bytesio = io.BytesIO(photo_file)
                                    photo_bytesio.name = f"photo_{item['message_id']}.jpg"
                                    photo_caption = f"📷 **Portfolio Stock Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}"
                                    if item.get("text"):
                                        photo_caption += f"\n\n{item['text']}"
                                    if item.get("ocr_text"):
                                        photo_caption += f"\n\n--- OCR Text ---\n{item['ocr_text']}"
                                    await bot.send_file(
                                        owner_entity,
                                        photo_bytesio,
                                        caption=photo_caption,
                                        link_preview=False
                                    )
                                    bot_activity_logger.info(f"  ✓ Photo sent to {owner}: {photo_caption}")
                                    continue  # Skip sending text separately
                            except Exception as photo_err:
                                logger.error(f"Failed to send photo: {photo_err}")
                                bot_activity_logger.error(f"  ✗ Failed to send photo: {photo_err}")
                                # Fallback: send the link and text
                                fallback_msg = f"⚠️ Not able to send photo, but here's the message:\n📊 **Portfolio Stock Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}"
                                if item.get("text"):
                                    fallback_msg += f"\n\n{item['text']}"
                                if item.get("ocr_text"):
                                    fallback_msg += f"\n\n--- OCR Text ---\n{item['ocr_text']}"
                                await bot.send_message(
                                    owner_entity,
                                    fallback_msg,
                                    link_preview=True
                                )
                                bot_activity_logger.info(f"  ✓ Fallback message sent to {owner}: {fallback_msg}")
                        
                        # Handle PDF if present
                        if item.get("has_pdf") and user:
                            try:
                                bot_activity_logger.info(f"  Processing PDF from message {item['message_id']}...")
                                original_message = await user.get_messages(item['chat_id'], ids=item['message_id'])
                                pdf_filename = extract_real_filename(original_message, entities[0])
                                bot_activity_logger.info(f"  Downloading PDF from message {item['message_id']}...")
                                pdf_file = await user.download_media(original_message, file=bytes)
                                if pdf_file:
                                    pdf_bytesio = io.BytesIO(pdf_file)
                                    pdf_bytesio.name = pdf_filename
                                    pdf_caption = f"📄 **Portfolio Stock Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}"
                                    if item.get("text"):
                                        pdf_caption += f"\n\n{item['text']}"
                                    if item.get("pdf_filename"):
                                        pdf_caption += f"\n\n--- PDF Filename ---\n{item['pdf_filename']}"
                                    await bot.send_file(
                                        owner_entity,
                                        pdf_bytesio,
                                        caption=pdf_caption,
                                        link_preview=False
                                    )
                                    bot_activity_logger.info(f"  ✓ PDF sent to {owner} with filename: {pdf_filename}, caption: {pdf_caption}")
                                    continue  # Skip sending text separately
                            except Exception as pdf_err:
                                logger.error(f"Failed to send PDF: {pdf_err}")
                                bot_activity_logger.error(f"  ✗ Failed to send PDF: {pdf_err}")
                                pdf_fallback_msg = f"⚠️ Not able to send PDF, but here's the source link:\n📊 **Portfolio Stock Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}"
                                if item.get("text"):
                                    pdf_fallback_msg += f"\n\n{item['text']}"
                                if item.get("pdf_filename"):
                                    pdf_fallback_msg += f"\n\n--- PDF Filename ---\n{item['pdf_filename']}"
                                await bot.send_message(
                                    owner_entity,
                                    pdf_fallback_msg,
                                    link_preview=True
                                )
                                bot_activity_logger.info(f"  ✓ PDF fallback message sent to {owner}: {pdf_fallback_msg}")
                        
                        # Handle text only
                        if item.get("text"):
                            text_msg = f"📊 **Portfolio Stock Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}\n\n{item['text']}"
                            await bot.send_message(
                                owner_entity,
                                text_msg,
                                link_preview=False
                            )
                            bot_activity_logger.info(f"  ✓ Text sent to {owner}: {text_msg}")
                        elif not item.get("has_photo") and not item.get("has_pdf"):
                            # No text, photo, or PDF—just send link
                            link_msg = f"📊 **Portfolio Stock Match**\nMatched: {', '.join(entities)}\nSource: {item['deep_link']}"
                            await bot.send_message(
                                owner_entity,
                                link_msg,
                                link_preview=True
                            )
                            bot_activity_logger.info(f"  ✓ Link sent to {owner}: {link_msg}")
                except Exception as e:
                    logger.error(f"Failed to send message to {owner}: {e}")
                    bot_activity_logger.error(f"  ✗ Failed to send to {owner}: {e}")
                
        except Exception as e:
            logger.error(f"Failed to send messages to {owner}: {e}")
            bot_activity_logger.error(f"  ✗ Failed to send to {owner}: {e}")


async def _schedule_buffer_flush(buffer_name: str):
    if buffer_name == "portfolio":
        global _portfolio_timer_task
        if _portfolio_timer_task and not _portfolio_timer_task.done():
            _portfolio_timer_task.cancel()
        _portfolio_timer_task = asyncio.create_task(_wait_and_flush(buffer_name))
    else:
        global _macro_timer_task
        if _macro_timer_task and not _macro_timer_task.done():
            _macro_timer_task.cancel()
        _macro_timer_task = asyncio.create_task(_wait_and_flush(buffer_name))


async def _wait_and_flush(buffer_name: str):
    try:
        await asyncio.sleep(_BATCH_WINDOW)
        await _flush_buffer(buffer_name)
    except asyncio.CancelledError:
        pass



# =====================================================================
# STREAMING SUBSCRIBER CONSUMPTION TIER (THE LISTENER ENGINE)
# =====================================================================
async def incoming_stream_pipeline(event: events.NewMessage.Event):
    # Filter configuration constraints
    if not (event.is_channel or event.is_group):
        return

    config = await get_system_config()
    monitored_raw = config.get("monitored_channels", [])
    ocr_channels_raw = config.get("ocr_channels", [])

    # Support both old format (bare int) and new format ({"id": int, "label": str})
    monitored_ids = set()
    for ch in monitored_raw:
        if isinstance(ch, dict):
            monitored_ids.add(ch["id"])
        else:
            monitored_ids.add(int(ch))
    
    # Get OCR channel IDs
    ocr_channel_ids = set()
    for ch in ocr_channels_raw:
        if isinstance(ch, dict):
            ocr_channel_ids.add(ch["id"])
        else:
            ocr_channel_ids.add(int(ch))
    
    # Commented out unnecessary repeated logs
    # channel_logger.info(f"[LIVE MONITOR] Monitored channel IDs: {sorted(monitored_ids)}")
    # channel_logger.info(f"[LIVE MONITOR] OCR channel IDs: {sorted(ocr_channel_ids)}")

    # Get chat info for logging
    chat = await event.get_chat()
    chat_label = getattr(chat, 'title', getattr(chat, 'username', str(event.chat_id)))

    # Extract source link
    chat_peer = str(event.chat_id).replace("-100", "")
    deep_link = f"https://t.me/c/{chat_peer}/{event.id}"
    if getattr(chat, 'username', None):
        deep_link = f"https://t.me/{chat.username}/{event.id}"
    
    # Get message timestamp (IST)
    message_timestamp_ist = event.date.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    text_content = event.text or ""
    ocr_text = ""
    pdf_filename = ""

    # Detect PDF: document with mime_type application/pdf
    has_pdf = False
    if event.document:
        mime = getattr(event.document, "mime_type", "") or ""
        if mime == "application/pdf":
            has_pdf = True
            # Extract PDF filename
            for attr in event.document.attributes:
                if hasattr(attr, "file_name"):
                    pdf_filename = attr.file_name
                    break

    has_photo = bool(event.photo)

    # Log EVERY message we see in monitored channels
    if event.chat_id in monitored_ids:
        # Short preview of text content (max 100 chars)
        short_text = (text_content[:100] + "...") if len(text_content) > 100 else (text_content or "(empty)")
        
        log_msg = (
            f"\n{'='*80}\n"
            f"[LIVE SCAN] NEW MESSAGE RECEIVED\n"
            f"{'='*80}\n"
            f"Channel: {chat_label} (ID: {event.chat_id})\n"
            f"Message ID: {event.id}\n"
            f"Deep Link: {deep_link}\n"
            f"Has PDF: {has_pdf}\n"
            f"PDF Filename: {pdf_filename}\n"
            f"Has Photo: {has_photo}\n"
            f"Short Text Preview: {short_text}\n"
        )
        channel_logger.info(log_msg)

        # ── PHASE 1: Text-only filter pass (no OCR yet, saves time & rate limits) ──
        combined_text_parts = [text_content]
        if pdf_filename:
            combined_text_parts.append(f"--- PDF FILENAME ---\n{pdf_filename}")
        combined_text_phase1 = "\n\n".join(combined_text_parts)

        # Quick check: if text alone already matches, we can skip OCR entirely.
        # If text doesn't match but there IS a photo, we'll run OCR and re-check.
        text_matched = False
        text_excluded = False
        text_match_details = []
        if combined_text_phase1.strip():
            text_matched, _, text_match_details = await execute_two_tier_filter(combined_text_phase1)
            # Check if text was excluded (universal or stock-specific)
            has_any_exclusion = any(d.get("excluded", False) for d in text_match_details)
            if has_any_exclusion:
                text_excluded = True

        # ── PHASE 2: OCR only if photo present AND text alone didn't match AND channel is OCR channel AND caption not excluded ──
        sent_to_ocr = False
        photo_bytes = None
        image_hash = None
        if has_photo and not text_matched and not text_excluded and event.chat_id in ocr_channel_ids:
            sent_to_ocr = True
            channel_logger.info(f"[LIVE SCAN] Sending photo to OCR for message {event.id}")
            try:
                # Download image to memory — no disk write at all
                photo_bytes = await event.download_media(file=bytes)
                if photo_bytes:
                    image_hash = hashlib.sha256(photo_bytes).hexdigest()
                    
                    # Check cache first
                    cached_result = await get_cached_ocr_result(image_hash)
                    if cached_result:
                        ocr_text = cached_result.get("extracted_text")
                        logger.info(f"Using cached OCR for image {deep_link} (hash={image_hash[:10]}...)")
                        ocr_logger.info(f"════════════════════════════════════════════════════════════")
                        ocr_logger.info(f"📷 Image: {deep_link}")
                        ocr_logger.info(f"✅ Using CACHED OCR (REAL-TIME)")
                        ocr_logger.info(f"   Match type: {cached_result.get('match_type')}")
                        ocr_logger.info(f"   Matched entities: {cached_result.get('matched_entities')}")
                        ocr_logger.info(f"   Extracted text: {ocr_text[:200]}...")
                    else:
                        # No cache — run OCR
                        ocr_text = await image_to_text(db, photo_bytes, bot, OWNERS)
                        logger.info(f"OCR extracted text from image: {ocr_text[:200]}...")
                        channel_logger.info(f"[OCR] Extracted text from image: {ocr_text[:200]}...")
                        ocr_logger.info(f"════════════════════════════════════════════════════════════")
                        ocr_logger.info(f"📷 Image: {deep_link}")
                        ocr_logger.info(f"🔍 Running NEW OCR (REAL-TIME)")
                    
                    # We'll save to cache after we run the final filter
            except Exception as e:
                logger.error(f"Failed to OCR image: {e}")
                channel_logger.error(f"[OCR] Failed to OCR image: {e}")
                ocr_logger.error(f"❌ Failed to OCR real-time image {deep_link}: {e}")
        elif has_photo and text_excluded and event.chat_id in ocr_channel_ids:
            channel_logger.info(f"[LIVE SCAN] Skipping OCR for message {event.id} - caption/text has exclusions")

        # Combine text + OCR + PDF filename for final filter
        combined_text_parts = [text_content]
        if pdf_filename:
            combined_text_parts.append(f"--- PDF FILENAME ---\n{pdf_filename}")
        if ocr_text:
            combined_text_parts.append(f"--- OCR TEXT ---\n{ocr_text}")
        combined_text = "\n\n".join(combined_text_parts)

        # Local keyword filter on combined text
        is_matched, match_type, match_details = await execute_two_tier_filter(combined_text)
        entities = [d["name"] for d in match_details if d["type"] == "stock" and not d["excluded"]] if match_type == "Portfolio Stock" else [d["name"] for d in match_details if d["type"] == "macro"]
        
        # Log match status
        if match_details:
            for detail in match_details:
                if detail["type"] == "stock":
                    if detail["excluded"]:
                        channel_logger.info(f"[LIVE SCAN] EXCLUDED MATCH - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}, Matched Exclusions: {', '.join(detail['matched_exclusions'])}")
                    else:
                        channel_logger.info(f"[LIVE SCAN] MATCHED - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}")
                else:
                    channel_logger.info(f"[LIVE SCAN] MATCHED - Macro Keyword: {', '.join(detail['matched_positive'])}")
            if has_pdf:
                channel_logger.info(f"[LIVE SCAN] PDF matched - Filename: {pdf_filename}")
            if has_photo:
                if sent_to_ocr:
                    channel_logger.info(f"[LIVE SCAN] Photo matched via OCR")
                else:
                    channel_logger.info(f"[LIVE SCAN] Photo's caption/text matched (no OCR needed)")
        else:
            channel_logger.info(f"[LIVE SCAN] NO MATCH - Not in portfolio or macro keywords")
        
        # Save OCR result to DB if we ran OCR
        if has_photo and not text_matched and event.chat_id in ocr_channel_ids and photo_bytes:
            await save_ocr_result_to_db(
                image_hash,
                ocr_text,
                deep_link,
                match_type,
                entities,
                is_matched
            )
            ocr_logger.info(f"════════════════════════════════════════════════════════════")
            ocr_logger.info(f"📷 Image: {deep_link}")
            if is_matched:
                ocr_logger.info(f"✅ MATCH FOUND")
                ocr_logger.info(f"   Match type: {match_type}")
                ocr_logger.info(f"   Matched entities: {', '.join(entities)}")
                ocr_logger.info(f"   Extracted text: {ocr_text[:200]}...")
            else:
                ocr_logger.info(f"❌ No match found")
                ocr_logger.info(f"   Extracted text: {ocr_text[:200]}...")
            ocr_logger.info(f"════════════════════════════════════════════════════════════\n")
        
        if not is_matched:
            return

        logger.info(f"[Filter Hit] {match_type} — {', '.join(entities)}. Sending to user(s) now.")

        # If text is empty, no PDF, and no photo, nothing useful to send
        if not combined_text and not has_pdf and not has_photo:
            channel_logger.info(f"[FILTERED] No text, PDF, or photo - Skipping")
            return

        # Exact-hash dedup on text (zero-token gate) — skip for PDF-only or photo-only messages
        msg_hash = None
        if text_content:
            normalised = _normalise(text_content)
            msg_hash = hashlib.sha256(normalised.encode()).hexdigest()
        else:
            # For PDF-only, create hash from file ID if available
            if event.document and event.document.file_id:
                msg_hash = hashlib.sha256(event.document.file_id.encode()).hexdigest()
            # For photo-only, create hash from photo file ID if available
            elif event.photo:
                # Find a PhotoSize that actually has a file_id (not PhotoSizeProgressive)
                photo_id = None
                sizes = getattr(event.photo, 'sizes', [])
                for s in reversed(sizes):
                    fid = getattr(s, 'file_id', None)
                    if fid:
                        photo_id = fid
                        break
                if not photo_id:
                    # Fallback: use the photo's top-level id
                    photo_id = str(getattr(event.photo, 'id', str(event.id)))
                msg_hash = hashlib.sha256(photo_id.encode()).hexdigest()
        
        # Check dedup
        if msg_hash:
            if await db["recent_news_hashes"].find_one({"_id": msg_hash}):
                logger.info(f"[Dedup-Gate1] Duplicate fingerprint in DB. Dropping.")
                channel_logger.info(f"[FILTERED] Duplicate message (hash: {msg_hash[:16]}...)")
                return
            await db["recent_news_hashes"].insert_one({"_id": msg_hash, "ts": datetime.now(IST)})

        # Now send to owners based on match type!
        for owner in OWNERS:
            try:
                # Use PeerUser directly (works even if no prior interaction)
                owner_entity = PeerUser(int(owner))
                
                if match_type == "Macro Economy":
                    # Send macro with matched keywords
                    matched_macro_str = ", ".join([", ".join(d["matched_positive"]) for d in match_details if d["type"] == "macro"])
                    macro_message = f"🌐 Live Update - Macro Match\nMatched: {matched_macro_str}\nFrom: {chat_label}\nPosted On: {message_timestamp_ist}\nSource: {deep_link}"
                    await bot.send_message(
                        owner_entity,
                        clean_text_for_telegram(macro_message, max_length=4096),
                        link_preview=True
                    )
                    bot_activity_logger.info(f"✓ Macro link sent to owner {owner}: {deep_link}")
                elif match_type == "Portfolio Stock":
                    # Get matched positive variants for portfolio
                    matched_positives_str = ", ".join([", ".join(d["matched_positive"]) for d in match_details if d["type"] == "stock" and not d["excluded"]])
                    forwarded = False
                    
                    # Try forwarding first if user session is available
                    if user:
                        try:
                            # Use PeerUser directly with user client too
                            owner_entity_user = PeerUser(int(owner))
                            await user.forward_messages(owner_entity_user, event.id, event.chat_id)
                            # Also send a note with matched keywords, channel, and timestamp
                            portfolio_note = f"📊 Live Update - Portfolio Match\nMatched: {matched_positives_str}\nFrom: {chat_label}\nPosted On: {message_timestamp_ist}\nSource: {deep_link}"
                            await bot.send_message(
                                owner_entity,
                                clean_text_for_telegram(portfolio_note, max_length=4096),
                                link_preview=False
                            )
                            bot_activity_logger.info(f"✓ Portfolio message forwarded to owner {owner}: {deep_link}")
                            forwarded = True
                        except Exception as forward_err:
                            # Don't log warning since this is expected for different owner/user sessions
                            pass
                    
                    # If forwarding failed or no user session, download and re-upload
                    if not forwarded:
                        header = f"📊 Live Update - Portfolio Match\nMatched: {matched_positives_str}\nFrom: {chat_label}\nPosted On: {message_timestamp_ist}\nSource: {deep_link}\n\n"
                        
                        # Handle photo if present
                        if has_photo:
                            try:
                                photo_bytes = await event.download_media(file=bytes)
                                if photo_bytes:
                                    photo_bytesio = io.BytesIO(photo_bytes)
                                    photo_bytesio.name = f"photo_{event.id}.jpg"
                                    caption = header
                                    if text_content:
                                        caption += text_content
                                    if ocr_text:
                                        caption += f"\n\n--- OCR Text ---\n{ocr_text}"
                                    # Clean and truncate caption for Telegram
                                    caption = clean_text_for_telegram(caption, max_length=1024)
                                    await bot.send_file(
                                        owner_entity,
                                        photo_bytesio,
                                        caption=caption,
                                        link_preview=False
                                    )
                                    bot_activity_logger.info(f"✓ Photo sent to owner {owner}: {deep_link}")
                                else:
                                    # No photo bytes, send text only
                                    message_text = header + (text_content or "")
                                    if ocr_text:
                                        message_text += f"\n\n--- OCR Text ---\n{ocr_text}"
                                    message_text = clean_text_for_telegram(message_text, max_length=4096)
                                    await bot.send_message(
                                        owner_entity,
                                        message_text,
                                        link_preview=True
                                    )
                                    bot_activity_logger.info(f"✓ Text sent to owner {owner}: {deep_link}")
                            except Exception as photo_err:
                                logger.error(f"Failed to send photo: {photo_err}")
                                bot_activity_logger.error(f"✗ Failed to send photo: {photo_err}")
                                # Fallback to text only
                                message_text = header + (text_content or "")
                                if ocr_text:
                                    message_text += f"\n\n--- OCR Text ---\n{ocr_text}"
                                message_text = clean_text_for_telegram(message_text, max_length=4096)
                                await bot.send_message(
                                    owner_entity,
                                    message_text,
                                    link_preview=True
                                )
                        
                        # Handle PDF if present
                        elif has_pdf:
                            try:
                                pdf_bytes = await event.download_media(file=bytes)
                                if pdf_bytes:
                                    pdf_bytesio = io.BytesIO(pdf_bytes)
                                    pdf_bytesio.name = pdf_filename
                                    caption = header
                                    if text_content:
                                        caption += text_content
                                    await bot.send_file(
                                        owner_entity,
                                        pdf_bytesio,
                                        caption=caption,
                                        link_preview=False
                                    )
                                    bot_activity_logger.info(f"✓ PDF sent to owner {owner}: {deep_link}")
                                else:
                                    # No PDF bytes, send text only
                                    await bot.send_message(
                                        owner_entity,
                                        header + (text_content or ""),
                                        link_preview=True
                                    )
                                    bot_activity_logger.info(f"✓ Text sent to owner {owner}: {deep_link}")
                            except Exception as pdf_err:
                                logger.error(f"Failed to send PDF: {pdf_err}")
                                bot_activity_logger.error(f"✗ Failed to send PDF: {pdf_err}")
                                # Fallback to text only
                                await bot.send_message(
                                    owner_entity,
                                    header + (text_content or ""),
                                    link_preview=True
                                )
                        
                        # Handle text only
                        else:
                            message_text = header + (text_content or "")
                            if ocr_text:
                                message_text += f"\n\n--- OCR Text ---\n{ocr_text}"
                            await bot.send_message(
                                owner_entity,
                                message_text,
                                link_preview=True
                            )
                            bot_activity_logger.info(f"✓ Text sent to owner {owner}: {deep_link}")
            except Exception as e:
                logger.error(f"Failed to send message to owner {owner}: {e}")
                bot_activity_logger.error(f"✗ Failed to send to owner {owner}: {e}")

# =====================================================================
# STARTUP CHECK: SCAN CHANNELS FOR OLD PORTFOLIO MESSAGES IF NO RECENT NEWS
# =====================================================================
async def scan_channels_for_last_24h_portfolio_messages():
    """
    Scans all monitored channels for messages about portfolio stocks (last 24 hours)
    and forwards them to owners, indicating they are last 24 hour data.
    Called on startup if no recent news hashes are found OR via /scan_old_messages command.
    """
    if not user:
        logger.error("Cannot scan channels: user session not configured.")
        return
    logger.info("Checking for last 24 hour portfolio and macro messages in monitored channels...")
    channel_logger.info("="*80)
    channel_logger.info("STARTING CHANNEL SCAN FOR LAST 24 HOUR PORTFOLIO & MACRO MESSAGES")
    channel_logger.info("="*80)
    bot_activity_logger.info(f"Scan started at: " + datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"))
    
    # Calculate cutoff time (24h ago in IST)
    now_ist = datetime.now(IST)
    cutoff_ist = now_ist - timedelta(days=1)
    bot_activity_logger.info(f"Cutoff time for messages: {cutoff_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    config = await get_system_config()
    monitored_raw = config.get("monitored_channels", [])
    ocr_channels_raw = config.get("ocr_channels", [])
    bot_activity_logger.info(f"Monitored channels from config: " + str(monitored_raw))
    
    # Get monitored channel IDs
    monitored_ids = set()
    for ch in monitored_raw:
        if isinstance(ch, dict):
            monitored_ids.add(ch["id"])
        else:
            monitored_ids.add(int(ch))
    
    # Get OCR channel IDs
    ocr_channel_ids = set()
    for ch in ocr_channels_raw:
        if isinstance(ch, dict):
            ocr_channel_ids.add(ch["id"])
        else:
            ocr_channel_ids.add(int(ch))
    bot_activity_logger.info(f"Monitored channel IDs: " + str(monitored_ids))
    bot_activity_logger.info(f"OCR channel IDs: " + str(ocr_channel_ids))
    
    if not monitored_ids:
        logger.info("No monitored channels to scan.")
        channel_logger.info("No monitored channels to scan.")
        bot_activity_logger.info("No monitored channels to scan.")
        # Still send empty text files
        await send_empty_scan_text_files()
        return
    
    # Initialize scan progress manager
    scan_progress = ProgressManager(bot, OWNERS)
    ocr_channel_count = len(ocr_channel_ids.intersection(monitored_ids))
    non_ocr_channel_count = len(monitored_ids) - ocr_channel_count
    await scan_progress.send_initial_progress(
        "🔍 Starting scan of monitored channels for last 24 hour messages...",
        f"Channels to scan: {len(monitored_ids)}\n"
        f"OCR enabled: {ocr_channel_count}\n"
        f"Non-OCR: {non_ocr_channel_count}\n"
        f"Progress: 0/{len(monitored_ids)} channels scanned"
    )
    
    # Collect all matched messages first (for text files, no hash checks yet)
    all_matched_messages = []
    max_messages_per_channel = 1000  # Prevent endless scanning
    bot_activity_logger.info(f"Starting to collect matched messages (max {max_messages_per_channel} per channel)...")
    
    for i, channel_id in enumerate(monitored_ids, 1):
        try:
            # Get channel info and update progress
            chat = await user.get_entity(channel_id)
            channel_label = getattr(chat, 'title', getattr(chat, 'username', str(channel_id)))
            is_ocr_channel = channel_id in ocr_channel_ids
                
            await scan_progress.update_progress(
                "🔍 Scanning monitored channels for last 24 hour messages...",
                f"Current: {channel_label} ({'OCR ENABLED' if is_ocr_channel else 'NON-OCR'})\n"
                f"Progress: {i}/{len(monitored_ids)} channels scanned"
            )

            channel_logger.info(f"\nScanning channel: {channel_label} (ID: {channel_id})")
            bot_activity_logger.info(f"Scanning channel: {channel_label} (ID: {channel_id})")
            
            # Fetch messages from the last 24 hours
            messages_scanned = 0
            messages_skipped = 0
            messages_matched_this_channel = 0
            messages_beyond_cutoff = 0
            
            # Use iter_messages correctly: start from most recent, go backwards
            # Also check message.date explicitly
            async for message in user.iter_messages(
                channel_id, 
                limit=max_messages_per_channel,
                reverse=False,  # False = most recent first (default)
                offset_date=now_ist,  # Start from now and go back
                chunk_size=100  # Fetch 100 messages per API call to reduce round trips
            ):
                messages_scanned += 1
                
                # Check if message has already been processed
                if await is_message_processed(channel_id, message.id):
                    messages_skipped +=1
                    continue
                
                # Mark message as processed immediately
                await mark_message_processed(channel_id, message.id)
                
                # Update progress with message count every 50 messages to avoid Telegram rate limits
                if messages_scanned % 50 == 0:
                    await scan_progress.update_progress(
                        "🔍 Scanning monitored channels for last 24 hour messages...",
                        f"Current: {channel_label} ({'OCR ENABLED' if is_ocr_channel else 'NON-OCR'})\n"
                        f"Channel progress: {messages_scanned} messages scanned (max {max_messages_per_channel} for safety, {messages_skipped} skipped)\n"
                        f"Overall: {i}/{len(monitored_ids)} channels scanned"
                    )
                
                # Convert message.date (which is UTC) to IST
                if message.date.tzinfo is None:
                    # If message.date is naive, assume it's UTC
                    message_date_utc = message.date.replace(tzinfo=timezone.utc)
                else:
                    message_date_utc = message.date
                message_date_ist = message_date_utc.astimezone(IST)
                
                # Check if message is older than cutoff
                if message_date_ist < cutoff_ist:
                    messages_beyond_cutoff += 1
                    if messages_beyond_cutoff >= 5:
                        # If we've seen 5 messages in a row beyond cutoff, stop scanning this channel
                        channel_logger.info(f"Stopping scan for {channel_label}: found 5+ messages beyond cutoff")
                        bot_activity_logger.info(f"Stopping scan for {channel_label}: found 5+ messages beyond cutoff")
                        break
                    continue
                
                text_content = message.text or ""
                pdf_filename = ""
                
                # Check if it has a PDF
                has_pdf = False
                if message.document:
                    mime = getattr(message.document, "mime_type", "") or ""
                    if mime == "application/pdf":
                        has_pdf = True
                        # Extract PDF filename
                        for attr in message.document.attributes:
                            if hasattr(attr, "file_name"):
                                pdf_filename = attr.file_name
                                break
                
                has_photo = bool(message.photo)
                
                # Get source link for logging
                chat_peer = str(channel_id).replace("-100", "")
                deep_link = f"https://t.me/c/{chat_peer}/{message.id}"
                if getattr(chat, 'username', None):
                    deep_link = f"https://t.me/{chat.username}/{message.id}"
                
                # Short preview of text content (max 100 chars)
                short_text = (text_content[:100] + "...") if len(text_content) > 100 else (text_content or "(empty)")
                
                # Log EVERY message we see
                log_msg = (
                    f"\n---\n"
                    f"[MANUAL SCAN] SCANNED MESSAGE\n"
                    f"---\n"
                    f"Channel: {channel_label} (ID: {channel_id})\n"
                    f"Message ID: {message.id}\n"
                    f"Message Date (IST): {message_date_ist.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                    f"Deep Link: {deep_link}\n"
                    f"Has PDF: {has_pdf}\n"
                    f"PDF Filename: {pdf_filename}\n"
                    f"Has Photo: {has_photo}\n"
                    f"Short Text Preview: {short_text}\n"
                )
                channel_logger.info(log_msg)
                
                # ── PASS 1: Text-only filter (NO OCR yet) ──────────────────────────────
                # Check text + PDF filename first to avoid unnecessary OCR calls
                combined_text_phase1_parts = [text_content]
                if pdf_filename:
                    combined_text_phase1_parts.append(f"--- PDF FILENAME ---\n{pdf_filename}")
                combined_text_phase1 = "\n\n".join(combined_text_phase1_parts)

                text_matched = False
                text_excluded = False
                match_type_phase1 = ""
                details_phase1 = []
                entities_phase1 = []
                if combined_text_phase1.strip():
                    text_matched, match_type_phase1, details_phase1 = await execute_two_tier_filter(combined_text_phase1)
                    entities_phase1 = [d["name"] for d in details_phase1 if d["type"] == "stock" and not d["excluded"]] if match_type_phase1 == "Portfolio Stock" else [d["name"] for d in details_phase1 if d["type"] == "macro"]
                    # Check if text was excluded (universal or stock-specific)
                    has_any_exclusion = any(d.get("excluded", False) for d in details_phase1)
                    if has_any_exclusion:
                        text_excluded = True

                # Log any matches/exclusions in phase 1
                if details_phase1:
                    for detail in details_phase1:
                        if detail["type"] == "stock":
                            if detail["excluded"]:
                                channel_logger.info(f"[SCAN OLD MESSAGES] EXCLUDED MATCH - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}, Matched Exclusions: {', '.join(detail['matched_exclusions'])}")
                            else:
                                channel_logger.info(f"[SCAN OLD MESSAGES] MATCHED - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}")
                        else:
                            channel_logger.info(f"[SCAN OLD MESSAGES] MATCHED - Macro Keyword: {', '.join(detail['matched_positive'])}")

                # Skip if no text, no PDF, and no photo — nothing to work with
                if not text_content and not has_pdf and not has_photo:
                    channel_logger.info(f"[FILTERED (SCAN)] No text, PDF, or photo - Skipping")
                    bot_activity_logger.info(f"[SKIPPED] No text/PDF/photo for message: {deep_link}")
                    continue
                
                # If text matched OR text was excluded, record it now. If not matched AND not excluded AND has photo AND in OCR channel, defer for OCR pass.
                needs_ocr_now = has_photo and not text_matched and not text_excluded and channel_id in ocr_channel_ids
                if has_photo and text_excluded and channel_id in ocr_channel_ids:
                    channel_logger.info(f"[SCAN OLD MESSAGES] Skipping OCR for message {message.id} - caption/text has exclusions")
                if not text_matched and not needs_ocr_now:
                    channel_logger.info(f"[FILTERED (SCAN)] No text match, and no photo (or photo not in OCR channel) - skipping")
                    continue

                # Generate message hash for deduplication (text-based hash now, OCR hash added later if needed)
                msg_hash = None
                if text_content:
                    normalised = _normalise(text_content)
                    msg_hash = hashlib.sha256(normalised.encode()).hexdigest()
                elif message.document and getattr(message.document, 'file_id', None):
                    msg_hash = hashlib.sha256(message.document.file_id.encode()).hexdigest()
                elif has_photo and message.photo:
                    # Find a PhotoSize that has a file_id (PhotoSizeProgressive doesn't have one)
                    photo_id = None
                    for s in reversed(getattr(message.photo, 'sizes', [])):
                        fid = getattr(s, 'file_id', None)
                        if fid:
                            photo_id = fid
                            break
                    if not photo_id:
                        photo_id = str(getattr(message.photo, 'id', str(message.id)))
                    msg_hash = hashlib.sha256(photo_id.encode()).hexdigest()
                
                # Add to all matched messages list (OCR text will be filled in pass 2 for photos)
                all_matched_messages.append({
                    "text": text_content,
                    "ocr_text": "",          # filled in pass 2 if needed
                    "pdf_filename": pdf_filename,
                    "deep_link": deep_link,
                    "chat_id": channel_id,
                    "message_id": message.id,
                    "has_pdf": has_pdf,
                    "has_photo": has_photo,
                    "entities": entities_phase1,
                    "channel_label": channel_label,
                    "date": message_date_ist,
                    "match_type": match_type_phase1,
                    "msg_hash": msg_hash,
                    # Flag: needs OCR pass if photo present, text didn't match, AND channel is OCR channel
                    "needs_ocr": has_photo and not text_matched and channel_id in ocr_channel_ids,
                    "text_matched": text_matched,
                })
                
                if text_matched:
                    if match_type_phase1 == "Portfolio Stock":
                        channel_logger.info(f"[MATCHED TEXT (SCAN)] Portfolio stocks found: {', '.join(entities_phase1)}")
                    else:
                        channel_logger.info(f"[MATCHED TEXT (SCAN)] Macro keywords found: {', '.join(entities_phase1)}")
                    bot_activity_logger.info(f"[MATCHED TEXT] Found {match_type_phase1}: {', '.join(entities_phase1)} in message: {deep_link}")
                else:
                    channel_logger.info(f"[DEFERRED (SCAN)] Photo message needs OCR to determine match: {deep_link}")
                    bot_activity_logger.info(f"[DEFERRED OCR] Photo-only message will be checked after text pass: {deep_link}")
                
                messages_matched_this_channel += 1
                bot_activity_logger.info(f"Adding message to list: {deep_link}")
            
            # Update progress one last time when channel is done
            await scan_progress.update_progress(
                "🔍 Scanning monitored channels for last 24 hour messages...",
                f"Current: {channel_label} ({'OCR ENABLED' if is_ocr_channel else 'NON-OCR'}) - DONE\n"
                f"Channel progress: {messages_scanned} messages scanned (max {max_messages_per_channel} for safety, {messages_skipped} skipped)\n"
                f"Overall: {i}/{len(monitored_ids)} channels scanned"
            )
            bot_activity_logger.info(f"Channel {channel_label} done: scanned {messages_scanned}, skipped {messages_skipped}, matched {messages_matched_this_channel}, stopped early: {messages_beyond_cutoff >=5}")
        except Exception as e:
            logger.error(f"Error scanning channel {channel_id}: {e}")
            channel_logger.error(f"Error scanning channel {channel_id}: {e}")
            bot_activity_logger.error(f"Error scanning channel {channel_id}: {e}")
            import traceback
            bot_activity_logger.error(f"Stack trace: {traceback.format_exc()}")
            continue
    
    # Deduplicate all_matched_messages right after collecting to prevent duplicates
    seen_msg_hashes = set()
    deduplicated_matched_messages = []
    for msg in all_matched_messages:
        msg_hash = msg.get('msg_hash') or f"{msg['chat_id']}_{msg['message_id']}"
        if msg_hash not in seen_msg_hashes:
            seen_msg_hashes.add(msg_hash)
            deduplicated_matched_messages.append(msg)
    all_matched_messages = deduplicated_matched_messages
    
    # Finalize scan progress first
    await scan_progress.finalize_progress(f"✅ Channel scan complete!\n{len(all_matched_messages)} initial matches found.")

    # ── PASS 2: Sequential OCR for deferred photo messages ──────────────────────
    # These are messages where text alone didn't match but a photo is present.
    # We process them one at a time to respect rate limits.
    deferred_ocr_messages = [m for m in all_matched_messages if m.get("needs_ocr")]
    OCR_BATCH_THRESHOLD = 10  # if more than this many images, send OCR results as a separate file
    ocr_matched_count = 0
    ocr_no_match_count = 0

    if deferred_ocr_messages:
        # Initialize OCR progress manager
        ocr_progress = ProgressManager(bot, OWNERS)
        await ocr_progress.send_initial_progress(
            f"🔍 Starting OCR scan of {len(deferred_ocr_messages)} images...",
            f"Progress: 0/{len(deferred_ocr_messages)}\n"
            f"✅ Matches found: 0\n"
            f"❌ No match: 0"
        )
        bot_activity_logger.info(f"="*80)
        bot_activity_logger.info(f"PASS 2: Running OCR on {len(deferred_ocr_messages)} deferred photo message(s)")
        bot_activity_logger.info(f"="*80)

        send_as_file = len(deferred_ocr_messages) > OCR_BATCH_THRESHOLD
        if send_as_file:
            bot_activity_logger.info(f"Large image batch ({len(deferred_ocr_messages)} images > threshold {OCR_BATCH_THRESHOLD}). OCR results will be sent as a text file.")
        
        # Initialize OCR results files with proper format
        now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        ocr_portfolio_content = f"OCR Results - Portfolio Stock Matches\nGenerated on: {now_ist_str}\n\n"
        ocr_macro_content = f"OCR Results - Macro Economy Matches\nGenerated on: {now_ist_str}\n\n"
        ocr_matched_messages_list = []
        ocr_matched_idx = 0
        for i, msg in enumerate(deferred_ocr_messages, 1):
            try:
                bot_activity_logger.info(f"  [{i}/{len(deferred_ocr_messages)}] Processing photo in message {msg['message_id']} from {msg['channel_label']}")
                original_message = await user.get_messages(msg['chat_id'], ids=msg['message_id'])
                # Download to memory — no disk write
                photo_bytes = await user.download_media(original_message, file=bytes)
                if photo_bytes:
                    # Compute image hash for caching
                    image_hash = hashlib.sha256(photo_bytes).hexdigest()
                    
                    # Check cache first
                    cached_result = await get_cached_ocr_result(image_hash)
                    if cached_result:
                        ocr_text = cached_result.get("extracted_text")
                        msg["ocr_text"] = ocr_text
                        bot_activity_logger.info(f"  Using cached OCR result for {msg['deep_link']} (hash={image_hash[:10]}...)")
                        ocr_logger.info(f"════════════════════════════════════════════════════════════")
                        ocr_logger.info(f"📷 Image: {msg['deep_link']}")
                        ocr_logger.info(f"✅ Using CACHED OCR")
                        ocr_logger.info(f"   Match type: {cached_result.get('match_type')}")
                        ocr_logger.info(f"   Matched entities: {cached_result.get('matched_entities')}")
                        ocr_logger.info(f"   Extracted text: {ocr_text[:200]}...")
                        
                        # Re-run filter just in case portfolio/macro keywords changed
                        combined_with_ocr = msg["text"] or ""
                        if msg.get("pdf_filename"):
                            combined_with_ocr += f"\n\n--- PDF FILENAME ---\n{msg['pdf_filename']}"
                        combined_with_ocr += f"\n\n--- OCR TEXT ---\n{ocr_text}"
                        is_matched, match_type, match_details = await execute_two_tier_filter(combined_with_ocr)
                        entities = [d["name"] for d in match_details if d["type"] == "stock" and not d["excluded"]] if match_type == "Portfolio Stock" else [d["name"] for d in match_details if d["type"] == "macro"]
                        
                        # Log match details from OCR
                        if match_details:
                            for detail in match_details:
                                if detail["type"] == "stock":
                                    if detail["excluded"]:
                                        channel_logger.info(f"[SCAN OLD MESSAGES] EXCLUDED MATCH (OCR) - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}, Matched Exclusions: {', '.join(detail['matched_exclusions'])}")
                                    else:
                                        channel_logger.info(f"[SCAN OLD MESSAGES] MATCHED (OCR) - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}")
                                else:
                                    channel_logger.info(f"[SCAN OLD MESSAGES] MATCHED (OCR) - Macro Keyword: {', '.join(detail['matched_positive'])}")
                        
                        # Update the cache with new match info if needed
                        await save_ocr_result_to_db(
                            image_hash, ocr_text, msg["deep_link"], match_type, entities, is_matched
                        )
                    else:
                        # No cached result — run OCR
                        bot_activity_logger.info(f"  No cached OCR result, running OCR on {msg['deep_link']}")
                        ocr_logger.info(f"════════════════════════════════════════════════════════════")
                        ocr_logger.info(f"📷 Image: {msg['deep_link']}")
                        ocr_logger.info(f"🔍 Running NEW OCR")
                        ocr_text = await image_to_text(db, photo_bytes, bot, OWNERS)
                        msg["ocr_text"] = ocr_text
                        bot_activity_logger.info(f"  OCR done: {ocr_text[:150]}...")
                        channel_logger.info(f"[OCR (SCAN PASS 2)] {msg['deep_link']}: {ocr_text[:150]}...")

                        # Re-run filter with OCR text included
                        combined_with_ocr = msg["text"] or ""
                        if msg.get("pdf_filename"):
                            combined_with_ocr += f"\n\n--- PDF FILENAME ---\n{msg['pdf_filename']}"
                        combined_with_ocr += f"\n\n--- OCR TEXT ---\n{ocr_text}"
                        is_matched, match_type, match_details = await execute_two_tier_filter(combined_with_ocr)
                        entities = [d["name"] for d in match_details if d["type"] == "stock" and not d["excluded"]] if match_type == "Portfolio Stock" else [d["name"] for d in match_details if d["type"] == "macro"]
                        
                        # Log match details from OCR
                        if match_details:
                            for detail in match_details:
                                if detail["type"] == "stock":
                                    if detail["excluded"]:
                                        channel_logger.info(f"[SCAN OLD MESSAGES] EXCLUDED MATCH (OCR) - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}, Matched Exclusions: {', '.join(detail['matched_exclusions'])}")
                                    else:
                                        channel_logger.info(f"[SCAN OLD MESSAGES] MATCHED (OCR) - Stock: {detail['name']}, Matched Positives: {', '.join(detail['matched_positive'])}")
                                else:
                                    channel_logger.info(f"[SCAN OLD MESSAGES] MATCHED (OCR) - Macro Keyword: {', '.join(detail['matched_positive'])}")
                        
                        # Save result to cache
                        await save_ocr_result_to_db(
                            image_hash, ocr_text, msg["deep_link"], match_type, entities, is_matched
                        )
                    
                    # Update counters
                    if is_matched:
                        ocr_matched_count +=1
                        ocr_matched_idx +=1
                        ocr_matched_messages_list.append(msg)
                    else:
                        ocr_no_match_count +=1
                    
                    # Update progress messages using our ProgressManager
                    await ocr_progress.update_progress(
                        "🔍 OCR in progress...",
                        f"Progress: {i}/{len(deferred_ocr_messages)}\n"
                        f"Current: {msg['channel_label']} (Msg ID: {msg['message_id']})\n"
                        f"✅ Matches found: {ocr_matched_count}\n"
                        f"❌ No match: {ocr_no_match_count}"
                    )
                    
                    # Process the match result
                    if is_matched:
                        msg["match_type"] = match_type
                        msg["entities"] = entities
                        msg["text_matched"] = True  # now it matched via OCR
                        bot_activity_logger.info(f"  OCR match: {match_type} — {', '.join(entities)}")
                        channel_logger.info(f"[MATCHED VIA OCR (SCAN)] {match_type} — {', '.join(entities)}")
                        ocr_logger.info(f"✅ MATCH FOUND")
                        ocr_logger.info(f"   Match type: {match_type}")
                        ocr_logger.info(f"   Matched entities: {', '.join(entities)}")
                        ocr_logger.info(f"   Extracted text: {ocr_text[:200]}...")
                        ocr_logger.info(f"════════════════════════════════════════════════════════════\n")
                    else:
                        # Still no match even after OCR — mark for removal
                        msg["ocr_no_match"] = True
                        bot_activity_logger.info(f"  Still no match after OCR — will exclude from results")
                        channel_logger.info(f"[FILTERED VIA OCR (SCAN)] No match even after OCR: {msg['deep_link']}")
                        ocr_logger.info(f"❌ No match found")
                        ocr_logger.info(f"════════════════════════════════════════════════════════════\n")
                    
                    if send_as_file and is_matched:
                        # Build proper OCR entry in same format as last 24h news files
                        match_type_str = "📊 Portfolio Stock Match" if match_type == "Portfolio Stock" else "🌐 Macro Economy Match"
                        ocr_entry = f"--- Image {ocr_matched_idx} ---\n"
                        ocr_entry += f"Match Type: {match_type_str}\n"
                        ocr_entry += f"Posted on: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                        ocr_entry += f"From Channel: {msg['channel_label']}\n"
                        ocr_entry += f"Link: {msg['deep_link']}\n"
                        ocr_entry += f"Matched Entities: {', '.join(entities)}\n"
                        if msg["text"]:
                            ocr_entry += f"Content:\n{msg['text']}\n"
                        ocr_entry += f"OCR Text:\n{ocr_text}\n"
                        if msg.get("pdf_filename"):
                            ocr_entry += f"PDF Filename:\n{msg['pdf_filename']}\n"
                        ocr_entry += "\n"
                        
                        if match_type == "Portfolio Stock":
                            ocr_portfolio_content += ocr_entry
                        else:
                            ocr_macro_content += ocr_entry
            except Exception as e:
                logger.error(f"[OCR Pass 2] Failed on message {msg['message_id']}: {e}")
                bot_activity_logger.error(f"  OCR failed for {msg['deep_link']}: {e}")
                msg["ocr_no_match"] = True

        # Remove messages that failed to match even after OCR
        all_matched_messages = [m for m in all_matched_messages if not m.get("ocr_no_match")]

        # Finalize OCR progress
        await ocr_progress.finalize_progress(
            f"✅ OCR scan complete!\n"
            f"Processed: {len(deferred_ocr_messages)} images\n"
            f"✅ Matches found: {ocr_matched_count}\n"
            f"❌ No match: {ocr_no_match_count}\n"
            f"Preparing final files..."
        )

        # Send OCR results as files if batch was large
        if send_as_file and (ocr_portfolio_content != f"OCR Results - Portfolio Stock Matches\nGenerated on: {now_ist_str}\n\n" or ocr_macro_content != f"OCR Results - Macro Economy Matches\nGenerated on: {now_ist_str}\n\n"):
            for owner in OWNERS:
                try:
                    owner_entity = PeerUser(int(owner))
                    
                    # Send OCR Portfolio results if there are any
                    if ocr_portfolio_content != f"OCR Results - Portfolio Stock Matches\nGenerated on: {now_ist_str}\n\n":
                        ocr_portfolio_bytes = ocr_portfolio_content.encode("utf-8")
                        ocr_portfolio_bytesio = io.BytesIO(ocr_portfolio_bytes)
                        ocr_portfolio_filename = f"OCR_Portfolio_Results_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
                        ocr_portfolio_bytesio.name = ocr_portfolio_filename
                        await bot.send_file(
                            owner_entity,
                            ocr_portfolio_bytesio,
                            caption=f"🖼️ OCR Portfolio Results — {len(deferred_ocr_messages)} images processed",
                            link_preview=False
                        )
                        bot_activity_logger.info(f"✓ OCR Portfolio results file sent to {owner}")
                    
                    # Send OCR Macro results if there are any
                    if ocr_macro_content != f"OCR Results - Macro Economy Matches\nGenerated on: {now_ist_str}\n\n":
                        ocr_macro_bytes = ocr_macro_content.encode("utf-8")
                        ocr_macro_bytesio = io.BytesIO(ocr_macro_bytes)
                        ocr_macro_filename = f"OCR_Macro_Results_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
                        ocr_macro_bytesio.name = ocr_macro_filename
                        await bot.send_file(
                            owner_entity,
                            ocr_macro_bytesio,
                            caption=f"🖼️ OCR Macro Results — {len(deferred_ocr_messages)} images processed",
                            link_preview=False
                        )
                        bot_activity_logger.info(f"✓ OCR Macro results file sent to {owner}")
                    
                    # Tell user to wait for final files
                    await bot.send_message(
                        owner_entity,
                        "📂 Preparing final summary files, please wait...",
                        link_preview=False
                    )
                except Exception as e:
                    logger.error(f"Failed to send OCR results files to {owner}: {e}")
        else:
            # If no OCR results files, still tell user to wait for final files
            for owner in OWNERS:
                try:
                    owner_entity = PeerUser(int(owner))
                    await bot.send_message(
                        owner_entity,
                        "📂 Preparing final summary files, please wait...",
                        link_preview=False
                    )
                except Exception as e:
                    logger.error(f"Failed to send wait message to {owner}: {e}")
    else:
        # No OCR needed, still tell user to wait for final files
        for owner in OWNERS:
            try:
                owner_entity = PeerUser(int(owner))
                await bot.send_message(
                    owner_entity,
                    "📂 Preparing final summary files, please wait...",
                    link_preview=False
                )
            except Exception as e:
                logger.error(f"Failed to send wait message to {owner}: {e}")

    # Now separate messages to forward (apply hash checks to skip already processed)
    messages_to_forward = []
    seen_hashes_in_scan = set()
    for msg in all_matched_messages:
        msg_hash = msg.get('msg_hash')
        if msg_hash:
            # Check if we've already seen this hash in this scan
            if msg_hash in seen_hashes_in_scan:
                bot_activity_logger.info(f"[SKIPPED (SCAN)] Duplicate in scan: {msg['deep_link']}")
                continue
            # Check if we've already processed this message before
            if await db["recent_news_hashes"].find_one({"_id": msg_hash}):
                channel_logger.info(f"[FILTERED (SCAN)] Already processed message - Skipping forwarding")
                bot_activity_logger.info(f"[SKIPPED] Already processed: {msg['deep_link']}")
                continue
            seen_hashes_in_scan.add(msg_hash)
        messages_to_forward.append(msg)
    
    # For final text files: use all unique matched messages (deduplicated within the scan, including OCR)
    unique_for_final_text_files = []
    seen_hashes_for_final_text = set()
    for msg in all_matched_messages:
        msg_hash = msg.get('msg_hash') or str(msg['message_id']) + str(msg['chat_id'])
        if msg_hash not in seen_hashes_for_final_text:
            seen_hashes_for_final_text.add(msg_hash)
            unique_for_final_text_files.append(msg)
    
    bot_activity_logger.info(f"Total matched messages: {len(all_matched_messages)}, unique for final text files: {len(unique_for_final_text_files)}, to forward: {len(messages_to_forward)}")
    
    # Split into portfolio and macro for final text files
    final_portfolio_messages = [m for m in unique_for_final_text_files if m["match_type"] == "Portfolio Stock"]
    final_macro_messages = [m for m in unique_for_final_text_files if m["match_type"] == "Macro Economy"]
    
    # Split into portfolio and macro for forwarding
    portfolio_messages_forward = [m for m in messages_to_forward if m["match_type"] == "Portfolio Stock"]
    macro_messages_forward = [m for m in messages_to_forward if m["match_type"] == "Macro Economy"]
    
    # Collect all unique entities/stock names
    all_portfolio_stocks = set()
    all_macro_keywords = set()
    for msg in unique_for_final_text_files:
        if msg["match_type"] == "Portfolio Stock":
            all_portfolio_stocks.update(msg["entities"])
        else:
            all_macro_keywords.update(msg["entities"])
    
    # Build notification message for forwarding
    notification_parts = []
    notification_parts.append(f"🔍 **Last 24 Hour Data Scan (OCR Complete)**\n")
    notification_parts.append(f"Total matched messages: {len(all_matched_messages)}\n")
    notification_parts.append(f"Unique for final text files: {len(unique_for_final_text_files)}\n")
    notification_parts.append(f"Messages to forward: {len(messages_to_forward)}\n")
    if all_portfolio_stocks:
        notification_parts.append(f"📈 **Portfolio News**: {', '.join(sorted(all_portfolio_stocks))}")
    if all_macro_keywords:
        notification_parts.append(f"🌐 **Macro News**: {', '.join(sorted(all_macro_keywords))}")
    notification_text = "\n".join(notification_parts)
    
    # Generate final text file contents using all unique matched messages (including OCR)
    now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    final_portfolio_content = f"Last 24 Hour Portfolio News\nGenerated on: {now_ist_str}\n\n"
    final_macro_content = f"Last 24 Hour Macro News\nGenerated on: {now_ist_str}\n\n"
    
    # Fill portfolio messages (all unique, including OCR)
    for idx, msg in enumerate(final_portfolio_messages, 1):
        match_type_str = "📊 Portfolio Stock Match"
        final_portfolio_content += f"--- Message {idx} ---\n"
        final_portfolio_content += f"Match Type: {match_type_str}\n"
        final_portfolio_content += f"Posted on: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
        final_portfolio_content += f"From Channel: {msg['channel_label']}\n"
        final_portfolio_content += f"Matched Entities: {', '.join(msg['entities'])}\n"
        if msg["text"]:
            final_portfolio_content += f"Content:\n{msg['text']}\n"
        if msg.get("ocr_text"):
            final_portfolio_content += f"OCR Text:\n{msg['ocr_text']}\n"
        if msg.get("pdf_filename"):
            final_portfolio_content += f"PDF Filename:\n{msg['pdf_filename']}\n"
        final_portfolio_content += "\n"
    
    # Fill macro messages (all unique, including OCR)
    for idx, msg in enumerate(final_macro_messages, 1):
        match_type_str = "🌐 Macro Economy Match"
        final_macro_content += f"--- Message {idx} ---\n"
        final_macro_content += f"Match Type: {match_type_str}\n"
        final_macro_content += f"Posted on: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
        final_macro_content += f"From Channel: {msg['channel_label']}\n"
        final_macro_content += f"Matched Entities: {', '.join(msg['entities'])}\n"
        if msg["text"]:
            final_macro_content += f"Content:\n{msg['text']}\n"
        if msg.get("ocr_text"):
            final_macro_content += f"OCR Text:\n{msg['ocr_text']}\n"
        if msg.get("pdf_filename"):
            final_macro_content += f"PDF Filename:\n{msg['pdf_filename']}\n"
        final_macro_content += "\n"
    
    logger.info(f"Found {len(unique_for_final_text_files)} last 24 hour unique messages for final text files, {len(messages_to_forward)} to forward.")
    bot_activity_logger.info(f"="*80)
    bot_activity_logger.info(f"STARTING TO FORWARD LAST 24 HOUR MESSAGES ({len(messages_to_forward)} to forward)")
    bot_activity_logger.info(f"="*80)
    
    # Forward to owners
    for owner in OWNERS:
        try:
            owner_entity = PeerUser(int(owner))
            
            bot_activity_logger.info(f"Sending forwarding summary to owner: {owner}")
            await bot.send_message(
                owner_entity,
                f"{notification_text}\n\n"
                f"Forwarding new messages now, plus final summary text files...",
                link_preview=False
            )
            bot_activity_logger.info(f"✓ Forwarding summary sent to owner: {owner}")
            
            for idx, msg in enumerate(messages_to_forward, 1):
                bot_activity_logger.info(f"[{idx}/{len(messages_to_forward)}] Processing message: {msg['deep_link']} (Entities: {', '.join(msg['entities'])}, Type: {msg['match_type']}, date: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')})")
                
                try:
                    if msg["match_type"] == "Macro Economy":
                        # Skip sending individual macro links, just log
                        bot_activity_logger.info(f"  Skipping macro message (only included in text file): {msg['deep_link']}")
                    else:
                        match_label = "📊 Portfolio Stock"
                        header = (
                            f"📜 **Last 24 Hour {match_label} Match**\n"
                            f"Matched: **{', '.join(msg['entities'])}**\n"
                            f"From: {msg['channel_label']}\n"
                            f"Date: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                        )
                        
                        # Handle photo if present
                        if msg.get("has_photo") and user:
                            try:
                                bot_activity_logger.info(f"  Processing photo from message {msg['message_id']}...")
                                original_message = await user.get_messages(msg['chat_id'], ids=msg['message_id'])
                                photo_file = await user.download_media(original_message, file=bytes)
                                if photo_file:
                                    photo_bytesio = io.BytesIO(photo_file)
                                    photo_bytesio.name = f"photo_{msg['message_id']}.jpg"
                                    caption = header + f"Source: {msg['deep_link']}"
                                    if msg.get("text"):
                                        caption += f"\n\n{msg['text']}"
                                    if msg.get("ocr_text"):
                                        caption += f"\n\n--- OCR Text ---\n{msg['ocr_text']}"
                                    await bot.send_file(
                                        owner_entity,
                                        photo_bytesio,
                                        caption=caption,
                                        link_preview=False
                                    )
                                    bot_activity_logger.info(f"  ✓ Photo sent to {owner}")
                                    continue
                            except Exception as photo_err:
                                logger.error(f"Failed to send photo: {photo_err}")
                                bot_activity_logger.error(f"  ✗ Failed to send photo: {photo_err}")
                        
                        # Handle PDF if present
                        if msg.get("has_pdf") and user:
                            try:
                                bot_activity_logger.info(f"  Processing PDF from message {msg['message_id']}...")
                                original_message = await user.get_messages(msg["chat_id"], ids=msg["message_id"])
                                pdf_filename = extract_real_filename(original_message, msg['entities'][0])
                                pdf_file = await user.download_media(original_message, file=bytes)
                                if pdf_file:
                                    pdf_bytesio = io.BytesIO(pdf_file)
                                    pdf_bytesio.name = pdf_filename
                                    caption = header + f"Source: {msg['deep_link']}"
                                    if msg.get("text"):
                                        caption += f"\n\n{msg['text']}"
                                    if msg.get("pdf_filename"):
                                        caption += f"\n\n--- PDF Filename ---\n{msg['pdf_filename']}"
                                    await bot.send_file(
                                        owner_entity,
                                        pdf_bytesio,
                                        caption=caption,
                                        link_preview=False
                                    )
                                    bot_activity_logger.info(f"  ✓ PDF sent to {owner} with filename: {pdf_filename}")
                                    continue
                            except Exception as pdf_err:
                                logger.error(f"Failed to send PDF: {pdf_err}")
                                bot_activity_logger.error(f"  ✗ Failed to send PDF: {pdf_err}")
                        
                        # Handle text only
                        full_message = header + f"Source: {msg['deep_link']}"
                        if msg.get("text"):
                            full_message += f"\n\n{msg['text']}"
                        await bot.send_message(owner_entity, full_message, link_preview=False)
                        bot_activity_logger.info(f"  ✓ Text sent to {owner}")
                except Exception as e:
                    logger.error(f"Failed to send message to {owner}: {e}")
                    bot_activity_logger.error(f"  ✗ Failed to send to {owner}: {e}")
                
                if msg["match_type"] == "Portfolio Stock":
                    bot_activity_logger.info(f"  ✓ Message [{idx}] sent successfully")
            
            # Send final text files
            # Send final Portfolio News file
            final_portfolio_bytes = final_portfolio_content.encode("utf-8")
            final_portfolio_bytesio = io.BytesIO(final_portfolio_bytes)
            final_portfolio_filename = f"Last_24_Hour_Portfolio_News_Final_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
            final_portfolio_bytesio.name = final_portfolio_filename
            await bot.send_file(
                owner_entity,
                final_portfolio_bytesio,
                caption=f"📄 Final Last 24 Hour Portfolio News Summary - {len(final_portfolio_messages)} messages",
                link_preview=False
            )
            bot_activity_logger.info(f"✓ Final portfolio news text file sent to owner: {owner}")
            
            # Send final Macro News file
            final_macro_bytes = final_macro_content.encode("utf-8")
            final_macro_bytesio = io.BytesIO(final_macro_bytes)
            final_macro_filename = f"Last_24_Hour_Macro_News_Final_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
            final_macro_bytesio.name = final_macro_filename
            await bot.send_file(
                owner_entity,
                final_macro_bytesio,
                caption=f"📄 Final Last 24 Hour Macro News Summary - {len(final_macro_messages)} messages",
                link_preview=False
            )
            bot_activity_logger.info(f"✓ Final macro news text file sent to owner: {owner}")
                
        except Exception as e:
            logger.error(f"Failed to forward last 24 hour messages to {owner}: {e}")
            bot_activity_logger.error(f"✗ Failed to forward messages to owner {owner}: {e}")
            import traceback
            bot_activity_logger.error(f"Stack trace: {traceback.format_exc()}")
    
    # Mark all forwarded messages as processed
    for msg in messages_to_forward:
        if msg['msg_hash']:
            await db["recent_news_hashes"].update_one(
                {"_id": msg['msg_hash']},
                {"$set": {"_id": msg['msg_hash'], "ts": datetime.now(IST)}},
                upsert=True
            )
    
    bot_activity_logger.info("Scan and forwarding complete!")


async def send_empty_scan_text_files():
    """Helper function to send empty text files when no channels are configured or no messages are matched."""
    now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    portfolio_file_content = f"Last 24 Hour Portfolio News\nGenerated on: {now_ist_str}\n\nNo messages found.\n"
    macro_file_content = f"Last 24 Hour Macro News\nGenerated on: {now_ist_str}\n\nNo messages found.\n"
    
    for owner in OWNERS:
        try:
            owner_entity = PeerUser(int(owner))
            
            # Send Portfolio News file
            portfolio_file_bytes = portfolio_file_content.encode("utf-8")
            portfolio_file_bytesio = io.BytesIO(portfolio_file_bytes)
            portfolio_filename = f"Last_24_Hour_Portfolio_News_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
            portfolio_file_bytesio.name = portfolio_filename
            await bot.send_file(
                owner_entity,
                portfolio_file_bytesio,
                caption="📄 Last 24 Hour Portfolio News Summary - 0 messages",
                link_preview=False
            )
            bot_activity_logger.info(f"✓ Portfolio news text file sent to owner: {owner}")
            
            # Send Macro News file
            macro_file_bytes = macro_file_content.encode("utf-8")
            macro_file_bytesio = io.BytesIO(macro_file_bytes)
            macro_filename = f"Last_24_Hour_Macro_News_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
            macro_file_bytesio.name = macro_filename
            await bot.send_file(
                owner_entity,
                macro_file_bytesio,
                caption="📄 Last 24 Hour Macro News Summary - 0 messages",
                link_preview=False
            )
            bot_activity_logger.info(f"✓ Macro news text file sent to owner: {owner}")
        except Exception as e:
            logger.error(f"Failed to send empty text files to {owner}: {e}")
            bot_activity_logger.error(f"✗ Failed to send empty text files to owner {owner}: {e}")


# =====================================================================
# MAIN RUNTIME EXECUTION ENTRY ENGINE TERMINAL OVERVIEW SETUP 
# =====================================================================
async def user_client_run_wrapper():
    """Wrapper for user client run loop that handles Telethon errors gracefully."""
    from telethon.errors import PersistentTimestampOutdatedError, HistoryGetFailedError
    while True:
        try:
            await user.run_until_disconnected()
            break  # Exit loop if disconnected normally
        except PersistentTimestampOutdatedError:
            logger.warning("Telegram persistent timestamp outdated - continuing...")
            await asyncio.sleep(2)  # Wait before reconnecting
        except HistoryGetFailedError:
            logger.warning("Telegram history fetch failed - continuing...")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Unexpected error in user client: {e}")
            await asyncio.sleep(2)

async def main():
    await init_db_defaults()

    logger.info("Starting Telegram clients...")

    # Start bot client (handles commands and UI)
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Bot client started.")

    # Load user session from DB and initialize user client
    from telethon.sessions import StringSession
    config = await get_system_config()
    user_session_str = config.get("user_session", "")
    global user
    user = TelegramClient(
        StringSession(user_session_str),
        api_id=API_ID,
        api_hash=API_HASH
    )
    
    # Start user client (reads channels the user has joined)
    if user_session_str:
        await user.start()
        logger.info("User client started.")
        # Register message handler for user client
        user.add_event_handler(incoming_stream_pipeline, events.NewMessage())
    else:
        logger.warning("No user session found in DB. User client not started. Please set user session via /settings.")

    # No automatic scan on deployment - scan only via /scan_old_messages command

    # Run clients concurrently until disconnected
    tasks = [bot.run_until_disconnected()]
    if user_session_str:  # Only run user client if we have a session
        tasks.append(user_client_run_wrapper())
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Termination sequence detected. Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())