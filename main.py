import os
import io
import re
import csv
import json
import hashlib
import logging
import asyncio
from datetime import datetime, timedelta, timezone

# Indian Standard Time: UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

# Telethon
from telethon import TelegramClient, events, Button
from telethon.tl.types import Channel, Chat, User, DocumentAttributeFilename
from telethon.errors import MessageTooLongError

# Scheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Local modules
from config import API_ID, API_HASH, BOT_TOKEN, OWNERS, MONITORED_CHANNELS, db
from prompt import ANALYSIS_SYSTEM_PROMPT
from gemini import ai_manager
from logs import channel_logger, activity_logger
from webserver import keep_alive

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# State Machine for conversational interactions
# Keys: user_id -> Dict containing 'action' and arbitrary metadata
USER_STATES = {}

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
    doc = await db["config"].find_one({"_id": "gemini_settings"})
    if not doc:
        await db["config"].insert_one({
            "_id": "gemini_settings",
            "keys": [],
            "system_prompt": ANALYSIS_SYSTEM_PROMPT,
            "input_mode": "Text + Images",
            "pdf_analysis_mode": False,
            "monitored_channels": MONITORED_CHANNELS,
            "user_session": ""
        })
    else:
        # Ensure user_session field exists
        if "user_session" not in doc:
            await db["config"].update_one({"_id": "gemini_settings"}, {"$set": {"user_session": ""}})

    # ── Migration: strip legacy 'project' and 'cooldown_until' fields from stored keys ──
    # Old schema: {"key": "...", "project": "...", "cooldown_until": ...}
    # New schema: {"key": "..."} — cooldown is managed in-memory only
    result = await db["config"].update_one(
        {"_id": "gemini_settings"},
        {"$unset": {"keys.$[].project": "", "keys.$[].cooldown_until": ""}}
    )
    if result.modified_count:
        logger.info("Migration: removed legacy 'project' and 'cooldown_until' fields from stored API keys.")

    macro_doc = await db["config"].find_one({"_id": "macro_settings"})
    if not macro_doc:
        await db["config"].insert_one({
            "_id": "macro_settings",
            "macro_keywords": ["RBI", "Nifty", "Bank Nifty", "Budget", "Inflation", "Fed", "Interest Rate"]
        })

    # TTL index: hashes auto-expire after 1 hour so identical reposts within 60 min are dropped
    await db["recent_news_hashes"].create_index("ts", expireAfterSeconds=3600)

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
    return await db["config"].find_one({"_id": "gemini_settings"})

async def generate_ai_variants(stock_name: str) -> dict:
    """Calls Gemini to securely infer variants and exclusion tags."""
    prompt = f"""
    Analyze the stock asset token name: "{stock_name}"
    Provide a robust JSON object map defining variations and exclusion contexts to eliminate noise.
    
    Respond STRICTLY with a valid JSON document using this format:
    {{
      "positive_variants": ["Exact Asset Name", "Ticker Symbol (BSE/NSE)", "Common shorthand abbreviations"],
      "exclusion_variants": ["Explicit sister companies", "Overlapping sectoral names", "Ambiguous dictionary collisions to avoid"]
    }}
    """
    sys_instruction = "You are a data validation logic parser. Output clean JSON only."
    
    try:
        raw_res = await ai_manager.generate_content(
            prompt=prompt, 
            system_instruction=sys_instruction,
            response_mime_type="application/json"
        )
        data = json.loads(raw_res.strip())
        return data
    except Exception as e:
        logger.error(f"Error parsing variant generation payload: {e}")
        return {
            "positive_variants": [stock_name],
            "exclusion_variants": []
        }

# =====================================================================
# TWO-TIER LOCAL FILTERING (ZERO TOKEN WASTE)
# =====================================================================
async def execute_two_tier_filter(text: str) -> tuple[bool, str, list[str]]:
    """
    Evaluates incoming raw context payloads against local caches using word-boundary regex.
    Returns: (is_matched, match_type, list_of_matched_entity_names)
    match_type is "Portfolio Stock" if any portfolio stocks are matched, else "Macro Economy" if any macro keywords matched
    """
    if not text:
        return False, "", []
    
    normalized_text = text.lower()
    
    # Tier 1: Check Specific Stock Portfolio Rules - Sort by longest positive variant first
    stocks = await db["portfolio"].find({}).to_list(length=None)
    stocks_sorted = sorted(
        stocks,
        key=lambda s: len(max(s.get("positive_variants", [s.get("stock_name", "")]), key=len)),
        reverse=True
    )
    
    matched_portfolio = []
    for stock in stocks_sorted:
        positives = stock.get("positive_variants", [])
        exclusions = stock.get("exclusion_variants", [])
        
        # Look for positive matches using word-boundary regex
        matched_positive = False
        for variant in positives:
            pattern = rf"\b{re.escape(variant.lower())}\b"
            if re.search(pattern, normalized_text):
                matched_positive = True
                break
        
        if matched_positive:
            # Check if any exclusions are present in the text (word boundaries)
            has_exclusion = False
            for exc in exclusions:
                exc_pattern = rf"\b{re.escape(exc.lower())}\b"
                if re.search(exc_pattern, normalized_text):
                    has_exclusion = True
                    break
            
            if not has_exclusion:
                matched_portfolio.append(stock.get("stock_name"))
    
    if matched_portfolio:
        return True, "Portfolio Stock", matched_portfolio
    
    # Tier 2: Check Global Macro Economy Keywords (also with word boundaries)
    matched_macro = []
    macro_doc = await db["config"].find_one({"_id": "macro_settings"})
    if macro_doc:
        keywords = macro_doc.get("macro_keywords", [])
        for kw in keywords:
            kw_pattern = rf"\b{re.escape(kw.lower())}\b"
            if re.search(kw_pattern, normalized_text):
                matched_macro.append(kw.upper())
    
    if matched_macro:
        return True, "Macro Economy", matched_macro
                
    return False, "", []

# =====================================================================
# INTERACTIVE SETTINGS MECHANICS (UI GENERATOR)
# =====================================================================
def build_settings_keyboard(mode: str, pdf_mode: bool = False) -> list:
    pdf_label = "🔬 PDF Analysis: ON" if pdf_mode else "🔬 PDF Analysis: OFF"
    keyboard = [
        [
            Button.inline("📊 View Portfolio", data="view_portfolio"),
            Button.inline("➕ Add Stock", data="add_stock")
        ],
        [
            Button.inline("🔑 Manage API Keys", data="manage_keys"),
            Button.inline("📝 Edit Prompt", data="view_prompt")
        ],
        [
            Button.inline("📡 Monitored Channels", data="view_channels"),
            Button.inline(f"⚙️ Mode: {mode}", data="toggle_mode")
        ],
        [
            Button.inline("📥 Download CSV Portfolio", data="export_csv"),
            Button.inline("📤 Upload CSV Bulk", data="prompt_upload_csv")
        ],
        [
            Button.inline("🌐 Macro Keywords", data="view_macro_keywords"),
            Button.inline(pdf_label, data="toggle_pdf_mode")
        ],
        [
            Button.inline("🔐 Manage User Session", data="manage_user_session")
        ]
    ]
    return keyboard

async def _settings_keyboard(config: dict) -> list:
    """Builds the settings keyboard with current mode and pdf_mode from config."""
    return build_settings_keyboard(
        config.get("input_mode", "Text + Images"),
        config.get("pdf_analysis_mode", False)
    )
async def start_command_handler(event: events.NewMessage.Event):
    await event.respond(
        "👋 **Welcome to Market Intelligence Bot!**\n\n"
        "This bot monitors Telegram channels for financial news and analyses them against your portfolio using AI.\n\n"
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
        "/settings - Open settings menu (portfolio, channels, API keys, modes)\n"
        "/add_channel [link/id] - Add a new channel to monitor\n"
        "/remove_channel [link/id] - Stop monitoring a channel\n"
        "/scan_old_messages - Scan last 24h of monitored channels for missed messages\n"
        "/logs - Send today's activity and channel logs\n"
        "/help - Show this command list\n\n"
        "**Settings Menu Features:**\n"
        "• View/Add/Remove Portfolio Stocks\n"
        "• Import/Export Portfolio CSV\n"
        "• Add/Remove Macro Keywords\n"
        "• Manage Gemini API Keys\n"
        "• Toggle PDF Analysis Mode\n"
        "• View/Manage Monitored Channels\n"
        "• Configure System Prompt\n\n"
        "**How It Works:**\n"
        "1. Add portfolio stocks (or import CSV) and macro keywords\n"
        "2. Add channels to monitor\n"
        "3. The bot scans messages for matches and forwards them\n"
        "4. AI analyses portfolio messages (if PDF analysis is enabled)\n\n"
        "**CSV Format:**\n"
        "Columns: stock_name, positive_variants (comma-separated), exclusion_variants (comma-separated)\n"
    )
    await event.respond(help_text, link_preview=False)

@bot.on(events.NewMessage(pattern="/logs"))
async def logs_command_handler(event: events.NewMessage.Event):
    if not is_authorized(event.sender_id):
        return
    
    # Get today's log file paths
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    activity_log_path = os.path.join("activity_logs", f"activity_{today_str}.log")
    channel_log_path = os.path.join("channel_logs", f"channel_activity_{today_str}.log")
    
    files_sent = 0
    
    # Check and send activity log
    if os.path.exists(activity_log_path) and os.path.getsize(activity_log_path) > 0:
        try:
            activity_log_bytes = open(activity_log_path, 'rb').read()
            activity_log_bytesio = io.BytesIO(activity_log_bytes)
            activity_log_bytesio.name = f"activity_{today_str}.txt"
            await bot.send_file(
                event.sender_id,
                activity_log_bytesio,
                caption="📋 Today's Activity Log"
            )
            files_sent +=1
        except Exception as e:
            logger.error(f"Failed to send activity log: {e}")
    
    # Check and send channel log
    if os.path.exists(channel_log_path) and os.path.getsize(channel_log_path) >0:
        try:
            channel_log_bytes = open(channel_log_path, 'rb').read()
            channel_log_bytesio = io.BytesIO(channel_log_bytes)
            channel_log_bytesio.name = f"channel_activity_{today_str}.txt"
            await bot.send_file(
                event.sender_id,
                channel_log_bytesio,
                caption="📡 Today's Channel Activity Log"
            )
            files_sent +=1
        except Exception as e:
            logger.error(f"Failed to send channel log: {e}")
    
    if files_sent ==0:
        await event.respond("📭 No logs available for today yet!")
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
        for s in stocks:
            pos = ', '.join(s.get('positive_variants', []))
            exc = ', '.join(s.get('exclusion_variants', []))
            txt += f"• **{s['stock_name']}**\n"
            txt += f"  ✅ `{pos}`\n"
            if exc:
                txt += f"  ❌ `{exc}`\n"
            txt += "\n"

        nav = []
        if page > 0:
            nav.append(Button.inline("◀️ Prev", data=f"view_portfolio_page:{page - 1}"))
        if (page + 1) * PAGE_SIZE < total:
            nav.append(Button.inline("Next ▶️", data=f"view_portfolio_page:{page + 1}"))

        kbd = []
        if nav:
            kbd.append(nav)
        kbd.append([Button.inline("⬅️ Back", data="back_to_settings")])
        try:
            await event.edit(txt, buttons=kbd)
        except MessageTooLongError:
            await event.answer("⚠️ Page content too long, try exporting CSV instead.", alert=True)

    elif data == "add_stock":
        USER_STATES[user_id] = {"action": "AWAITING_STOCK_NAME"}
        await event.edit(
            "📝 Enter the **Stock Name** you wish to onboard (e.g., `Reliance Industries`):",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "manage_keys":
        keys_list = config.get("keys", [])
        txt = "🔑 **Gemini API Key Cluster**\n\n"
        if not keys_list:
            txt += "No API keys registered yet."
        else:
            for idx, k in enumerate(keys_list):
                masked_key = f"{k['key'][:6]}...{k['key'][-4:]}"
                cooldown = k.get("cooldown_until", None)
                if cooldown is None:
                    status = "🟢 Active"
                else:
                    if isinstance(cooldown, str):
                        cooldown = datetime.fromisoformat(cooldown)
                    if cooldown.tzinfo is None:
                        cooldown = cooldown.replace(tzinfo=IST)
                    else:
                        cooldown = cooldown.astimezone(IST)
                    status = "🟢 Active" if cooldown <= datetime.now(IST) else "🔴 Cooling Down"
                txt += f"{idx+1}. Key: `{masked_key}`\n   Status: {status}\n\n"

        kbd = [
            [Button.inline("➕ Add API Key", data="add_key_prompt")],
            [Button.inline("🗑️ Clear All Keys", data="clear_keys")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)

    elif data == "add_key_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_KEY_PAYLOAD"}
        await event.edit(
            "🔑 Send your **Gemini API Key** (just the key, nothing else):",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "clear_keys":
        await db["config"].update_one({"_id": "gemini_settings"}, {"$set": {"keys": []}})
        await event.answer("All API Keys cleared.", alert=True)
        await event.edit("Cleared.", buttons=await _settings_keyboard(config))

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
        await db["config"].update_one({"_id": "gemini_settings"}, {"$set": {"user_session": ""}})
        await event.answer("User session cleared. Please restart the bot for changes to take effect.", alert=True)
        await event.edit("Cleared.", buttons=await _settings_keyboard(config))

    elif data == "view_prompt":
        prompt_txt = config.get("system_prompt", "None")
        # Truncate prompt text to avoid exceeding message length limit
        truncated = prompt_txt[:3500] + "..." if len(prompt_txt) > 3500 else prompt_txt
        txt = f"📝 **Active Core Analysis Prompt Framework:**\n\n```\n{truncated}\n```"
        kbd = [
            [Button.inline("🔄 Reset Default Framework", data="reset_prompt")],
            [Button.inline("⬅️ Back", data="back_to_settings")]
        ]
        await event.edit(txt, buttons=kbd)

    elif data == "reset_prompt":
        await db["config"].update_one({"_id": "gemini_settings"}, {"$set": {"system_prompt": ANALYSIS_SYSTEM_PROMPT}})
        await event.answer("Prompt framework reset to default spec.", alert=True)
        await event.edit("Reset completed.", buttons=await _settings_keyboard(config))

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

    elif data == "add_channel_prompt":
        USER_STATES[user_id] = {"action": "AWAITING_CHANNEL_INPUT"}
        await event.edit(
            "📡 Send the channel **@username**, **invite link** (`https://t.me/...`), or **numeric ID**:",
            buttons=[[Button.inline("❌ Cancel", data="back_to_settings")]]
        )

    elif data == "toggle_mode":
        current_mode = config.get("input_mode", "Text + Images")
        new_mode = "Text Only" if current_mode == "Text + Images" else "Text + Images"
        await db["config"].update_one({"_id": "gemini_settings"}, {"$set": {"input_mode": new_mode}})
        await event.answer(f"Mode: {new_mode}")
        await event.edit(buttons=await _settings_keyboard(await get_system_config()))

    elif data == "toggle_pdf_mode":
        current = config.get("pdf_analysis_mode", False)
        new_val = not current
        await db["config"].update_one({"_id": "gemini_settings"}, {"$set": {"pdf_analysis_mode": new_val}})
        state_label = "ON" if new_val else "OFF"
        await event.answer(f"PDF Analysis: {state_label}")
        await event.edit(buttons=await _settings_keyboard(await get_system_config()))

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
            
        processing_msg = await event.respond(f"🤖 Interrogating Gemini for AI synonym mapping rules structure configurations for **{stock_name}**...")
        variants = await generate_ai_variants(stock_name)
        
        # Enforce consistency
        if stock_name not in variants.get("positive_variants", []):
            variants.setdefault("positive_variants", []).append(stock_name)
            
        await db["portfolio"].update_one(
            {"stock_name": stock_name},
            {"$set": {
                "stock_name": stock_name,
                "positive_variants": variants.get("positive_variants", []),
                "exclusion_variants": variants.get("exclusion_variants", [])
            }},
            upsert=True
        )
        
        USER_STATES.pop(user_id, None)
        await bot.delete_messages(event.chat_id, processing_msg.id)
        await event.respond(
            f"✅ **Added Stock!** Monitoring for `{variants.get('positive_variants')}` "
            f"while explicitly ignoring `{variants.get('exclusion_variants')}`."
        )

    elif state["action"] == "AWAITING_KEY_PAYLOAD":
        text = event.text.strip()
        try:
            val_key = text.strip()
            if not val_key:
                await event.respond("❌ Key cannot be empty.")
                return

            await db["config"].update_one(
                {"_id": "gemini_settings"},
                {"$push": {"keys": {"key": val_key}}}
            )
            USER_STATES.pop(user_id, None)
            await event.respond(f"✅ API key `{val_key[:6]}...{val_key[-4:]}` added successfully.")
        except Exception as e:
            await event.respond(f"❌ Failed to save key: {e}")

    elif state["action"] == "AWAITING_USER_SESSION":
        text = event.text.strip()
        try:
            user_session = text.strip()
            if not user_session:
                await event.respond("❌ User session cannot be empty.")
                return

            await db["config"].update_one(
                {"_id": "gemini_settings"},
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
            # Parse t.me links to extract the username or channel identifier
            # Handles: https://t.me/username, https://t.me/c/1234567/99, @username, or raw ID
            identifier = raw
            if "t.me/c/" in raw:
                # Private channel link: https://t.me/c/1234567890/99 → ID is -100<channel_id>
                part = raw.split("t.me/c/")[1].split("/")[0]
                identifier = int(f"-100{part}")
            elif "t.me/" in raw:
                # Public channel link: extract username
                identifier = raw.split("t.me/")[1].split("/")[0]

            entity = await user.get_entity(identifier)
            channel_id = entity.id

            # Telethon returns bare IDs for channels; store as -100<id> for consistency
            if not str(channel_id).startswith("-"):
                channel_id = int(f"-100{channel_id}")

            label = getattr(entity, "username", None) or getattr(entity, "title", str(channel_id))

            # Check if already monitored
            existing = await db["config"].find_one({"_id": "gemini_settings", "monitored_channels.id": channel_id})
            if existing:
                await bot.delete_messages(event.chat_id, processing_msg.id)
                USER_STATES.pop(user_id, None)
                await event.respond(f"⚠️ `{label}` is already in the monitored list.")
                return

            await db["config"].update_one(
                {"_id": "gemini_settings"},
                {"$push": {"monitored_channels": {"id": channel_id, "label": label}}}
            )
            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)
            await event.respond(f"✅ Now monitoring **{label}** (`{channel_id}`)")

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
            with open(file_path, mode='r', encoding='utf-8') as f:
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
                f"• Skipped (no changes): **{skipped}** stocks\n\n"
                f"_Use the Add Stock button to let Gemini generate smart variants for individual stocks._"
            )
        except Exception as err:
            await event.respond(f"❌ Error processing CSV: `{err}`")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
            USER_STATES.pop(user_id, None)
            await bot.delete_messages(event.chat_id, processing_msg.id)

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

# =====================================================================
# AD-HOC CHANNEL MANAGEMENT COMMANDS
# =====================================================================
async def _resolve_channel_id(identifier) -> tuple[int, str]:
    """Resolve a username, t.me link, or numeric ID to (channel_id, label) using user client."""
    if not user:
        raise RuntimeError("User session not configured. Please set user session via /settings first.")
    raw = str(identifier).strip()
    if "t.me/c/" in raw:
        part = raw.split("t.me/c/")[1].split("/")[0]
        identifier = int(f"-100{part}")
    elif "t.me/" in raw:
        identifier = raw.split("t.me/")[1].split("/")[0]

    entity = await user.get_entity(identifier)
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
            {"_id": "gemini_settings"},
            {"$addToSet": {"monitored_channels": {"id": channel_id, "label": label}}}
        )
        await event.respond(f"✅ Now monitoring **{label}** (`{channel_id}`)")
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
        channel_id, label = await _resolve_channel_id(parts[1].strip())
        await db["config"].update_one(
            {"_id": "gemini_settings"},
            {"$pull": {"monitored_channels": {"id": channel_id}}}
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
    activity_logger.info("="*80)
    activity_logger.info("MANUAL SCAN TRIGGERED VIA /scan_old_messages COMMAND")
    activity_logger.info("="*80)
    try:
        await scan_channels_for_last_24h_portfolio_messages()
        await event.respond("✅ Scan complete! Check your messages for forwarded last 24 hour data and summary files.")
    except Exception as ex:
        logger.error(f"Error during manual scan: {ex}")
        activity_logger.error(f"Error during manual scan: {ex}")
        await event.respond(f"❌ Error during scan: `{ex}`")


# =====================================================================
# DUPLICATE DETECTION: FINGERPRINT + SEMANTIC SIMILARITY GUARD
# =====================================================================

def _normalise(text: str) -> str:
    return re.sub(r'\s+', ' ', text.lower().strip())

async def is_duplicate_news(text_content: str) -> bool:
    """
    Two-gate deduplication check before any Gemini call is made.
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

    # ── Gate 2: semantic similarity against recent logs ─────────────
    window_start = datetime.now(IST) - timedelta(minutes=30)
    recent_logs = await db["news_logs"].find(
        {"timestamp": {"$gte": window_start}}
    ).sort("timestamp", -1).limit(5).to_list(length=5)

    if not recent_logs:
        return False  # Nothing to compare against

    # Build a compact comparison block
    comparison_block = "\n\n---\n\n".join(
        f"[Entry {i+1}]: {log.get('raw_text', '')[:500]}"
        for i, log in enumerate(recent_logs)
    )

    similarity_prompt = (
        "You are a strict news deduplication engine.\n\n"
        "NEW MESSAGE:\n"
        f"{text_content[:800]}\n\n"
        "RECENT PROCESSED MESSAGES (last 30 minutes):\n"
        f"{comparison_block}\n\n"
        "Does the NEW MESSAGE convey the same core news event as ANY of the recent messages above? "
        "Answer with exactly one word: YES or NO."
    )

    try:
        verdict = await ai_manager.generate_content(
            prompt=similarity_prompt,
            system_instruction="You are a binary deduplication classifier. Respond with YES or NO only."
        )
        is_similar = verdict.strip().upper().startswith("YES")
        if is_similar:
            logger.info("[Dedup-Gate2] Semantic similarity match detected. Dropping near-duplicate.")
        return is_similar
    except Exception as e:
        logger.warning(f"[Dedup-Gate2] Similarity check failed ({e}). Allowing message through.")
        return False

# =====================================================================
# BATCH BUFFER: holds unique messages, waits 30s, flushes all at once
# =====================================================================

# Structure: { msg_hash -> {"message": {...}, "entities": list[str], "match_type": str} }
_message_buffer: dict = {}
_buffer_timer_task: asyncio.Task = None
_BATCH_WINDOW = 30  # seconds to wait before flushing buffer


async def _flush_message_buffer():
    """
    Called after BATCH_WINDOW seconds of silence.
    FIRST forwards all unique messages (text and PDF) to owners,
    THEN attempts Gemini analysis (if applicable).
    """
    global _message_buffer, _buffer_timer_task
    messages = list(_message_buffer.values())
    _message_buffer.clear()
    _buffer_timer_task = None
    
    if not messages:
        return

    logger.info(f"[Batch] Flushing {len(messages)} unique message(s).")
    activity_logger.info(f"="*80)
    activity_logger.info(f"PROCESSING BATCH OF {len(messages)} UNIQUE MESSAGES")
    activity_logger.info(f"="*80)
    
    for i, msg_bucket in enumerate(messages, 1):
        item = msg_bucket["message"]
        entities = msg_bucket["entities"]
        activity_logger.info(f"  [{i}] {item['deep_link']} | Entities: {', '.join(entities)}")

    # ── FIRST: Forward all unique messages (text and PDF) to owners ──────────
    activity_logger.info(f"\nSTEP 1: Sending messages to owners first")
    for owner in OWNERS:
        try:
            await bot.send_message(
                owner,
                f"📨 **New Updates**\n"
                f"Unique Messages: {len(messages)}",
                link_preview=False
            )
            
            for msg_bucket in messages:
                item = msg_bucket["message"]
                entities = msg_bucket["entities"]
                match_type = msg_bucket["match_type"]
                try:
                    # First, try to forward the actual message using the user session
                    if not user:
                        raise RuntimeError("User session not configured")
                    owner_entity = await bot.get_entity(owner)
                    await user.forward_messages(owner_entity, item['message_id'], item['chat_id'])
                    activity_logger.info(f"  ✓ Original message forwarded to {owner}")
                    continue  # Skip fallback since forwarding worked
                except Exception as forward_err:
                    logger.warning(f"Failed to forward original message: {forward_err}, falling back to manual send")
                
                # Fallback if forwarding failed
                if match_type == "Macro Economy":
                    await bot.send_message(
                        owner,
                        f"🌐 Macro News: {item['deep_link']}",
                        link_preview=True
                    )
                    activity_logger.info(f"  ✓ Macro news link sent to {owner}")
                else:
                    if item.get("has_pdf"):
                        try:
                            if not user:
                                raise RuntimeError("User session not configured")
                            activity_logger.info(f"  Processing PDF from message {item['message_id']}...")
                            original_message = await user.get_messages(item['chat_id'], ids=item['message_id'])
                            pdf_filename = extract_real_filename(original_message, entities[0])
                            pdf_file = await user.download_media(original_message, file=bytes)
                            if pdf_file:
                                pdf_bytesio = io.BytesIO(pdf_file)
                                pdf_bytesio.name = pdf_filename
                                await bot.send_file(
                                    owner,
                                    pdf_bytesio,
                                    caption=f"PDF about {', '.join(entities)}\nSource: {item['deep_link']}"
                                )
                                activity_logger.info(f"  ✓ PDF sent to {owner} with filename: {pdf_filename}")
                            else:
                                raise Exception("Downloaded file is None")
                        except Exception as pdf_err:
                            logger.error(f"Failed to send PDF: {pdf_err}")
                            activity_logger.error(f"  ✗ Failed to send PDF: {pdf_err}")
                            await bot.send_message(
                                owner,
                                f"⚠️ Could not forward PDF, but here's the source link:\n{item['deep_link']}",
                                link_preview=True
                            )
                    
                    if item.get("text"):
                        await bot.send_message(
                            owner,
                            f"📨 {item['deep_link']}\n\n{item['text']}",
                            link_preview=False
                        )
                        activity_logger.info(f"  ✓ Text sent to {owner}")
                
        except Exception as e:
            logger.error(f"Failed to send messages to {owner}: {e}")
            activity_logger.error(f"  ✗ Failed to send to {owner}: {e}")

    # ── AI Analysis: group messages by first matched entity (for now) or process individually? Let's process portfolio messages together ──────────
    portfolio_messages = [m for m in messages if m["match_type"] == "Portfolio Stock"]
    config = await get_system_config()
    
    if portfolio_messages:
        # Skip AI analysis if pdf_analysis_mode is off and there are only PDFs
        has_only_pdfs = all(not m["message"].get("text") and m["message"].get("has_pdf") for m in portfolio_messages)
        if has_only_pdfs and not config.get("pdf_analysis_mode", False):
            activity_logger.info("Skipping AI analysis: PDF-only messages and PDF analysis mode is off.")
        else:
            combined_messages = ""
            all_links = []
            for i, msg_bucket in enumerate(portfolio_messages, 1):
                item = msg_bucket["message"]
                entities = msg_bucket["entities"]
                if item.get("text"):
                    combined_messages += f"--- MESSAGE {i} (Entities: {', '.join(entities)}) ---\n{item['text']}\nSOURCE: {item['deep_link']}\n\n"
                all_links.append(item["deep_link"])

            if combined_messages.strip() or config.get("pdf_analysis_mode", False):
                primary_link = all_links[0]
                all_links_str = "\n".join(f"{i+1}. {l}" for i, l in enumerate(all_links))

                user_prompt = (
                    f"The following {len(portfolio_messages)} portfolio message(s) were received within a short window.\n"
                    f"Analyse them collectively as one intelligence report.\n\n"
                    f"{combined_messages}"
                    f"ALL SOURCE LINKS:\n{all_links_str}\n\n"
                    f"TELEGRAM_LINK_REFERENCE: {primary_link}"
                )
                
                activity_logger.info(f"\nSTEP 2: Sending portfolio messages to AI for analysis")
                try:
                    raw_analysis = await ai_manager.generate_content(
                        prompt=user_prompt,
                        system_instruction=config.get("system_prompt", ANALYSIS_SYSTEM_PROMPT)
                    )
                    final_output = raw_analysis.replace("{{telegram_link}}", primary_link)

                    await db["news_logs"].insert_one({
                        "timestamp": datetime.now(IST),
                        "batch_size": len(portfolio_messages),
                        "deep_link": primary_link,
                        "all_links": all_links,
                        "raw_text": combined_messages,
                        "matched_entities": [m["entities"] for m in portfolio_messages],
                        "match_type": "Portfolio Stock",
                        "analysis_output": final_output
                    })
                    
                    activity_logger.info(f"  ✓ AI analysis SUCCESSFUL")

                    for owner in OWNERS:
                        try:
                            await bot.send_message(owner, final_output, link_preview=False)
                            activity_logger.info(f"  ✓ Sent AI analysis to {owner}")
                        except Exception as e:
                            logger.error(f"Broadcast failed to {owner}: {e}")
                            activity_logger.error(f"  ✗ Failed to send AI analysis to {owner}: {e}")

                except Exception as e:
                    logger.error(f"[Batch] Gemini call failed: {e}")
                    activity_logger.error(f"  ✗ AI analysis FAILED: {e}")
                    activity_logger.info(f"  (Messages were already forwarded earlier)")


def _schedule_buffer_flush():
    """
    (Re)starts the 30-second countdown for buffer flush.
    If a timer is already running, cancel it and start fresh —
    this means the window extends each time a new message arrives.
    """
    global _buffer_timer_task
    if _buffer_timer_task and not _buffer_timer_task.done():
        _buffer_timer_task.cancel()

    async def _delayed_flush():
        await asyncio.sleep(_BATCH_WINDOW)
        await _flush_message_buffer()

    _buffer_timer_task = asyncio.ensure_future(_delayed_flush())


# =====================================================================
# STREAMING SUBSCRIBER CONSUMPTION TIER (THE LISTENER ENGINE)
# =====================================================================
async def incoming_stream_pipeline(event: events.NewMessage.Event):
    # Filter configuration constraints
    if not (event.is_channel or event.is_group):
        return

    config = await get_system_config()
    monitored_raw = config.get("monitored_channels", [])

    # Support both old format (bare int) and new format ({"id": int, "label": str})
    monitored_ids = set()
    for ch in monitored_raw:
        if isinstance(ch, dict):
            monitored_ids.add(ch["id"])
        else:
            monitored_ids.add(int(ch))

    # Get chat info for logging
    chat = await event.get_chat()
    chat_label = getattr(chat, 'title', getattr(chat, 'username', str(event.chat_id)))

    # Extract source link
    chat_peer = str(event.chat_id).replace("-100", "")
    deep_link = f"https://t.me/c/{chat_peer}/{event.id}"
    if getattr(chat, 'username', None):
        deep_link = f"https://t.me/{chat.username}/{event.id}"

    text_content = event.text or ""

    # Detect PDF: document with mime_type application/pdf
    has_pdf = False
    if event.document:
        mime = getattr(event.document, "mime_type", "") or ""
        if mime == "application/pdf":
            has_pdf = True

    # Log EVERY message we see in monitored channels
    if event.chat_id in monitored_ids:
        log_msg = (
            f"\n{'='*80}\n"
            f"NEW MESSAGE RECEIVED\n"
            f"{'='*80}\n"
            f"Channel: {chat_label} (ID: {event.chat_id})\n"
            f"Message ID: {event.id}\n"
            f"Deep Link: {deep_link}\n"
            f"Has PDF: {has_pdf}\n"
            f"Has Photo: {bool(event.photo)}\n"
            f"Has Document: {bool(event.document)}\n"
        )
        if text_content:
            log_msg += f"\nTEXT CONTENT:\n{text_content}\n"
        else:
            log_msg += "\nTEXT CONTENT: (empty)\n"
        channel_logger.debug(log_msg)

        if event.chat_id not in monitored_ids:
            return

        # Mode filter
        mode = config.get("input_mode", "Text + Images")
        if bool(event.photo) and mode == "Text Only":
            logger.info("Dropping image payload — Text Only mode active.")
            channel_logger.info(f"[FILTERED] Dropped - Image payload, Text Only mode active")
            return

        # Local keyword filter
        is_matched, match_type, entities = await execute_two_tier_filter(text_content)
        
        if not is_matched:
            channel_logger.info(f"[FILTERED] No match - Not in portfolio or macro keywords")
            return

        logger.info(f"[Filter Hit] {match_type} — {', '.join(entities)}. Buffering for batch.")
        channel_logger.info(f"[MATCHED] {match_type} - Entities: {', '.join(entities)}")

        # If text is empty and no PDF, nothing useful to buffer
        if not text_content and not has_pdf:
            channel_logger.info(f"[FILTERED] No text or PDF - Skipping")
            return

        # Exact-hash dedup on text (zero-token gate) — skip for PDF-only messages
        msg_hash = None
        if text_content:
            normalised = _normalise(text_content)
            msg_hash = hashlib.sha256(normalised.encode()).hexdigest()
        else:
            # For PDF-only, create hash from file ID if available, or just skip dedup for now
            if event.document and event.document.file_id:
                msg_hash = hashlib.sha256(event.document.file_id.encode()).hexdigest()
        
        if msg_hash:
            if msg_hash in _message_buffer:
                logger.info(f"[Dedup-Gate1] Duplicate already in buffer. Dropping.")
                channel_logger.info(f"[FILTERED] Duplicate message in buffer (hash: {msg_hash[:16]}...)")
                return
            if await db["recent_news_hashes"].find_one({"_id": msg_hash}):
                logger.info(f"[Dedup-Gate1] Duplicate fingerprint in DB. Dropping.")
                channel_logger.info(f"[FILTERED] Duplicate message (hash: {msg_hash[:16]}...)")
                return
            await db["recent_news_hashes"].insert_one({"_id": msg_hash, "ts": datetime.now(IST)})

        # Add to message buffer and (re)start the flush timer
        _message_buffer[msg_hash or str(event.id)] = {
            "message": {
                "text": text_content,
                "deep_link": deep_link,
                "chat_id": event.chat_id,
                "message_id": event.id,
                "has_pdf": has_pdf,
            },
            "entities": entities,
            "match_type": match_type
        }

        logger.info(f"[Batch] Message buffer now has {len(_message_buffer)} unique message(s). Timer reset to {_BATCH_WINDOW}s.")
        _schedule_buffer_flush()

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
    activity_logger.info(f"Scan started at: " + datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"))
    
    # Calculate cutoff time (24h ago in IST)
    now_ist = datetime.now(IST)
    cutoff_ist = now_ist - timedelta(days=1)
    activity_logger.info(f"Cutoff time for messages: {cutoff_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    config = await get_system_config()
    monitored_raw = config.get("monitored_channels", [])
    activity_logger.info(f"Monitored channels from config: " + str(monitored_raw))
    
    # Get monitored channel IDs
    monitored_ids = set()
    for ch in monitored_raw:
        if isinstance(ch, dict):
            monitored_ids.add(ch["id"])
        else:
            monitored_ids.add(int(ch))
    activity_logger.info(f"Monitored channel IDs: " + str(monitored_ids))
    
    if not monitored_ids:
        logger.info("No monitored channels to scan.")
        channel_logger.info("No monitored channels to scan.")
        activity_logger.info("No monitored channels to scan.")
        return
    
    # Collect unique messages to forward (key: msg_hash, value: message data)
    unique_messages = {}
    max_messages_per_channel = 1000  # Prevent endless scanning
    activity_logger.info(f"Starting to collect matched messages (max {max_messages_per_channel} per channel)...")
    
    for channel_id in monitored_ids:
        try:
            # Get channel info
            chat = await user.get_entity(channel_id)
            channel_label = getattr(chat, 'title', getattr(chat, 'username', str(channel_id)))
            channel_logger.info(f"\nScanning channel: {channel_label} (ID: {channel_id})")
            activity_logger.info(f"Scanning channel: {channel_label} (ID: {channel_id})")
            
            # Fetch messages from the last 24 hours
            messages_scanned = 0
            messages_matched_this_channel = 0
            messages_beyond_cutoff = 0
            
            # Use iter_messages correctly: start from most recent, go backwards
            # Also check message.date explicitly
            async for message in user.iter_messages(
                channel_id, 
                limit=max_messages_per_channel,
                reverse=False,  # False = most recent first (default)
                offset_date=now_ist  # Start from now and go back
            ):
                messages_scanned += 1
                
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
                        activity_logger.info(f"Stopping scan for {channel_label}: found 5+ messages beyond cutoff")
                        break
                    continue
                
                text_content = message.text or ""
                
                # Check if it has a PDF
                has_pdf = False
                if message.document:
                    mime = getattr(message.document, "mime_type", "") or ""
                    if mime == "application/pdf":
                        has_pdf = True
                
                # Get source link for logging
                chat_peer = str(channel_id).replace("-100", "")
                deep_link = f"https://t.me/c/{chat_peer}/{message.id}"
                if getattr(chat, 'username', None):
                    deep_link = f"https://t.me/{chat.username}/{message.id}"
                
                # Log EVERY message we see
                log_msg = (
                    f"\n---\n"
                    f"SCANNED MESSAGE (OLD DATA)\n"
                    f"---\n"
                    f"Message ID: {message.id}\n"
                    f"Message Date (IST): {message_date_ist.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                    f"Deep Link: {deep_link}\n"
                    f"Has PDF: {has_pdf}\n"
                    f"Has Photo: {bool(message.photo)}\n"
                    f"Has Document: {bool(message.document)}\n"
                )
                if text_content:
                    log_msg += f"\nTEXT CONTENT:\n{text_content}\n"
                else:
                    log_msg += "\nTEXT CONTENT: (empty)\n"
                channel_logger.debug(log_msg)
                
                # Check if this message matches our portfolio OR macro keywords
                is_matched, match_type, entities = await execute_two_tier_filter(text_content)
                
                if not is_matched:
                    channel_logger.info(f"[FILTERED (SCAN)] No match - Not in portfolio or macro keywords")
                    continue
                
                if match_type == "Portfolio Stock":
                    channel_logger.info(f"[MATCHED (SCAN)] Portfolio stocks found: {', '.join(entities)}")
                else:  # Macro Economy
                    channel_logger.info(f"[MATCHED (SCAN)] Macro keywords found: {', '.join(entities)}")
                activity_logger.info(f"[MATCHED] Found {match_type}: {', '.join(entities)} in message: {deep_link} (date: {message_date_ist.strftime('%Y-%m-%d %H:%M:%S IST')})")
                
                # Skip if no text and no PDF
                if not text_content and not has_pdf:
                    channel_logger.info(f"[FILTERED (SCAN)] No text or PDF - Skipping")
                    activity_logger.info(f"[SKIPPED] No text/PDF for message: {deep_link}")
                    continue
                
                # Generate message hash for deduplication
                msg_hash = None
                if text_content:
                    normalised = _normalise(text_content)
                    msg_hash = hashlib.sha256(normalised.encode()).hexdigest()
                else:
                    # For PDF-only, create hash from file ID if available
                    if message.document and message.document.file_id:
                        msg_hash = hashlib.sha256(message.document.file_id.encode()).hexdigest()
                
                # Skip if we already have this message
                if msg_hash and msg_hash in unique_messages:
                    channel_logger.info(f"[FILTERED (SCAN)] Duplicate message - Skipping")
                    activity_logger.info(f"[SKIPPED] Duplicate message: {deep_link}")
                    continue
                
                # Check if we've already processed this message before
                if msg_hash and await db["recent_news_hashes"].find_one({"_id": msg_hash}):
                    channel_logger.info(f"[FILTERED (SCAN)] Already processed message - Skipping")
                    activity_logger.info(f"[SKIPPED] Already processed: {deep_link}")
                    continue
                
                messages_matched_this_channel += 1
                activity_logger.info(f"Adding message to forward list: {deep_link}")
                
                # Add to unique messages dict
                unique_messages[msg_hash or str(message.id)] = {
                    "text": text_content,
                    "deep_link": deep_link,
                    "chat_id": channel_id,
                    "message_id": message.id,
                    "has_pdf": has_pdf,
                    "entities": entities,
                    "channel_label": channel_label,
                    "date": message_date_ist,
                    "match_type": match_type,
                    "msg_hash": msg_hash
                }
            
            activity_logger.info(f"Channel {channel_label} done: scanned {messages_scanned}, matched {messages_matched_this_channel}, stopped early: {messages_beyond_cutoff >=5}")
        except Exception as e:
            logger.error(f"Error scanning channel {channel_id}: {e}")
            channel_logger.error(f"Error scanning channel {channel_id}: {e}")
            activity_logger.error(f"Error scanning channel {channel_id}: {e}")
            import traceback
            activity_logger.error(f"Stack trace: {traceback.format_exc()}")
            continue
    
    # Convert unique_messages dict to list
    messages_to_forward = list(unique_messages.values())
    activity_logger.info(f"Total unique matched messages to forward: {len(messages_to_forward)}")
    if not messages_to_forward:
        logger.info("No last 24 hour messages found.")
        activity_logger.info("No last 24 hour messages found during scan.")
        return
    
    # Split into portfolio and macro
    portfolio_messages = [m for m in messages_to_forward if m["match_type"] == "Portfolio Stock"]
    macro_messages = [m for m in messages_to_forward if m["match_type"] == "Macro Economy"]
    
    # Generate text file contents
    now_ist_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    portfolio_file_content = f"Last 24 Hour Portfolio News\nGenerated on: {now_ist_str}\n\n"
    macro_file_content = f"Last 24 Hour Macro News\nGenerated on: {now_ist_str}\n\n"
    
    # Fill portfolio messages
    for idx, msg in enumerate(portfolio_messages, 1):
        portfolio_file_content += f"--- Message {idx} ---\n"
        portfolio_file_content += f"Posted on: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
        portfolio_file_content += f"From Channel: {msg['channel_label']}\n"
        portfolio_file_content += f"Entities (Portfolio Stocks): {', '.join(msg['entities'])}\n"
        if msg["text"]:
            portfolio_file_content += f"Content:\n{msg['text']}\n"
        portfolio_file_content += "\n"
    
    # Fill macro messages
    for idx, msg in enumerate(macro_messages, 1):
        macro_file_content += f"--- Message {idx} ---\n"
        macro_file_content += f"Posted on: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
        macro_file_content += f"From Channel: {msg['channel_label']}\n"
        macro_file_content += f"Entities (Macro Keywords): {', '.join(msg['entities'])}\n"
        if msg["text"]:
            macro_file_content += f"Content:\n{msg['text']}\n"
        macro_file_content += "\n"
    
    logger.info(f"Found {len(messages_to_forward)} last 24 hour unique messages to forward.")
    activity_logger.info(f"="*80)
    activity_logger.info(f"STARTING TO FORWARD LAST 24 HOUR MESSAGES ({len(messages_to_forward)} found)")
    activity_logger.info(f"="*80)
    
    # Forward to owners
    for owner in OWNERS:
        try:
            activity_logger.info(f"Sending summary to owner: {owner}")
            await bot.send_message(
                owner,
                f"🔍 **Last 24 Hour Data Found**\n\n"
                f"Found {len(messages_to_forward)} unique messages (portfolio + macro) from the last 24 hours.\n"
                f"Forwarding them now, plus summary text files...",
                link_preview=False
            )
            activity_logger.info(f"✓ Summary sent to owner: {owner}")
            
            for idx, msg in enumerate(messages_to_forward, 1):
                activity_logger.info(f"[{idx}/{len(messages_to_forward)}] Forwarding message: {msg['deep_link']} (Entities: {', '.join(msg['entities'])}, Type: {msg['match_type']}, date: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')})")
                
                try:
                    # First, try to forward the actual message using the user session
                    # Get owner entity for user session
                    if not user:
                        raise RuntimeError("User session not configured")
                    owner_entity = await bot.get_entity(owner)
                    await user.forward_messages(owner_entity, msg['message_id'], msg['chat_id'])
                    activity_logger.info(f"  ✓ Original message forwarded to {owner}")
                    # Add a note saying it's last 24 hour data
                    await bot.send_message(
                        owner,
                        f"📜 **Last 24 Hour Data** about **{', '.join(msg['entities'])}**\n"
                        f"From: {msg['channel_label']}\n"
                        f"Date: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}",
                        link_preview=False
                    )
                    activity_logger.info(f"  ✓ Message [{idx}] sent successfully")
                    continue  # Skip fallback since forwarding worked
                except Exception as forward_err:
                    logger.warning(f"Failed to forward original message: {forward_err}, falling back to manual send")
                
                # Fallback if forwarding failed
                if msg["match_type"] == "Macro Economy":
                    # For macro news, just send the link
                    await bot.send_message(
                        owner,
                        f"🌐 Last 24 Hour Macro News: {msg['deep_link']}",
                        link_preview=True
                    )
                    activity_logger.info(f"  ✓ Macro news link sent to {owner}")
                else:
                    # For portfolio news, send full content including PDF
                    header = (
                        f"📜 **Last 24 Hour Portfolio Data** about **{', '.join(msg['entities'])}**\n"
                        f"From: {msg['channel_label']}\n"
                        f"Date: {msg['date'].strftime('%Y-%m-%d %H:%M:%S IST')}\n"
                        f"Source: {msg['deep_link']}\n"
                    )
                    await bot.send_message(owner, header, link_preview=False)
                    
                    if msg["has_pdf"]:
                        try:
                            if not user:
                                raise RuntimeError("User session not configured")
                            activity_logger.info(f"  Processing PDF from message {msg['message_id']}...")
                            # Get the original message object
                            original_message = await user.get_messages(msg["chat_id"], ids=msg["message_id"])
                            
                            # Extract the proper filename
                            pdf_filename = extract_real_filename(original_message, msg['entities'][0])
                            
                            # Download and send via bot (most reliable method)
                            logger.debug(f"[PDF SEND (SCAN)] About to download media!")
                            pdf_file = await user.download_media(original_message, file=bytes)
                            logger.debug(f"[PDF SEND (SCAN)] Downloaded file is None? {pdf_file is None}")
                            if pdf_file:
                                # Wrap bytes in BytesIO and set .name attribute for proper filename
                                pdf_bytesio = io.BytesIO(pdf_file)
                                pdf_bytesio.name = pdf_filename
                                logger.debug(f"[PDF SEND (SCAN)] Calling bot.send_file with BytesIO object, name: {pdf_bytesio.name}")
                                await bot.send_file(
                                    owner,
                                    pdf_bytesio,
                                    caption=f"PDF about {', '.join(msg['entities'])} from {msg['channel_label']}\nSource: {msg['deep_link']}"
                                )
                                activity_logger.info(f"  ✓ PDF sent successfully with filename: {pdf_filename}")
                            else:
                                raise Exception("Downloaded file is None")
                        except Exception as pdf_err:
                            logger.error(f"Failed to send PDF: {pdf_err}")
                            activity_logger.error(f"  ✗ Failed to send PDF: {pdf_err}")
                            await bot.send_message(
                                owner,
                                f"⚠️ Could not forward PDF, but here's the source link:\n{msg['deep_link']}",
                                link_preview=True
                            )
                    
                    if msg["text"]:
                        await bot.send_message(
                            owner,
                            f"{msg['text']}",
                            link_preview=False
                        )
                        activity_logger.info(f"  ✓ Text message sent")
                
                activity_logger.info(f"  ✓ Message [{idx}] sent successfully")
            
            # Now send the summary text files!
            # Send Portfolio News file if there are portfolio messages
            if portfolio_messages:
                portfolio_file_bytes = portfolio_file_content.encode("utf-8")
                portfolio_file_bytesio = io.BytesIO(portfolio_file_bytes)
                portfolio_filename = f"Last_24_Hour_Portfolio_News_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
                portfolio_file_bytesio.name = portfolio_filename
                await bot.send_file(
                    owner,
                    portfolio_file_bytesio,
                    caption=f"📄 Last 24 Hour Portfolio News Summary - {len(portfolio_messages)} messages",
                    link_preview=False
                )
                activity_logger.info(f"✓ Portfolio news text file sent to owner: {owner}")
            # Send Macro News file if there are macro messages
            if macro_messages:
                macro_file_bytes = macro_file_content.encode("utf-8")
                macro_file_bytesio = io.BytesIO(macro_file_bytes)
                macro_filename = f"Last_24_Hour_Macro_News_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.txt"
                macro_file_bytesio.name = macro_filename
                await bot.send_file(
                    owner,
                    macro_file_bytesio,
                    caption=f"📄 Last 24 Hour Macro News Summary - {len(macro_messages)} messages",
                    link_preview=False
                )
                activity_logger.info(f"✓ Macro news text file sent to owner: {owner}")
                
        except Exception as e:
            logger.error(f"Failed to forward last 24 hour messages to {owner}: {e}")
            activity_logger.error(f"✗ Failed to forward messages to owner {owner}: {e}")
            import traceback
            activity_logger.error(f"Stack trace: {traceback.format_exc()}")
    
    # Mark all forwarded messages as processed
    for msg in messages_to_forward:
        if msg['msg_hash']:
            await db["recent_news_hashes"].update_one(
                {"_id": msg['msg_hash']},
                {"$set": {"_id": msg['msg_hash'], "ts": datetime.now(IST)}},
                upsert=True
            )
    
    activity_logger.info("Scan and forwarding complete!")


# =====================================================================
# DAILY CONSOLIDATED MASTER SCHEDULER BATCH PROCESSING REPORT
# =====================================================================
async def compile_and_dispatch_daily_report():
    logger.info("Starting Daily Consolidated Portfolio Intelligence Reporting process pipeline...")
    past_24h = datetime.now(IST) - timedelta(days=1)
    
    logs = await db["news_logs"].find({"timestamp": {"$gte": past_24h}}).to_list(length=5000)
    
    if not logs:
        logger.info("No recorded insights entries found within tracking window parameters over the past day period.")
        return

    compiled_data_block = ""
    for idx, item in enumerate(logs):
        # Handle both old (matched_entity) and new (matched_entities) log entries
        if 'matched_entities' in item:
            entity_label = ', '.join([', '.join(ents) for ents in item['matched_entities']])
        else:
            entity_label = item['matched_entity']
        compiled_data_block += f"--- BLOCK ENTRY {idx+1} ({entity_label}) ---\n"
        compiled_data_block += f"{item['analysis_output']}\n\n"

    master_compiler_prompt = f"""
    You are an elite Lead Portfolio Manager. Review the following consolidated block updates from the past 24 hours of intelligence analysis reports:
    
    {compiled_data_block}
    
    Compile a beautifully formatted Master Portfolio Intelligence Summary Report. 
    Group entries logically by high priority alerts (RED/ORANGE first) down to macro indicators.
    Ensure that under EACH analysis block overview item summary inside your report, you print its precise, corresponding clickable source Telegram Deep-Link exactly as provided in the raw block logs.
    """
    
    try:
        master_report_output = await ai_manager.generate_content(
            prompt=master_compiler_prompt,
            system_instruction="You are a data reporting intelligence synthesis execution program engine core framework tracker. Output clean structured markdown text."
        )
        
        for owner in OWNERS:
            try:
                await bot.send_message(
                    owner, 
                    f"📋 **24-HOUR CONSOLIDATED PORTFOLIO INTELLIGENCE MASTER SUMMARY REPORT**\n\n{master_report_output}",
                    link_preview=False
                )
            except Exception as e:
                logger.error(f"Error publishing daily reports compilation to user ID profile {owner}: {e}")
    except Exception as ex:
        logger.error(f"Failed generating automated aggregate system portfolio data report matrix: {ex}")

# =====================================================================
# MAIN RUNTIME EXECUTION ENTRY ENGINE TERMINAL OVERVIEW SETUP 
# =====================================================================
async def main():
    await init_db_defaults()
    await ai_manager.sync_keys()

    # Scheduler runs in IST — daily report at 8:00 PM IST
    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(compile_and_dispatch_daily_report, 'cron', hour=20, minute=0)
    scheduler.start()

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

    # Check if there are any recent news hashes in the last 24 hours
    past_24h = datetime.now(IST) - timedelta(days=1)
    recent_hashes = await db["recent_news_hashes"].count_documents({"ts": {"$gte": past_24h}})
    
    if recent_hashes == 0:
        logger.info("No recent news hashes found in last 24h. Scanning channels for last 24 hour messages...")
        await scan_channels_for_last_24h_portfolio_messages()
    else:
        logger.info(f"Found {recent_hashes} recent news hashes. Skipping channel scan for last 24 hour messages.")

    # Run clients concurrently until disconnected
    tasks = [bot.run_until_disconnected()]
    if user_session_str:  # Only run user client if we have a session
        tasks.append(user.run_until_disconnected())
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Termination sequence detected. Shutting down...")

keep_alive()
if __name__ == "__main__":
    asyncio.run(main())
