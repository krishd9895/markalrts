"""
session.py — Telethon user session management.

Two roles:
  1. STANDALONE CLI   : `python session.py`  →  interactive phone + OTP flow
                         that generates a StringSession and saves it to MongoDB.
  2. RUNTIME LIBRARY  : imported by main.py  →  session status tracking,
                         owner notifications, hot-reload (apply new session
                         without a restart), the hardened user-client run
                         wrapper, and the per-message pipeline safety guard.

Every session/authorization concern belongs in this file — main.py only
imports symbols from here and wires them into the event loop / handlers.
"""

import asyncio
import logging
import os
import sys
import traceback as _tb
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import PeerUser
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    FloodWaitError,
    PersistentTimestampOutdatedError,
    HistoryGetFailedError,
    AuthKeyError,
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    SessionExpiredError,
    UserDeactivatedBanError,
    PhoneNumberBannedError,
)
from telethon.errors.rpcerrorlist import UnauthorizedError

load_dotenv()

bot_activity_logger = logging.getLogger("bot_activity")

# =========================================================================
# PRIVATE LAN IP DETECTION (shared helper — MUST match failover.py)
# =========================================================================
# Private/link-local IP ranges that are NOT unique across physical nodes
# (every Docker/WSL container on every host gets a 10.x.x.x by default).
# The ACTUAL uniqueness key for leader election is the UUID NODE_ID.
# These helpers exist ONLY to prevent misleading diagnostic text.
_PRIVATE_IP_PREFIXES = (
    "10.",
    "192.168.",
    "172.16.", "172.17.", "172.18.", "172.19.", "172.20.",
    "172.21.", "172.22.", "172.23.", "172.24.", "172.25.",
    "172.26.", "172.27.", "172.28.", "172.29.", "172.30.", "172.31.",
    "127.",
    "169.254.",
    "::1", "fe80:",
)


def _is_private_or_link_local_ip(ip: str) -> bool:
    """Return True if ip is in a private/loopback/link-local (non-unique) range."""
    if not ip:
        return True
    ip = ip.strip()
    return any(ip.startswith(pfx) for pfx in _PRIVATE_IP_PREFIXES)


def _ip_for_logs(ip: str, *, with_warning: bool = True) -> str:
    """Render an IP for logs. Private LAN IPs get an explicit non-unique tag."""
    if not ip or _is_private_or_link_local_ip(ip):
        if with_warning:
            return f"{ip or 'unknown'}  ⚠️ PRIVATE LAN — NOT unique per node"
        return f"{ip or 'unknown'} (private LAN, not unique)"
    return ip


# =========================================================================
# FAILOVER NODE IDENTITY  (read from env vars injected by failover.py)
# =========================================================================
# When failover.py is in use it passes node identity as environment
# variables to the child process (main.py).  session.py reads them here
# so it can verify this node is the elected leader before starting the
# Telethon user client — preventing AuthKeyDuplicatedError from two nodes
# sharing the same session simultaneously.
#
# If the env vars are absent (single-node, no failover.py) every guard
# check returns True automatically — zero behavioural change.
#
# IMPORTANT: failover.py is NEVER imported here.  Importing it would run
# its top-level code (MongoDB pool, keep_alive, watchdog state) inside the
# child process, which would break everything.
#
# SAFETY:  The ONLY reliable uniqueness key in this dict is node_id (UUID).
#          node_ip is diagnostic display ONLY — never use it for equality.
# =========================================================================
def _get_failover_identity() -> dict | None:
    """
    Return the HA node identity injected by failover.py as env vars, or
    None if this process was not started by failover (single-node mode).
    """
    node_id    = os.getenv("FAILOVER_NODE_ID", "").strip()
    service_id = os.getenv("FAILOVER_SERVICE_ID", "").strip()
    if not node_id or not service_id:
        return None   # not running under failover
    ip_raw = os.getenv("FAILOVER_NODE_IP", "")
    ip_is_private_env = os.getenv("FAILOVER_NODE_IP_IS_PRIVATE_LAN", "")
    if ip_is_private_env == "1":
        ip_is_private = True
    elif ip_is_private_env == "0":
        ip_is_private = False
    else:
        ip_is_private = _is_private_or_link_local_ip(ip_raw)
    return {
        "node_id":          node_id,
        "node_alias":       os.getenv("FAILOVER_NODE_ALIAS", ""),
        "node_ip":          ip_raw,
        "node_ip_is_private_lan": ip_is_private,
        "service_id":       service_id,
        "database_name":    os.getenv("FAILOVER_DB_NAME", "Failover"),
        "collection_name":  os.getenv("FAILOVER_COLLECTION", "Services"),
        "heartbeat_timeout": int(os.getenv("FAILOVER_HB_TIMEOUT", "60")),
    }

_FAILOVER_IDENTITY: dict | None = _get_failover_identity()

# =========================================================================
# STANDALONE-CLI CONFIG
# =========================================================================
# These values are ONLY used for the `python session.py` CLI.
# main.py reads the same env vars from config.py (imported once, at startup).
# =========================================================================
API_ID_CLI   = int(os.getenv("API_ID", "0"))
API_HASH_CLI = os.getenv("API_HASH", "")
MONGO_URI    = os.getenv("MONGO_URI", "")

if __name__ == "__main__":  # strict guard — validate only when run as script
    if not API_ID_CLI or not API_HASH_CLI:
        print("ERROR: API_ID and API_HASH must be set in your .env file.")
        sys.exit(1)
    if not MONGO_URI:
        print("ERROR: MONGO_URI must be set in your .env file.")
        sys.exit(1)


# ── Shared helpers (safe to import) ────────────────────────────────────────
def _is_imported_from_bot() -> bool:
    """
    True if this file was ``import session`` from the bot runtime; False when
    executed directly as the standalone CLI.  Lets us print() from the CLI
    and use ``logger`` (or raise) when imported by main.py.
    """
    return __name__ != "__main__"


def prompt(msg: str, secret: bool = False) -> str:
    """Read input from the terminal, optionally hiding the typed text."""
    if secret:
        import getpass
        return getpass.getpass(msg).strip()
    return input(msg).strip()


# =========================================================================
# STANDALONE CLI  (kept from the original session.py, slightly polished)
# =========================================================================
async def generate_session() -> str:
    """Interactive CLI flow: phone → OTP (→ 2FA) → StringSession string."""
    if _is_imported_from_bot():
        raise RuntimeError(
            "generate_session() (the interactive terminal flow) is not available "
            "when session.py is imported. Use the bot's /generate_session command instead."
        )

    print("\n" + "=" * 60)
    print("  Telethon User Session Generator")
    print("=" * 60)
    print("This will log in to your Telegram account and create a")
    print("session string that the bot uses to read channels.\n")

    phone = prompt("Enter your phone number (with country code, e.g. +911234567890): ")

    async with TelegramClient(StringSession(), API_ID_CLI, API_HASH_CLI) as client:
        await client.connect()

        # Request the OTP
        try:
            await client.send_code_request(phone)
        except FloodWaitError as e:
            print(f"\nToo many attempts. Please wait {e.seconds} seconds and try again.")
            return ""

        print("\nAn OTP has been sent to your Telegram account / SMS.")

        # Attempt to sign in — retry on bad/expired code
        signed_in = False
        for attempt in range(3):
            code = prompt("Enter the OTP code: ")
            try:
                await client.sign_in(phone, code)
                signed_in = True
                break
            except PhoneCodeInvalidError:
                print("  ✗ Incorrect code. Please try again.")
            except PhoneCodeExpiredError:
                print("  ✗ Code has expired. Requesting a new one...")
                await client.send_code_request(phone)
            except SessionPasswordNeededError:
                # 2FA is enabled
                print("\n  Two-factor authentication is enabled on this account.")
                for pw_attempt in range(3):
                    password = prompt("  Enter your 2FA password: ", secret=True)
                    try:
                        await client.sign_in(password=password)
                        signed_in = True
                        break
                    except Exception as e:
                        print(f"  ✗ Wrong password: {e}")
                break

        if not signed_in:
            if not await client.is_user_authorized():
                print("\nFailed to sign in after multiple attempts. Aborting.")
                return ""

        me = await client.get_me()
        print(
            f"\n  ✓ Signed in as: {me.first_name} "
            f"(@{me.username or 'no username'}, ID: {me.id})"
        )

        session_string = client.session.save()
        return session_string


async def save_to_mongo(session_string: str):
    """Upsert the session string into MongoDB (bot_settings document)."""
    if _is_imported_from_bot():
        # main.py already has its own db handle from config.py; keep this
        # CLI-only routine strictly for the standalone script.
        raise RuntimeError(
            "save_to_mongo() (CLI) is not available when session.py is imported. "
            "Use db['config'].update_one(...) from main.py instead."
        )
    db_client = AsyncIOMotorClient(MONGO_URI)
    try:
        db = db_client["market_intelligence"]
        await db["config"].update_one(
            {"_id": "bot_settings"},
            {"$set": {"user_session": session_string}},
            upsert=True,
        )
    finally:
        db_client.close()


async def _cli_main():
    session_string = await generate_session()

    if not session_string:
        print("\nNo session generated. Exiting.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Session String (keep this private)")
    print("=" * 60)
    print(session_string)
    print("=" * 60)

    # Ask before saving
    save = prompt("\nSave this session to MongoDB so the bot can use it? [Y/n]: ")
    if save.lower() in ("", "y", "yes"):
        try:
            await save_to_mongo(session_string)
            print("\n  ✓ Session saved to MongoDB successfully.")
            print("  Restart the bot to activate the new session.")
        except Exception as e:
            print(f"\n  ✗ Failed to save to MongoDB: {e}")
            print("  You can still copy the session string above and set it manually via /settings.")
    else:
        print("\nNot saved. Copy the session string above if you need it.")

    print()


if __name__ == "__main__":
    asyncio.run(_cli_main())


# =========================================================================
#   RUNTIME SESSION MODULE  (imported by main.py)
# =========================================================================
# Everything below this line exposes symbols that main.py imports and uses.
# It is all still in session.py — because every concern related to the
# user session (status, error classification, owner alerts, hot-reload,
# the run-wrapper, the pipeline guard) belongs here.
# =========================================================================

# -------------------------------------------------------------------------
# A. Session-error classifiers + severity groups
# -------------------------------------------------------------------------

# Exceptions that mean "this session string is dead, stop reconnecting and
# tell the owner to regenerate".  Retrying these is spam.
FATAL_AUTH_ERRORS = (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    AuthKeyError,
    SessionExpiredError,
    UserDeactivatedBanError,
    PhoneNumberBannedError,
    UnauthorizedError,
)


def classify_user_client_error(exc: BaseException) -> str:
    """
    Map a Telethon exception (or any Exception) to one of the friendly
    categories understood by the owner notifier.

    Returns
    -------
    str
        One of:  ``"session_conflict"``, ``"session_expired"``,
        ``"account_banned"``, ``"no_session"``, ``"general"``.
    """
    if isinstance(exc, AuthKeyDuplicatedError):
        return "session_conflict"
    if isinstance(exc, (AuthKeyUnregisteredError, AuthKeyError, UnauthorizedError, SessionExpiredError)):
        return "session_expired"
    if isinstance(exc, (UserDeactivatedBanError, PhoneNumberBannedError)):
        return "account_banned"
    msg = str(exc).lower()
    if "duplicate" in msg or "different ip" in msg or "two locat" in msg:
        return "session_conflict"
    if "expired" in msg or "revoked" in msg or "unauthor" in msg or "auth key" in msg:
        return "session_expired"
    if "ban" in msg or "deactivated" in msg:
        return "account_banned"
    return "general"


# -------------------------------------------------------------------------
# B. Global session status tracker
# -------------------------------------------------------------------------

USER_CLIENT_STATUS: dict = {
    "connected": False,
    "last_error": None,
    "last_error_time": None,
    "consecutive_failures": 0,
    "error_notified": False,
}
"""
Singleton dict (module-level global) that main.py's handlers and the run
wrapper both read and write to keep a consistent view of the user client's
health across the bot command surface.
"""


# -------------------------------------------------------------------------
# B2. Leader-node guard — prevents session conflict across HA nodes
# -------------------------------------------------------------------------

async def is_this_node_the_session_leader() -> tuple[bool, str]:
    """
    Check MongoDB to confirm that *this* process is the current elected
    leader before allowing the Telethon user client to connect.

    Returns
    -------
    (allowed: bool, reason: str)
        allowed=True  → this node is the leader (or failover is not in use)
        allowed=False → another node is the active leader; do NOT start the
                        user client here or Telegram will kill both sessions
                        with AuthKeyDuplicatedError.
    """
    if _FAILOVER_IDENTITY is None:
        # failover.py is not in use — single-node deployment, always allowed.
        return True, "no-failover"

    node_id       = _FAILOVER_IDENTITY.get("node_id")
    node_alias    = _FAILOVER_IDENTITY.get("node_alias")
    node_ip       = _FAILOVER_IDENTITY.get("node_ip", "unknown")
    service_id    = _FAILOVER_IDENTITY.get("service_id")
    db_name       = _FAILOVER_IDENTITY.get("database_name")
    coll_name     = _FAILOVER_IDENTITY.get("collection_name")
    hb_timeout    = _FAILOVER_IDENTITY.get("heartbeat_timeout", 60)
    mongo_uri     = os.getenv("MONGO_URI", "")

    if not mongo_uri or not service_id:
        # Can't verify — allow by default so bot doesn't get stuck.
        return True, "failover-config-missing"

    _log = bot_activity_logger
    try:
        import pymongo
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=4000)
        doc = client[db_name][coll_name].find_one({"_id": service_id})
        client.close()

        if not doc:
            # No record yet — we are probably the very first node bootstrapping.
            return True, "no-leader-record"

        current_leader = doc.get("current_leader", {})
        leader_node_id = current_leader.get("node_id")
        leader_node_alias = current_leader.get("node_alias", "?")
        leader_node_ip = current_leader.get("node_ip", "")
        last_heartbeat = current_leader.get("last_heartbeat")

        # Check if the leader record is still fresh (within heartbeat timeout).
        leader_alive = False
        if last_heartbeat:
            try:
                if last_heartbeat.tzinfo is None:
                    last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - last_heartbeat).total_seconds()
                leader_alive = elapsed < hb_timeout
            except Exception:
                leader_alive = False

        if not leader_alive:
            # No active leader — allow this node to attempt.
            return True, "leader-expired-or-absent"

        if leader_node_id == node_id:
            # This node IS the leader — the UUID node_id is the only authoritative key.
            return True, f"this-node-is-leader node={node_alias or node_id[:12]}"

        # Another node is the active leader.
        #
        # IMPORTANT: We ONLY compare leader_node_id (UUID) above — never compare IPs
        # for uniqueness because the same 10.x.x.x private LAN address can appear on
        # two different physical hosts behind NAT and cause the guard to be wrong.
        # The IPs below are logged purely for diagnostic display; they carry no
        # authority in this function.
        _log.warning(
            "[SessionGuard] Another node holds the leader lease "
            "(leader_node_alias=%s, leader_node_id=%s, leader_ip=%s). "
            "User client will NOT be started on this node to avoid "
            "AuthKeyDuplicatedError.",
            leader_node_alias,
            (leader_node_id or "?")[:16] + "…" if leader_node_id and len(leader_node_id) > 16 else (leader_node_id or "?"),
            _ip_for_logs(leader_node_ip),
        )
        if leader_node_alias and leader_node_alias != "?":
            return False, (
                f"standby-node — leader is '{leader_node_alias}'"
            )
        return False, (
            f"standby-node — leader node_id="
            f"{(leader_node_id or 'unknown')[:20]}{'…' if leader_node_id and len(leader_node_id) > 20 else ''}"
        )

    except Exception as guard_exc:
        _log.warning(
            "[SessionGuard] Could not verify leader identity (%s). "
            "Allowing user client start as a safe fallback.",
            guard_exc,
        )
        return True, f"guard-check-failed: {guard_exc}"



async def notify_owners_about_user_client_error(
    error_msg: str,
    error_type: str = "general",
    *,
    bot_client: TelegramClient | None = None,
    owners: list[int] | tuple[int, ...] | None = None,
    ist_tz: timezone | None = None,
    clean_text_fn=None,
    cooldown: timedelta = timedelta(hours=1),
) -> None:
    """
    Send an actionable Telegram message to every OWNER when the user client
    has a problem.

    The call is deduplicated (same exact error within ``cooldown`` is
    suppressed) so a fast reconnect loop won't spam owners.
    """
    # ----- Dependency wiring ---------------------------------------------
    # session.py deliberately does NOT import from main.py (would create a
    # circular import).  Instead, main.py injects the tiny deps it has
    # (bot, owners, IST tz, clean_text_for_telegram) right here so the
    # module stays self-contained.  The two fallback branches let this
    # helper degrade gracefully if called before injection completes.
    if bot_client is None:
        from __main__ import bot as _bot  # type: ignore[attr-defined]
        bot_client = _bot
    if owners is None:
        from __main__ import OWNERS as _owners  # type: ignore[attr-defined]
        owners = list(_owners)
    if ist_tz is None:
        ist_tz = timezone(timedelta(hours=5, minutes=30))
    if clean_text_fn is None:
        # Minimal sanitiser if main.py hasn't injected the real one yet.
        def _fallback_clean(text: str, max_length: int = 500) -> str:
            if not text:
                return ""
            t = (text.replace("**", "").replace("*", "")
                       .replace("__", "").replace("_", "")
                       .replace("`", "").replace("```", ""))
            return t if len(t) <= max_length else t[: max_length - 3] + "..."
        clean_text_fn = _fallback_clean

    now = datetime.now(ist_tz)
    prev_error = USER_CLIENT_STATUS.get("last_error", "")
    prev_time  = USER_CLIENT_STATUS.get("last_error_time")
    already_notified = USER_CLIENT_STATUS.get("error_notified", False)

    same_error = (
        already_notified
        and prev_error == error_msg
        and prev_time is not None
        and (now - prev_time) < cooldown
    )

    if same_error:
        bot_activity_logger.info(
            "[UserClient-Error] Suppressing duplicate owner alert "
            "(same error within %s cooldown): %s…",
            cooldown, error_msg[:80],
        )
        return

    USER_CLIENT_STATUS["last_error"]      = error_msg
    USER_CLIENT_STATUS["last_error_time"] = now
    USER_CLIENT_STATUS["error_notified"]  = True

    if error_type == "session_conflict":
        friendly = (
            "⚠️ **USER SESSION CONFLICT DETECTED**\n\n"
            "Your Telethon user session is being used from **two different IPs/devices** at the same time.\n\n"
            "Telegram terminated this session to protect your account.\n\n"
            "**How to fix:**\n"
            "1. Stop any other bot/script using the *same* Telegram user session\n"
            "2. Run `/generate_session` below and complete the phone + OTP flow to create a **fresh session**\n"
            "3. Save it to MongoDB (click the button offered after sign-in)\n\n"
            f"Technical error:\n`{clean_text_fn(error_msg, 500)}`\n"
        )
    elif error_type == "session_expired":
        friendly = (
            "⚠️ **USER SESSION EXPIRED / REVOKED**\n\n"
            "The Telethon user session stored in MongoDB is no longer valid.\n"
            "It may have been revoked by Telegram, or the account logged out elsewhere.\n\n"
            "**How to fix:**\n"
            "Run `/generate_session` and complete the phone + OTP flow to create a fresh session.\n\n"
            f"Technical error:\n`{clean_text_fn(error_msg, 500)}`\n"
        )
    elif error_type == "account_banned":
        friendly = (
            "🚫 **TELEGRAM ACCOUNT BANNED / DEACTIVATED**\n\n"
            "The Telegram user account used by the user client has been banned, deactivated, or restricted.\n\n"
            "**How to fix:**\n"
            "1. Recover the account via Telegram support, OR use a different phone number\n"
            "2. Run `/generate_session` with the new/working account\n\n"
            f"Technical error:\n`{clean_text_fn(error_msg, 500)}`\n"
        )
    elif error_type == "no_session":
        friendly = (
            "ℹ️ **NO USER SESSION CONFIGURED**\n\n"
            "The bot started successfully, but there is no Telethon user session saved in MongoDB yet.\n\n"
            "**How to fix:**\n"
            "Run `/generate_session` now and complete the phone + OTP flow.\n"
            "Without a user session the bot cannot read messages from monitored channels.\n"
        )
    else:
        friendly = (
            f"⚠️ **USER CLIENT ERROR**\n\n"
            f"`{clean_text_fn(error_msg, 800)}`\n\n"
            "If this keeps happening, run `/generate_session` to refresh the user session.\n"
        )

    logger = bot_activity_logger
    logger.error("[UserClient-Error] Notifying owners: %s", error_msg)
    for owner in owners:
        try:
            await bot_client.send_message(
                PeerUser(int(owner)),
                friendly,
                link_preview=False,
            )
            await asyncio.sleep(0.25)
        except Exception as send_err:
            logger.error(
                "[UserClient-Error] Failed to notify owner %s: %s", owner, send_err
            )


# -------------------------------------------------------------------------
# D. Per-message pipeline safety wrapper
# -------------------------------------------------------------------------

def build_safe_pipeline_wrapper(real_pipeline_coro_fn):
    """
    Return a new coroutine-function that calls ``real_pipeline_coro_fn``
    inside a broad try/except so one bad message can never crash the user
    client event loop.

    Usage (main.py)::

        from session import build_safe_pipeline_wrapper
        _safe = build_safe_pipeline_wrapper(incoming_stream_pipeline)
        user.add_event_handler(_safe, events.NewMessage())
    """
    channel_logger = logging.getLogger("channel_activity")

    async def _safe_wrapper(event):
        try:
            await real_pipeline_coro_fn(event)
        except Exception as pipe_exc:
            chat_id = getattr(event, "chat_id", "?")
            msg_id  = getattr(event, "id", "?")
            err_str = f"{type(pipe_exc).__name__}: {pipe_exc}"
            bot_activity_logger.error(
                "[Pipeline] Uncaught error processing msg chat=%s id=%s: %s\n%s",
                chat_id, msg_id, err_str, _tb.format_exc(),
            )
            try:
                channel_logger.error(
                    "[PIPELINE ERROR] chat=%s msg_id=%s: %s", chat_id, msg_id, err_str
                )
            except Exception:
                pass

    return _safe_wrapper


# -------------------------------------------------------------------------
# E. Session hot-reload — "save → live" with no restart
# -------------------------------------------------------------------------

async def apply_user_session_and_reconnect(
    new_session_str: str | None,
    *,
    api_id: int,
    api_hash: str,
    safe_pipeline_coro_fn,
    old_user_client: TelegramClient | None,
) -> tuple[TelegramClient, bool, str]:
    """
    Replace the current user client with one backed by ``new_session_str``
    and attempt ``.start()`` + ``.get_me()`` right away so the operator
    sees an immediate "it worked (or didn't)" response.

    Parameters
    ----------
    new_session_str : str or None
        New StringSession text.  ``None`` / ``""`` = clear/disable.
    api_id, api_hash
        Telegram API credentials (injected from config.py by main.py).
    safe_pipeline_coro_fn
        The already-wrapped pipeline handler (returned by
        :func:`build_safe_pipeline_wrapper`).  Attached to the new client.
    old_user_client : TelegramClient or None
        The previous global ``user`` instance; disconnected & torn down if
        it exists.

    Returns
    -------
    (new_user_client, success, user_facing_message)
    """
    new_session_str = (new_session_str or "").strip()

    # ── 1. Tear down the old client cleanly ────────────────────────────
    if old_user_client is not None:
        try:
            try:
                old_user_client.remove_event_handler(safe_pipeline_coro_fn)
            except Exception:
                pass
            if old_user_client.is_connected():
                await old_user_client.disconnect()
                bot_activity_logger.info(
                    "[SessionReload] Old user client disconnected."
                )
        except Exception as disc_exc:
            bot_activity_logger.warning(
                "[SessionReload] Disconnect of old client noisy: %s", disc_exc
            )

    # ── 2. Build a brand new client ───────────────────────────────────
    new_client = TelegramClient(
        StringSession(new_session_str),
        api_id=api_id,
        api_hash=api_hash,
    )

    USER_CLIENT_STATUS["error_notified"]     = False
    USER_CLIENT_STATUS["consecutive_failures"] = 0

    # Cleared / disabled case — nothing more to start.
    if not new_session_str:
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = "Session explicitly cleared by operator"
        bot_activity_logger.info(
            "[SessionReload] Session cleared; user client disabled."
        )
        return (
            new_client,
            True,
            "✅ Session cleared. The bot remains online; run /generate_session whenever you want to re-enable channel reading.",
        )

    # ── 3. Start + validate the freshly-built client ──────────────────
    bot_activity_logger.info(
        "[SessionReload] Attempting to start user client with freshly saved session..."
    )
    try:
        await new_client.start()
        me = await new_client.get_me()
        new_client.add_event_handler(safe_pipeline_coro_fn, events.NewMessage())
        USER_CLIENT_STATUS["connected"]  = True
        USER_CLIENT_STATUS["last_error"] = None
        name  = getattr(me, "first_name", "?") if me else "?"
        uname = getattr(me, "username", None)   if me else None
        who   = f"{name} (@{uname})" if uname else name
        bot_activity_logger.info(
            "[SessionReload] User client reconnected as %s. No restart needed.", who
        )
        return (
            new_client,
            True,
            (
                "✅ **New session activated immediately — no restart needed!**\n\n"
                f"Signed in as **{who}**.\n\n"
                "Monitored-channel reading is live again."
            ),
        )
    except FATAL_AUTH_ERRORS as auth_exc:
        etype = classify_user_client_error(auth_exc)
        err_str = f"{type(auth_exc).__name__}: {auth_exc}"
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = err_str
        bot_activity_logger.critical(
            "[SessionReload] Saved session rejected (%s): %s", etype, err_str
        )
        # Notify immediately — operator just tried to save a broken session.
        try:
            await notify_owners_about_user_client_error(err_str, etype)
        except Exception:
            pass
        return (
            new_client,
            False,
            (
                "⚠️ The saved session was rejected by Telegram.\n\n"
                f"Error type: **{etype}**\n"
                f"`{err_str[:400]}`\n\n"
                "This usually means another client is using the *same* session at a different IP. "
                "Please re-run `/generate_session` to create a **fresh, unique** session."
            ),
        )
    except FloodWaitError as fw_exc:
        wait_s = int(getattr(fw_exc, "seconds", 60))
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = f"FloodWait after reload: {wait_s}s"
        bot_activity_logger.warning("[SessionReload] FloodWait %ss", wait_s)
        return (
            new_client,
            False,
            (
                f"⏱ Telegram is rate-limiting us right now ({wait_s}s).\n"
                "The user client will auto-reconnect via the run wrapper shortly. "
                "If it keeps failing, re-run `/generate_session` after the cooldown."
            ),
        )
    except Exception as reload_exc:
        etype = classify_user_client_error(reload_exc)
        err_str = f"{type(reload_exc).__name__}: {reload_exc}"
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = err_str
        bot_activity_logger.error(
            "[SessionReload] Apply failed: %s\n%s", err_str, _tb.format_exc()
        )
        try:
            await notify_owners_about_user_client_error(err_str, etype)
        except Exception:
            pass
        return (
            new_client,
            False,
            (
                "⚠️ Could not activate the saved session immediately.\n\n"
                f"`{err_str[:400]}`\n\n"
                "The run-wrapper will retry. If the error persists, re-run `/generate_session`."
            ),
        )


# -------------------------------------------------------------------------
# F. One-shot "start the user client" helper used by main()
# -------------------------------------------------------------------------

async def start_user_client_safely(
    existing_user_client: TelegramClient,
    *,
    session_str: str,
    safe_pipeline_coro_fn,
) -> bool:
    """
    Called from main() after ``user = TelegramClient(...)`` is already
    constructed.  Runs ``.start()`` + ``.get_me()`` + pipeline registration
    with broad, categorized error handling.  Also notifies owners on
    failure.

    Returns True when the client is fully live; False otherwise (main()
    will still start the bot, just without channel reading).
    """
    if not session_str:
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = "No user_session stored in MongoDB"
        bot_activity_logger.warning(
            "[Startup] No user session found in DB. User client NOT started. "
            "Owner must run /generate_session."
        )
        try:
            await notify_owners_about_user_client_error(
                "No user session configured at startup", "no_session",
            )
        except Exception as notify_err:
            bot_activity_logger.error(
                "[Startup] Owner notification (no session) failed: %s", notify_err
            )
        return False

    # ── Leader guard: only start the Telethon user client on the elected leader node ──
    allowed, reason = await is_this_node_the_session_leader()
    if not allowed:
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = f"Not the session leader: {reason}"
        bot_activity_logger.warning(
            "[Startup] SKIPPING user client start — this node is NOT the current "
            "leader. Reason: %s  "
            "The bot will continue without channel reading. "
            "When this node becomes leader it will start the user client automatically.",
            reason,
        )
        # Do NOT notify owners here — this is normal standby behaviour.
        return False

    bot_activity_logger.info(
        "[Startup] User session string found. Attempting to start user client..."
    )
    try:
        await existing_user_client.start()
        me = await existing_user_client.get_me()
        if me:
            bot_activity_logger.info(
                "[Startup] User client started as %s (@%s, id=%s)",
                me.first_name, me.username or "no username", me.id,
            )
        existing_user_client.add_event_handler(safe_pipeline_coro_fn, events.NewMessage())
        USER_CLIENT_STATUS["connected"]          = True
        USER_CLIENT_STATUS["consecutive_failures"] = 0
        USER_CLIENT_STATUS["error_notified"]     = False
        USER_CLIENT_STATUS["last_error"]         = None
        return True
    except FATAL_AUTH_ERRORS as auth_exc:
        etype   = classify_user_client_error(auth_exc)
        err_str = f"user.start() failed: {type(auth_exc).__name__}: {auth_exc}"
        USER_CLIENT_STATUS["connected"]            = False
        USER_CLIENT_STATUS["last_error"]           = err_str
        USER_CLIENT_STATUS["consecutive_failures"] = 1
        bot_activity_logger.critical(
            "[Startup] USER CLIENT AUTH FAILURE (%s): %s. "
            "Bot will continue running without the user client.",
            etype, err_str,
        )
        try:
            await notify_owners_about_user_client_error(err_str, etype)
        except Exception as notify_err:
            bot_activity_logger.error(
                "[Startup] Owner notification (auth fail) failed: %s", notify_err
            )
        return False
    except FloodWaitError as fw_exc:
        wait_s = int(getattr(fw_exc, "seconds", 60))
        USER_CLIENT_STATUS["connected"]  = False
        USER_CLIENT_STATUS["last_error"] = f"FloodWait during startup: {wait_s}s"
        bot_activity_logger.warning(
            "[Startup] User client blocked by FloodWaitError (%ss). "
            "Bot continues, user client will be retried via the run wrapper.",
            wait_s,
        )
        try:
            await notify_owners_about_user_client_error(
                f"FloodWaitError on startup. Telegram asked us to wait {wait_s}s. "
                f"User client will be retried automatically.",
                "general",
            )
        except Exception:
            pass
        return False
    except Exception as user_start_exc:
        etype   = classify_user_client_error(user_start_exc)
        err_str = f"user.start() failed: {type(user_start_exc).__name__}: {user_start_exc}"
        USER_CLIENT_STATUS["connected"]            = False
        USER_CLIENT_STATUS["last_error"]           = err_str
        USER_CLIENT_STATUS["consecutive_failures"] = 1
        bot_activity_logger.error(
            "[Startup] USER CLIENT START FAILED (%s): %s. "
            "Bot continues, user client will be retried via the run wrapper.",
            etype, err_str,
        )
        try:
            await notify_owners_about_user_client_error(err_str, etype)
        except Exception as notify_err:
            bot_activity_logger.error(
                "[Startup] Owner notification (generic) failed: %s", notify_err
            )
        return False


# -------------------------------------------------------------------------
# G. user_client_run_wrapper — hardened event loop for the user client
# -------------------------------------------------------------------------

async def user_client_run_wrapper(
    user_client_ref_getter,
    *,
    notify_owners_on_error: bool = True,
) -> None:
    """
    Keep the user client alive forever with classified error handling and
    automatic reconnects — *except* on fatal auth errors where we exit the
    wrapper cleanly (letting the operator regenerate a session via the
    still-running bot).

    Parameters
    ----------
    user_client_ref_getter : callable
        Zero-arg callable that returns the *current* global ``user``
        TelegramClient.  Needed because :func:`apply_user_session_and_reconnect`
        replaces the object in-place on hot-reload; a direct reference
        would keep using the old disconnected client.
    notify_owners_on_error : bool
        Disable in tests.

    Leader-guard behaviour
    ----------------------
    In a multi-node HA deployment (failover.py) only the **elected leader**
    is allowed to hold an active Telethon user session.  Every heartbeat
    cycle this wrapper checks MongoDB to confirm:

    * If this node is the leader → proceed normally.
    * If another node is the leader and its heartbeat is fresh → disconnect
      the user client (if running) and wait in a lightweight poll loop until
      this node becomes the leader.  The bot client remains fully operational
      so commands and alerts still work.
    * If the leader record has expired → allow this node to start the user
      client (failover recovery path).

    This eliminates ``AuthKeyDuplicatedError`` caused by two nodes sharing
    the same StringSession simultaneously.
    """
    logger = bot_activity_logger
    retry_count = 0
    max_retries_before_alert = 5
    # How often (seconds) to re-check leader status while in standby-wait.
    LEADER_POLL_INTERVAL = 15

    while True:
        # ── Leader guard ─────────────────────────────────────────────────
        try:
            allowed, reason = await is_this_node_the_session_leader()
        except Exception as guard_exc:
            logger.warning("[UserClient-Guard] Leader check raised: %s — allowing.", guard_exc)
            allowed, reason = True, f"guard-exception: {guard_exc}"

        if not allowed:
            # Disconnect the user client if it is currently connected so we
            # don't hold an active session while another node also holds one.
            user = user_client_ref_getter()
            if user is not None and user.is_connected():
                logger.warning(
                    "[UserClient-Guard] Leadership transferred to another node (%s). "
                    "Disconnecting user client on this node to prevent session conflict.",
                    reason,
                )
                try:
                    await user.disconnect()
                except Exception as disc_exc:
                    logger.warning("[UserClient-Guard] Disconnect noisy: %s", disc_exc)
                USER_CLIENT_STATUS["connected"]  = False
                USER_CLIENT_STATUS["last_error"] = f"Standby node — {reason}"

            logger.info(
                "[UserClient-Guard] Standby node. Polling every %ss for leader change. (%s)",
                LEADER_POLL_INTERVAL, reason,
            )
            await asyncio.sleep(LEADER_POLL_INTERVAL)
            continue
        # ── End leader guard ─────────────────────────────────────────────

        try:
            user = user_client_ref_getter()
            if user is None:
                logger.warning("[UserClient] user object is None; sleeping 30s.")
                await asyncio.sleep(30)
                continue

            # Explicit connectivity check — surfaces AuthKeyDuplicatedError
            # (the "two different IPs" error) long before we block on the
            # run_until_disconnected() call.
            if not user.is_connected():
                try:
                    await user.connect()
                except Exception as connect_exc:
                    etype = classify_user_client_error(connect_exc)
                    if notify_owners_on_error:
                        try:
                            await notify_owners_about_user_client_error(
                                f"user.connect() failed: "
                                f"{type(connect_exc).__name__}: {connect_exc}",
                                etype,
                            )
                        except Exception as notify_err:
                            logger.error(
                                "[UserClient] Owner notification failed: %s", notify_err
                            )
                    if isinstance(connect_exc, FATAL_AUTH_ERRORS):
                        USER_CLIENT_STATUS["connected"]  = False
                        USER_CLIENT_STATUS["last_error"] = (
                            f"{type(connect_exc).__name__}: {connect_exc}"
                        )
                        logger.critical(
                            "[UserClient] Fatal auth error on connect (%s); "
                            "stopping user client loop until new session is set.",
                            etype,
                        )
                        return  # → main.py keeps only the bot client alive
                    await asyncio.sleep(10)
                    continue

            # Sanity check: prove the auth still works this tick.
            try:
                me = await user.get_me()
                if me:
                    logger.info(
                        "[UserClient] Authorized as %s (@%s, id=%s)",
                        me.first_name, me.username or "no username", me.id,
                    )
                USER_CLIENT_STATUS["connected"]            = True
                USER_CLIENT_STATUS["consecutive_failures"] = 0
                retry_count = 0
            except Exception as me_exc:
                etype = classify_user_client_error(me_exc)
                USER_CLIENT_STATUS["connected"]            = False
                USER_CLIENT_STATUS["last_error"]           = f"{type(me_exc).__name__}: {me_exc}"
                USER_CLIENT_STATUS["consecutive_failures"] += 1
                if isinstance(me_exc, FATAL_AUTH_ERRORS):
                    if notify_owners_on_error:
                        try:
                            await notify_owners_about_user_client_error(
                                f"user.get_me() failed: "
                                f"{type(me_exc).__name__}: {me_exc}",
                                etype,
                            )
                        except Exception as notify_err:
                            logger.error(
                                "[UserClient] Owner notification failed: %s", notify_err
                            )
                    logger.critical(
                        "[UserClient] Fatal auth error on get_me (%s); "
                        "stopping user client loop until new session is set.",
                        etype,
                    )
                    return
                logger.warning(
                    "[UserClient] get_me() check failed: %s: %s",
                    type(me_exc).__name__, me_exc,
                )

            logger.info("[UserClient] Entering run_until_disconnected() loop")
            await user.run_until_disconnected()
            logger.warning(
                "[UserClient] run_until_disconnected() returned cleanly — reconnecting in 5s"
            )
            USER_CLIENT_STATUS["connected"] = False
            await asyncio.sleep(5)
            continue

        except PersistentTimestampOutdatedError:
            logger.warning(
                "[UserClient] PersistentTimestampOutdatedError — continuing after 2s"
            )
            USER_CLIENT_STATUS["connected"] = False
            await asyncio.sleep(2)
        except HistoryGetFailedError:
            logger.warning(
                "[UserClient] HistoryGetFailedError — continuing after 2s"
            )
            USER_CLIENT_STATUS["connected"] = False
            await asyncio.sleep(2)
        except FATAL_AUTH_ERRORS as auth_exc:
            etype   = classify_user_client_error(auth_exc)
            err_str = f"{type(auth_exc).__name__}: {auth_exc}"
            USER_CLIENT_STATUS["connected"]            = False
            USER_CLIENT_STATUS["last_error"]           = err_str
            USER_CLIENT_STATUS["consecutive_failures"] += 1
            logger.critical(
                "[UserClient] FATAL AUTH ERROR (%s): %s\n%s",
                etype, err_str, _tb.format_exc(),
            )
            if notify_owners_on_error:
                try:
                    await notify_owners_about_user_client_error(err_str, etype)
                except Exception as notify_err:
                    logger.error(
                        "[UserClient] Owner notification failed: %s", notify_err
                    )
            logger.critical(
                "[UserClient] Session is invalidated (conflict/expired/banned). "
                "User client will NOT auto-reconnect — operator must run /generate_session."
            )
            return  # → bot keeps serving recovery commands
        except FloodWaitError as fw_exc:
            wait_s = int(getattr(fw_exc, "seconds", 60))
            USER_CLIENT_STATUS["connected"]  = False
            USER_CLIENT_STATUS["last_error"] = f"FloodWait: {wait_s}s"
            logger.warning("[UserClient] FloodWaitError — sleeping %ss", wait_s)
            if notify_owners_on_error:
                try:
                    await notify_owners_about_user_client_error(
                        f"FloodWaitError: Telegram asked us to wait {wait_s} seconds. "
                        f"User client paused, will resume automatically.",
                        "general",
                    )
                except Exception:
                    pass
            await asyncio.sleep(wait_s + 5)
        except Exception as generic_exc:
            etype   = classify_user_client_error(generic_exc)
            err_str = f"{type(generic_exc).__name__}: {generic_exc}"
            USER_CLIENT_STATUS["connected"]            = False
            USER_CLIENT_STATUS["last_error"]           = err_str
            USER_CLIENT_STATUS["consecutive_failures"] += 1
            retry_count += 1
            logger.error(
                "[UserClient] Unexpected error (retry %s): %s\n%s",
                retry_count, err_str, _tb.format_exc(),
            )
            if retry_count >= max_retries_before_alert and notify_owners_on_error:
                try:
                    await notify_owners_about_user_client_error(err_str, etype)
                except Exception as notify_err:
                    logger.error(
                        "[UserClient] Owner notification failed: %s", notify_err
                    )
                retry_count = 0
            backoff = min(30, 2 ** min(retry_count, 5))
            await asyncio.sleep(backoff)
