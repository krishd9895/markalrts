# --- LAUNCH MODE CONFIGURATION ---
LAUNCH_MODE = "PYTHON"
TARGET = "main.py"
CUSTOM_RAW_COMMAND = ""

# --- INFRASTRUCTURE & DISTRIBUTED LOCK PROPERTIES ---
SERVICE_ID = "Markalrts_Bot"
DATABASE_NAME = "Failover"
COLLECTION_NAME = "Services"

# --- CLUSTER TIMING GUARDRAILS ---
HEARTBEAT_INTERVAL = 15
HEARTBEAT_TIMEOUT = 60
CHECK_INTERVAL = 5
LOCAL_RETRY_LIMIT = 3
STARTUP_GRACE_PERIOD = 5
MAX_NETWORK_GRACE_S = 30
MAX_LEADER_HISTORY = 20

# --- WATCHDOG VERSION ---
WATCHDOG_VERSION = "1.1.0"

# --- STANDARD IMPORTS ---
import os
import sys
import time
import uuid
import shlex
import random
import shutil
import socket
import signal
import hashlib
import platform
import logging
import subprocess
from datetime import datetime, timezone
from dotenv import load_dotenv
import pymongo
from pymongo.errors import PyMongoError, ConnectionFailure, DuplicateKeyError
from webserver import keep_alive

# ---------------------------------------------------------------------------
# 1. AUTOMATED HOST PC IDENTIFICATION & ENVIRONMENT RESOLUTION
# ---------------------------------------------------------------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
IS_WINDOWS = platform.system() == "Windows"

if not SERVICE_ID.strip() or not MONGO_URI or not MONGO_URI.strip():
    print("CRITICAL CONFIGURATION ERROR: Missing core environment variables or SERVICE_ID!", file=sys.stderr)
    sys.exit(1)

def resolve_host_pc_name():
    """
    Auto-identifies the physical machine name across Docker, WSL2, and bare-metal environments.
    Checks custom environment variables first, then host mount files, and finally fallback sockets.
    """
    # 1. Custom explicit name from .env (e.g. HOST_PC_NAME="Workstation-PC")
    explicit_name = os.getenv("HOST_PC_NAME") or os.getenv("PC_NAME")
    if explicit_name and explicit_name.strip():
        return explicit_name.strip()

    # 2. Check if mounted host hostname file exists (useful in Docker/WSL volume mounts)
    for host_file in ["/etc/host_hostname", "/etc/hostname_host"]:
        if os.path.exists(host_file):
            try:
                with open(host_file, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        return content
            except Exception:
                pass

    # 3. Check for WSL2 or system environment variables passed into containers
    wsl_host = os.getenv("WSL_HOST_NAME") or os.getenv("COMPUTERNAME")
    if wsl_host and wsl_host.strip():
        return wsl_host.strip()

    # 4. Standard system socket hostname fallback (returns container ID or host name)
    return socket.gethostname()

HOSTNAME = resolve_host_pc_name()
OS_INFO = f"{platform.system()} {platform.release()}"
PYTHON_VERSION = platform.python_version()

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))

SYSTEM_HOME = os.path.expanduser("~")
MACHINE_ID_FILE = os.path.join(SYSTEM_HOME, f".ha_watchdog_{SERVICE_ID}.id")

if os.path.exists(MACHINE_ID_FILE):
    try:
        with open(MACHINE_ID_FILE, "r", encoding="utf-8") as f:
            PERSISTENT_MACHINE_UUID = f.read().strip()
    except Exception:
        PERSISTENT_MACHINE_UUID = str(uuid.uuid4())
else:
    PERSISTENT_MACHINE_UUID = str(uuid.uuid4())
    try:
        with open(MACHINE_ID_FILE, "w", encoding="utf-8") as f:
            f.write(PERSISTENT_MACHINE_UUID)
    except Exception as e:
        print(f"WARNING: Unable to write machine identity token locally: {e}", file=sys.stderr)

NODE_ID = f"{PERSISTENT_MACHINE_UUID}:{PROJECT_PATH}"

PATH_HASH = hashlib.sha1(PROJECT_PATH.encode('utf-8')).hexdigest()[:8]
NODE_ALIAS = f"{HOSTNAME}:{PATH_HASH}"

# --- RESILIENT LOGGING ROUTER WITH FALLBACK INJECTION ---
old_log_record_factory = logging.getLogRecordFactory()

def custom_log_record_factory(*args, **kwargs):
    record = old_log_record_factory(*args, **kwargs)
    record.node_alias = NODE_ALIAS
    return record

logging.setLogRecordFactory(custom_log_record_factory)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Node: %(node_alias)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("failover_watchdog")

# --- UNIFIED RESOLVED VECTOR INTERPRETATION ---
final_exec_args = []
mode_upper = LAUNCH_MODE.strip().upper()

if mode_upper == "PYTHON":
    if not TARGET.strip():
        for candidate in ["main.py", "bot.py", "app.py"]:
            if os.path.exists(os.path.join(PROJECT_PATH, candidate)):
                TARGET = candidate
                break
        else:
            TARGET = "main.py"
    final_exec_args = [sys.executable, TARGET]

elif mode_upper == "DOCKER_COMPOSE":
    DOCKER_BIN = shutil.which("docker") or "docker"
    additional_flags = shlex.split(TARGET)
    final_exec_args = [DOCKER_BIN, "compose"] + additional_flags + ["up"]

elif mode_upper == "RAW":
    if not CUSTOM_RAW_COMMAND.strip():
        print("CRITICAL LOGIC ERROR: LAUNCH_MODE is set to 'RAW' but 'CUSTOM_RAW_COMMAND' is empty!", file=sys.stderr)
        sys.exit(1)
    final_exec_args = shlex.split(CUSTOM_RAW_COMMAND)

START_COMMAND_STRING = " ".join(f'"{arg}"' for arg in final_exec_args)
if os.getenv("PYCHARM_HOSTED"):
    IDE_CONTEXT = "PyCharm"
elif os.getenv("VSCODE_PID"):
    IDE_CONTEXT = "VS Code"
else:
    IDE_CONTEXT = "Terminal/Shell"

CONFIG_PAYLOAD = f"{START_COMMAND_STRING}|{HEARTBEAT_INTERVAL}|{HEARTBEAT_TIMEOUT}"
CONFIG_FINGERPRINT = hashlib.sha256(CONFIG_PAYLOAD.encode('utf-8')).hexdigest()

child_process = None
is_running = True
db_disconnect_tracker = None
startup_delay = random.uniform(0.5, 3.5)
watchdog_started_at = datetime.now(timezone.utc)
heartbeat_counter = 0
last_logged_status = {
    "leader": None,
    "forced_leader": None,
    "standby_count": None,
    "status": None
}

try:
    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000, retryReads=True, retryWrites=True)
    db_collection = mongo_client[DATABASE_NAME][COLLECTION_NAME]
except Exception as init_err:
    print(f"CRITICAL: Failed to initialize PyMongo pool: {init_err}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# 2. SYSTEM SIGNAL INTERCEPTION & CONTAINER-AWARE LIFECYCLE
# ---------------------------------------------------------------------------
def handle_shutdown_signal(signum, frame):
    global is_running
    logger.info(f"Received termination signal ({signal.Signals(signum).name}). Cleaning local environment...")
    is_running = False

signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)

def terminate_child():
    global child_process

    if LAUNCH_MODE.strip().upper() == "DOCKER_COMPOSE":
        logger.warning("Executing proactive Docker Compose stack teardown sequence...")
        try:
            down_args = list(final_exec_args)
            if down_args[-1] == "up" and "compose" in down_args:
                down_args[-1] = "down"
            else:
                down_args = [shutil.which("docker") or "docker", "compose", "down"]
            subprocess.run(down_args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20, check=False)
            logger.info("Docker infrastructure successfully verified down.")
        except Exception as e:
            logger.error(f"Failed to cleanly invoke stack teardown sequence: {e}")

    if child_process and child_process.poll() is None:
        if IS_WINDOWS:
            try:
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(child_process.pid)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            except Exception as e:
                logger.error(f"Windows taskkill tree termination failed: {e}")
        else:
            try:
                os.killpg(os.getpgid(child_process.pid), signal.SIGTERM)
                for _ in range(5):
                    if child_process.poll() is not None:
                        break
                    time.sleep(1)
                else:
                    os.killpg(os.getpgid(child_process.pid), signal.SIGKILL)
                    child_process.wait()
            except Exception as e:
                logger.error(f"POSIX process tree kill sequence failed: {e}")

    child_process = None

# ---------------------------------------------------------------------------
# 3. TRANSITIONAL LEADER ELECTORATE (DETERMINISTIC LEASE MANAGEMENT)
# ---------------------------------------------------------------------------
def setup_database_indexes():
    try:
        db_collection.create_index([("current_leader.node_id", pymongo.ASCENDING)], background=True)
        return True
    except PyMongoError as e:
        logger.error(f"Failed to optimize indexing configuration: {e}")
        return False

def get_normalized_forced_leader(doc):
    forced_leader = doc.get("forced_leader", {}).get("node_alias")
    if forced_leader == "":
        return None
    return forced_leader

def is_leader_active(doc):
    current_leader = doc.get("current_leader")
    if not current_leader:
        return False

    last_heartbeat = current_leader.get("last_heartbeat")
    if not last_heartbeat:
        return False

    try:
        if last_heartbeat.tzinfo is None:
            last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
        time_since_heartbeat = (datetime.now(timezone.utc) - last_heartbeat).total_seconds()
        return time_since_heartbeat < HEARTBEAT_TIMEOUT
    except Exception as e:
        logger.error(f"Error checking leader heartbeat: {e}")
        return False

def is_forced_leader_currently_active(doc, forced_leader_node_alias):
    if not forced_leader_node_alias:
        return False

    current_leader = doc.get("current_leader", {})
    if current_leader.get("node_alias") != forced_leader_node_alias:
        return False

    return is_leader_active(doc)

def remove_stale_standby_nodes(doc):
    standby_nodes = doc.get("standby_nodes", [])
    current_time = datetime.now(timezone.utc)
    updated_standby_nodes = []

    for node in standby_nodes:
        last_heartbeat = node.get("last_heartbeat")
        if last_heartbeat:
            try:
                if last_heartbeat.tzinfo is None:
                    last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
                time_since_heartbeat = (current_time - last_heartbeat).total_seconds()
                if time_since_heartbeat < HEARTBEAT_TIMEOUT:
                    updated_standby_nodes.append(node)
            except Exception as e:
                logger.error(f"Error checking standby node heartbeat: {e}")

    return updated_standby_nodes

def update_standby_node_heartbeat():
    try:
        current_time = datetime.now(timezone.utc)
        doc = db_collection.find_one({"_id": SERVICE_ID})
        if not doc:
            return

        updated_standby_nodes = remove_stale_standby_nodes(doc)

        node_found = False
        for i, node in enumerate(updated_standby_nodes):
            if node.get("node_id") == NODE_ID:
                updated_standby_nodes[i] = {
                    "node_id": NODE_ID,
                    "node_alias": NODE_ALIAS,
                    "hostname": HOSTNAME,
                    "last_heartbeat": current_time,
                    "os": OS_INFO,
                    "python_version": PYTHON_VERSION
                }
                node_found = True
                break

        if not node_found:
            updated_standby_nodes.append({
                "node_id": NODE_ID,
                "node_alias": NODE_ALIAS,
                "hostname": HOSTNAME,
                "last_heartbeat": current_time,
                "os": OS_INFO,
                "python_version": PYTHON_VERSION
            })

        db_collection.update_one(
            {"_id": SERVICE_ID},
            {"$set": {"standby_nodes": updated_standby_nodes}}
        )
        logger.debug("[STANDBY HEARTBEAT] Updated standby heartbeat successfully")
    except Exception as e:
        logger.error(f"Failed to update standby heartbeat: {e}")

def remove_self_from_standby():
    try:
        doc = db_collection.find_one({"_id": SERVICE_ID})
        if not doc:
            return

        updated_standby_nodes = [node for node in doc.get("standby_nodes", []) if node.get("node_id") != NODE_ID]

        db_collection.update_one(
            {"_id": SERVICE_ID},
            {"$set": {"standby_nodes": updated_standby_nodes}}
        )
        logger.debug("[STANDBY] Removed self from standby nodes")
    except Exception as e:
        logger.error(f"Failed to remove self from standby nodes: {e}")

def bootstrap_and_validate_lock():
    try:
        doc = db_collection.find_one({"_id": SERVICE_ID})

        if not doc:
            initial_state = {
                "_id": SERVICE_ID,
                "watchdog_version": WATCHDOG_VERSION,
                "current_leader": {
                    "node_id": None,
                    "node_alias": None,
                    "status": "offline",
                    "last_heartbeat": datetime.fromtimestamp(0, tz=timezone.utc),
                    "hostname": None,
                    "pid": None,
                    "started_at": None,
                    "heartbeat_count": 0,
                    "os": None,
                    "python_version": None
                },
                "forced_leader": {
                    "node_alias": None
                },
                "standby_nodes": [],
                "leader_history": [],
                "last_crash": None,
                "statistics": {
                    "leader_changes": 0,
                    "bot_restarts": 0,
                    "forced_takeovers": 0
                },
                "config_fingerprint": CONFIG_FINGERPRINT
            }
            try:
                db_collection.insert_one(initial_state)
                logger.info("Successfully bootstrapped the missing cluster control record.")
            except DuplicateKeyError:
                pass
        else:
            forced_leader = get_normalized_forced_leader(doc)
            doc_forced_leader = doc.get("forced_leader", {}).get("node_alias")
            if doc_forced_leader == "":
                logger.info("Normalizing forced_leader from empty string to None")
                db_collection.update_one(
                    {"_id": SERVICE_ID},
                    {"$set": {"forced_leader.node_alias": None}}
                )

            updates = {}
            if "standby_nodes" not in doc:
                updates["standby_nodes"] = []
            if "leader_history" not in doc:
                updates["leader_history"] = []
            if "last_crash" not in doc:
                updates["last_crash"] = None
            if "statistics" not in doc:
                updates["statistics"] = {
                    "leader_changes": 0,
                    "bot_restarts": 0,
                    "forced_takeovers": 0
                }
            if "watchdog_version" not in doc:
                updates["watchdog_version"] = WATCHDOG_VERSION

            if updates:
                db_collection.update_one({"_id": SERVICE_ID}, {"$set": updates})

        if doc and doc.get("config_fingerprint") != CONFIG_FINGERPRINT:
            logger.critical("🚨 CONFIGURATION FINGERPRINT MISMATCH! Execution immediately halted.")
            sys.exit(1)

        return True

    except (ConnectionFailure, PyMongoError) as e:
        logger.error(f"Error checking cluster validation status: {e}. Retrying pool in 5s...")
        time.sleep(5)
        return False

def release_leadership(doc=None):
    global heartbeat_counter
    try:
        current_time = datetime.now(timezone.utc)
        query = {"_id": SERVICE_ID, "current_leader.node_id": NODE_ID}
        
        if not doc:
            doc = db_collection.find_one(query)
        
        if doc:
            logger.info("[LEADER STATUS] Stepping down as leader - was previously active node")
            
            leader_history = doc.get("leader_history", [])
            current_leader = doc.get("current_leader", {})
            started_at = current_leader.get("started_at")
            
            if started_at:
                leader_history.append({
                    "node_alias": current_leader.get("node_alias"),
                    "started_at": started_at,
                    "ended_at": current_time
                })
                
                if len(leader_history) > MAX_LEADER_HISTORY:
                    leader_history = leader_history[-MAX_LEADER_HISTORY:]
            
            update = {
                "$set": {
                    "current_leader.status": "offline",
                    "leader_history": leader_history
                }
            }
            db_collection.update_one(query, update)
        
        heartbeat_counter = 0
        logger.info("Released leadership lock successfully.")
    except Exception as e:
        logger.error(f"Failed to issue clean leadership release: {e}")

def try_acquire_or_maintain_leadership(force_check_only=False, update_telemetry=False):
    global db_disconnect_tracker, heartbeat_counter

    try:
        doc = db_collection.find_one({"_id": SERVICE_ID})
        if not doc:
            return False

        forced_leader = get_normalized_forced_leader(doc)
        this_node_is_forced_leader = (forced_leader == NODE_ALIAS)

        current_leader = doc.get("current_leader", {})
        current_leader_node_alias = current_leader.get("node_alias")
        is_this_node_current_leader = (current_leader.get("node_id") == NODE_ID)
        is_current_leader_active = is_leader_active(doc)

        standby_nodes_count = len(doc.get("standby_nodes", []))
        
        status_changed = False
        if last_logged_status["leader"] != current_leader_node_alias:
            last_logged_status["leader"] = current_leader_node_alias
            status_changed = True
        if last_logged_status["forced_leader"] != forced_leader:
            last_logged_status["forced_leader"] = forced_leader
            status_changed = True
        if last_logged_status["standby_count"] != standby_nodes_count:
            last_logged_status["standby_count"] = standby_nodes_count
            status_changed = True
        
        if status_changed:
            logger.info(f"[LEADER STATUS] Forced leader: '{forced_leader}', This node is forced leader: {this_node_is_forced_leader}")
            logger.info(f"[LEADER STATUS] Current leader: '{current_leader_node_alias}', Active: {is_current_leader_active}")
            logger.info(f"[LEADER STATUS] Standby nodes: {standby_nodes_count}")
            logger.info(f"[LEADER STATUS] This node: '{NODE_ALIAS}'")

        if force_check_only:
            db_disconnect_tracker = None

            if is_this_node_current_leader:
                if forced_leader and not this_node_is_forced_leader:
                    is_fl_active = is_forced_leader_currently_active(doc, forced_leader)
                    if is_fl_active:
                        logger.warning(f"[LEADER STATUS] Administrative override active! Forced leader '{forced_leader}' is active. Stepping down.")
                        return False
                return True
            return False

        current_time = datetime.now(timezone.utc)
        
        if this_node_is_forced_leader:
            previous_leader_alias = current_leader_node_alias
            was_this_node_leader_before = is_this_node_current_leader
            
            if not was_this_node_leader_before:
                logger.info("[LEADER STATUS] This is the forced leader! Attempting to take over leadership...")
                if previous_leader_alias and previous_leader_alias != NODE_ALIAS:
                    logger.warning(f"[LEADER STATUS] Taking over from previous leader: {previous_leader_alias}")
            
            leader_history = doc.get("leader_history", [])
            if not was_this_node_leader_before and current_leader_node_alias:
                leader_history.append({
                    "node_alias": current_leader_node_alias,
                    "started_at": current_leader.get("started_at"),
                    "ended_at": current_time
                })
                if len(leader_history) > MAX_LEADER_HISTORY:
                    leader_history = leader_history[-MAX_LEADER_HISTORY:]
            
            if is_this_node_current_leader:
                heartbeat_counter += 1
            else:
                heartbeat_counter = 1
            
            stats = doc.get("statistics", {"leader_changes": 0, "bot_restarts": 0, "forced_takeovers": 0})
            if not was_this_node_leader_before:
                stats["leader_changes"] += 1
                stats["forced_takeovers"] += 1
            
            update_modifier = {
                "$set": {
                    "current_leader.node_id": NODE_ID,
                    "current_leader.node_alias": NODE_ALIAS,
                    "current_leader.status": "active",
                    "current_leader.hostname": HOSTNAME,
                    "current_leader.os": OS_INFO,
                    "current_leader.python_version": PYTHON_VERSION,
                    "current_leader.heartbeat_count": heartbeat_counter,
                    "leader_history": leader_history,
                    "statistics": stats,
                    "watchdog_version": WATCHDOG_VERSION,
                    "config_fingerprint": CONFIG_FINGERPRINT
                },
                "$currentDate": {
                    "current_leader.last_heartbeat": True
                }
            }
            
            if not was_this_node_leader_before:
                update_modifier["$set"]["current_leader.started_at"] = current_time
            
            if child_process and child_process.poll() is None:
                update_modifier["$set"]["current_leader.pid"] = child_process.pid

            result = db_collection.find_one_and_update(
                {"_id": SERVICE_ID},
                update_modifier,
                upsert=False,
                return_document=pymongo.ReturnDocument.AFTER
            )

            became_leader = result and result.get("current_leader", {}).get("node_id") == NODE_ID
            if became_leader and not is_this_node_current_leader:
                takeover_time = current_time.strftime("%Y-%m-%d %H:%M:%S UTC")
                logger.info(f"🎉 [TAKEOVER COMPLETE!] Took over as leader at {takeover_time}")
                logger.info(f"📋 [TAKEOVER DETAILS]:")
                logger.info(f"   • Previous leader: {previous_leader_alias if previous_leader_alias else 'No previous leader'}")
                logger.info("   • Reason: This node is the configured forced leader")

            db_disconnect_tracker = None
            return became_leader

        if forced_leader:
            is_fl_active = is_forced_leader_currently_active(doc, forced_leader)
            if is_fl_active:
                if last_logged_status["status"] != "waiting_for_fl":
                    logger.info(f"[LEADER STATUS] Waiting for forced leader '{forced_leader}' - it's currently active")
                    last_logged_status["status"] = "waiting_for_fl"
                return False
            else:
                if last_logged_status["status"] != "fl_offline":
                    logger.warning(f"[LEADER STATUS] Forced leader '{forced_leader}' is offline! Any node can take over!")
                    last_logged_status["status"] = "fl_offline"

        should_attempt_takeover = False
        takeover_reason = ""
        if not forced_leader:
            if not is_current_leader_active or is_this_node_current_leader:
                should_attempt_takeover = True
                if is_this_node_current_leader:
                    takeover_reason = "Continuing as current leader"
                elif not current_leader_node_alias or current_leader_node_alias == NODE_ALIAS:
                    takeover_reason = "No active leader, claiming leadership"
                else:
                    last_heartbeat = current_leader.get("last_heartbeat")
                    if last_heartbeat:
                        try:
                            if last_heartbeat.tzinfo is None:
                                last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
                            time_since_heartbeat = (datetime.now(timezone.utc) - last_heartbeat).total_seconds()
                            takeover_reason = f"Previous leader {current_leader_node_alias} heartbeat expired (last heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S UTC')})"
                        except Exception as e:
                            takeover_reason = f"Previous leader {current_leader_node_alias} heartbeat unreadable"
                    else:
                        takeover_reason = f"Previous leader {current_leader_node_alias} has no heartbeat"
        else:
            if not is_current_leader_active or is_this_node_current_leader:
                should_attempt_takeover = True
                if is_this_node_current_leader:
                    takeover_reason = "Continuing as current leader (forced leader is offline)"
                elif not current_leader_node_alias or current_leader_node_alias == NODE_ALIAS:
                    takeover_reason = "Forced leader is offline, no active leader, claiming leadership"
                else:
                    last_heartbeat = current_leader.get("last_heartbeat")
                    if last_heartbeat:
                        try:
                            if last_heartbeat.tzinfo is None:
                                last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
                            time_since_heartbeat = (datetime.now(timezone.utc) - last_heartbeat).total_seconds()
                            takeover_reason = f"Forced leader {forced_leader} is offline! Previous leader {current_leader_node_alias} heartbeat expired (last heartbeat: {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S UTC')})"
                        except Exception as e:
                            takeover_reason = f"Forced leader {forced_leader} is offline! Previous leader {current_leader_node_alias} heartbeat unreadable"
                    else:
                        takeover_reason = f"Forced leader {forced_leader} is offline! Previous leader {current_leader_node_alias} has no heartbeat"

        if should_attempt_takeover:
            previous_leader_alias = current_leader_node_alias
            was_this_node_leader_before = is_this_node_current_leader
            
            if not was_this_node_leader_before:
                logger.warning(f"[LEADER STATUS] Previous leader {previous_leader_alias if previous_leader_alias else 'No leader'} - taking over")
            
            leader_history = doc.get("leader_history", [])
            if not was_this_node_leader_before and current_leader_node_alias:
                leader_history.append({
                    "node_alias": current_leader_node_alias,
                    "started_at": current_leader.get("started_at"),
                    "ended_at": current_time
                })
                if len(leader_history) > MAX_LEADER_HISTORY:
                    leader_history = leader_history[-MAX_LEADER_HISTORY:]
            
            if is_this_node_current_leader:
                heartbeat_counter += 1
            else:
                heartbeat_counter = 1
            
            stats = doc.get("statistics", {"leader_changes": 0, "bot_restarts": 0, "forced_takeovers": 0})
            if not was_this_node_leader_before:
                stats["leader_changes"] += 1

            filter_query = {
                "_id": SERVICE_ID,
                "$expr": {
                    "$or": [
                        {"$eq": ["$current_leader.node_id", NODE_ID]},
                        {"$eq": ["$current_leader.node_id", None]},
                        {"$gt": [
                            "$$NOW",
                            {"$add": ["$current_leader.last_heartbeat", HEARTBEAT_TIMEOUT * 1000]}
                        ]}
                    ]
                }
            }

            update_modifier = {
                "$set": {
                    "current_leader.node_id": NODE_ID,
                    "current_leader.node_alias": NODE_ALIAS,
                    "current_leader.status": "active",
                    "current_leader.hostname": HOSTNAME,
                    "current_leader.os": OS_INFO,
                    "current_leader.python_version": PYTHON_VERSION,
                    "current_leader.heartbeat_count": heartbeat_counter,
                    "leader_history": leader_history,
                    "statistics": stats,
                    "watchdog_version": WATCHDOG_VERSION,
                    "config_fingerprint": CONFIG_FINGERPRINT
                },
                "$currentDate": {
                    "current_leader.last_heartbeat": True
                }
            }
            
            if not was_this_node_leader_before:
                update_modifier["$set"]["current_leader.started_at"] = current_time
            
            if child_process and child_process.poll() is None:
                update_modifier["$set"]["current_leader.pid"] = child_process.pid

            result = db_collection.find_one_and_update(
                filter_query,
                update_modifier,
                upsert=False,
                return_document=pymongo.ReturnDocument.AFTER
            )

            became_leader = result and result.get("current_leader", {}).get("node_id") == NODE_ID
            if became_leader and not is_this_node_current_leader:
                takeover_time = current_time.strftime("%Y-%m-%d %H:%M:%S UTC")
                logger.info(f"🎉 [TAKEOVER COMPLETE!] Took over as leader at {takeover_time}")
                logger.info("📋 [TAKEOVER DETAILS]:")
                logger.info(f"   • Previous leader: {previous_leader_alias if previous_leader_alias else 'No previous leader'}")
                logger.info(f"   • Reason: {takeover_reason}")

            db_disconnect_tracker = None
            return became_leader
        else:
            return False

    except (ConnectionFailure, PyMongoError) as e:
        logger.error(f"Database network communication fault: {e}")

        if db_disconnect_tracker is None:
            db_disconnect_tracker = time.time()

        if (time.time() - db_disconnect_tracker) > MAX_NETWORK_GRACE_S:
            logger.critical(f"🚨 CIRCUIT BREAKER TRIPPED! DB offline >{MAX_NETWORK_GRACE_S}s. Dropping lease.")
            return False

        return True

# ---------------------------------------------------------------------------
# 4. RUNTIME MAIN LOOP
# ---------------------------------------------------------------------------
def main():
    global child_process, is_running

    print(f"======================================================================", flush=True)
    print(f"🔥 HA PROCESS WATCHDOG ACTIVE | Mode: {LAUNCH_MODE}", flush=True)
    print(f"Service ID : {SERVICE_ID}", flush=True)
    print(f"NODE ALIAS : {NODE_ALIAS}", flush=True)
    print(f"Vector     : {final_exec_args}", flush=True)
    print(f"Version    : {WATCHDOG_VERSION}", flush=True)
    print(f"======================================================================\n", flush=True)

    logger.info(f"[STARTUP] Waiting {startup_delay:.2f}s before starting (randomized startup delay)")
    time.sleep(startup_delay)

    if not setup_database_indexes() or not bootstrap_and_validate_lock():
        return

    is_leader = False
    local_failures = 0
    last_heartbeat_time = 0
    last_standby_heartbeat_time = 0

    while is_running:
        try:
            # --- STANDBY MONITORING LAYER ---
            if not is_leader:
                if last_logged_status["status"] != "standby":
                    logger.info("[STATUS] This node is standby, checking for leadership...")
                    last_logged_status["status"] = "standby"

                current_time = time.time()
                if current_time - last_standby_heartbeat_time >= HEARTBEAT_INTERVAL:
                    update_standby_node_heartbeat()
                    last_standby_heartbeat_time = current_time

                if not try_acquire_or_maintain_leadership(update_telemetry=True):
                    time.sleep(CHECK_INTERVAL)
                    continue

                if not bootstrap_and_validate_lock():
                    release_leadership()
                    time.sleep(CHECK_INTERVAL)
                    continue

                remove_self_from_standby()

                is_leader = True
                local_failures = 0

            # --- ACTIVE SUPERVISOR LAYER ---
            if is_leader:
                if child_process is None or child_process.poll() is not None:
                    if child_process and child_process.poll() is not None:
                        exit_code = child_process.poll()
                        local_failures += 1
                        logger.warning(f"[BOT STATUS] Bot process crashed (Exit code: {exit_code}). Failures: {local_failures}/{LOCAL_RETRY_LIMIT}")
                        
                        try:
                            current_time = datetime.now(timezone.utc)
                            stats = db_collection.find_one({"_id": SERVICE_ID}, {"statistics": 1})
                            if stats:
                                stats = stats.get("statistics", {"leader_changes": 0, "bot_restarts": 0, "forced_takeovers": 0})
                                stats["bot_restarts"] += 1
                                db_collection.update_one(
                                    {"_id": SERVICE_ID},
                                    {"$set": {
                                        "last_crash": {
                                            "exit_code": exit_code,
                                            "timestamp": current_time,
                                            "node_alias": NODE_ALIAS
                                        },
                                        "statistics": stats
                                    }}
                                )
                        except Exception as e:
                            logger.error(f"Failed to update last crash info: {e}")
                        
                        child_process = None

                        if local_failures > LOCAL_RETRY_LIMIT:
                            logger.critical("[BOT STATUS] Local recovery limit breached. Relinquishing leadership lock.")
                            terminate_child()
                            release_leadership()
                            is_leader = False
                            last_logged_status["status"] = None
                            time.sleep(CHECK_INTERVAL)
                            continue

                    if not try_acquire_or_maintain_leadership():
                        logger.warning("[LEADER STATUS] Split-brain caught during crash recovery phase. Reverting to standby.")
                        is_leader = False
                        last_logged_status["status"] = None
                        continue

                    logger.info(f"[BOT STATUS] Starting bot process with: {final_exec_args}")
                    try:
                        if IS_WINDOWS:
                            child_process = subprocess.Popen(final_exec_args, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
                        else:
                            child_process = subprocess.Popen(final_exec_args, start_new_session=True)

                        time.sleep(STARTUP_GRACE_PERIOD)

                        if child_process.poll() is not None:
                            logger.error("[BOT STATUS] Bot process died during startup grace window.")
                            continue

                        try:
                            db_collection.update_one(
                                {"_id": SERVICE_ID, "current_leader.node_id": NODE_ID},
                                {"$set": {"current_leader.pid": child_process.pid}}
                            )
                        except Exception as e:
                            logger.error(f"Failed to update PID in MongoDB: {e}")

                        if try_acquire_or_maintain_leadership():
                            last_heartbeat_time = time.time()
                            logger.info("[BOT STATUS] Bot passed initial checks. Heartbeat tracking active.")
                        else:
                            logger.critical("[LEADER STATUS] Failed to retain lock during verification. Stopping bot.")
                            terminate_child()
                            is_leader = False
                            last_logged_status["status"] = None
                            continue
                    except Exception as e:
                        logger.error(f"[BOT STATUS] System failure attempting to start bot: {e}")
                        child_process = None
                        time.sleep(CHECK_INTERVAL)
                        continue

                # --- STEADY-STATE RUNTIME OPERATION ---
                current_time = time.time()

                if not try_acquire_or_maintain_leadership(force_check_only=True):
                    logger.critical("[LEADER STATUS] STEPDOWN TRIGGERED! Node identity overtaken or manual override active. Stopping bot.")
                    terminate_child()
                    release_leadership()
                    is_leader = False
                    last_logged_status["status"] = None
                    continue

                if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    if child_process.poll() is None:
                        if try_acquire_or_maintain_leadership(update_telemetry=False):
                            logger.debug(f"[HEARTBEAT] Heartbeat logged successfully (Count: {heartbeat_counter})")
                            last_heartbeat_time = current_time
                            local_failures = 0
                        else:
                            logger.critical("[LEADER STATUS] LEASE LOST! Lock overridden during heartbeat update. Stopping bot.")
                            terminate_child()
                            release_leadership()
                            is_leader = False
                            last_logged_status["status"] = None
                    else:
                        logger.warning("[BOT STATUS] Bot process died inside scheduled pulse window.")

                time.sleep(1)

        except PyMongoError as e:
            logger.error(f"[DATABASE] Database infrastructure connectivity issue: {e}. Re-verifying pool...")
            time.sleep(2)
        except Exception as e:
            logger.error(f"[SYSTEM] Unhandled exception in runtime supervisor loop: {e}")
            time.sleep(2)

    terminate_child()
    if is_leader:
        release_leadership()
    else:
        remove_self_from_standby()
    logger.info("[STATUS] Watchdog cleanup executed cleanly. Shutting down wrapper.")

keep_alive()

if __name__ == "__main__":
    main()
