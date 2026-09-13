import os
import socket
import logging
from flask import Flask
from threading import Thread

logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Port resolution ──────────────────────────────────────────────────────────
# Desired port comes from the environment (set in .env / docker-compose).
# If that port is already occupied, we scan upward until we find a free one
# so the webserver always starts regardless of port mismatches.

_DESIRED_PORT = int(os.getenv("WEBSERVER_PORT", 5090))
_PORT_SCAN_LIMIT = 100  # how many ports above the desired one to try


def _is_port_free(port: int) -> bool:
    """Return True if *port* can be bound on 0.0.0.0 right now."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def _resolve_port(desired: int) -> int:
    """
    Try *desired* first.  If it is taken, scan upward up to _PORT_SCAN_LIMIT
    ports and return the first free one found.
    Raises RuntimeError if no free port is found in that range.
    """
    if _is_port_free(desired):
        return desired

    logger.warning(
        f"[webserver] Port {desired} is not available. "
        f"Scanning {desired + 1}–{desired + _PORT_SCAN_LIMIT} for a free port..."
    )
    for port in range(desired + 1, desired + _PORT_SCAN_LIMIT + 1):
        if _is_port_free(port):
            logger.warning(
                f"[webserver] Port {desired} was busy — using port {port} instead. "
                f"Update WEBSERVER_PORT in .env and docker-compose.yml to make this permanent."
            )
            return port

    raise RuntimeError(
        f"[webserver] No free port found in range "
        f"{desired}–{desired + _PORT_SCAN_LIMIT}. "
        "Please free a port or increase _PORT_SCAN_LIMIT."
    )


# Resolve once at import time so keep_alive() always uses a known good port.
WEBSERVER_PORT = _resolve_port(_DESIRED_PORT)

if WEBSERVER_PORT != _DESIRED_PORT:
    print(
        f"[webserver] ⚠️  Desired port {_DESIRED_PORT} was busy. "
        f"Webserver started on port {WEBSERVER_PORT}."
    )
else:
    print(f"[webserver] ✅ Webserver will start on port {WEBSERVER_PORT}.")

# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return "I'm alive"

# ── Server thread ─────────────────────────────────────────────────────────────

def run():
    app.run(host='0.0.0.0', port=WEBSERVER_PORT)

def keep_alive():
    t = Thread(target=run, daemon=True)
    t.start()
