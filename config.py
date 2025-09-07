import os
from dotenv import load_dotenv

load_dotenv()

# Marzban API settings
MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))
IP_AGENT_PORT = os.getenv("IP_AGENT_PORT", "").strip()
IP_AGENT_SCHEME = os.getenv("IP_AGENT_SCHEME", "http").strip()
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8443"))

# Telegram settings
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() in ("true", "1", "yes")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NODE_REMINDER_INTERVAL = int(os.getenv("NODE_REMINDER_INTERVAL", "6"))

# Paths
NODE_CANDIDATE_PATHS = [
    "/connections",
    "/clients",
    "/status",
]

# Runtime state
stats = {
    "nodes": [],
    "last_update": None,
    "error": None,
    "system": None,
    "nodes_usage": None,
    "users_usage": None,
    "port_8443": {"unique_clients": 0, "clients": []},
}

# Token cache
_token_cache = {"token": None, "fetched_at": 0, "ttl": 300}