import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# =====================================================================
# CONFIGURATION & CONSTANTS
# =====================================================================
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
OWNERS = [int(x.strip()) for x in os.getenv("OWNERS").split(",") if x.strip()]
MONITORED_CHANNELS = [int(x.strip()) for x in os.getenv("MONITORED_CHANNELS", "").split(",") if x.strip()]

# Logging configuration
MAX_LOG_AGE_HOURS = int(os.getenv("MAX_LOG_AGE_HOURS", 24))
MAX_LOG_SIZE_BYTES = int(os.getenv("MAX_LOG_SIZE_BYTES", 2 * 1024 * 1024))  # 2MB default

# External OCR service URL (optional)
EXTERNAL_OCR_SERVICE_URL = os.getenv("EXTERNAL_OCR_SERVICE_URL","http://localhost:8181/api/ocr")


# Global DB client references
db_client = AsyncIOMotorClient(MONGO_URI)
db = db_client["market_intelligence"]
