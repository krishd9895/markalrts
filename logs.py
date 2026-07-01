import logging
import os
import re
from datetime import datetime, timedelta, timezone
from config import MAX_LOG_AGE_HOURS, MAX_LOG_SIZE_BYTES

# Indian Standard Time: UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))


# Custom logging formatter to use IST time
class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, IST)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S IST")


def trim_log_file(log_path):
    """Trim old lines from log file when it exceeds max size, or when entries are too old."""
    if not os.path.exists(log_path):
        return
    
    file_size = os.path.getsize(log_path)
    now = datetime.now(IST)
    cutoff_time = now - timedelta(hours=MAX_LOG_AGE_HOURS)
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # First filter out lines older than MAX_LOG_AGE_HOURS
        filtered_lines = []
        for line in lines:
            # Try to parse timestamp from line
            timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if timestamp_match:
                try:
                    # Parse the timestamp
                    line_time_str = timestamp_match.group(1)
                    line_time = datetime.strptime(line_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                    if line_time >= cutoff_time:
                        filtered_lines.append(line)
                except Exception:
                    # If timestamp parsing fails, keep the line
                    filtered_lines.append(line)
            else:
                # If no timestamp, keep the line
                filtered_lines.append(line)
        
        # Now check if we still need to trim more lines to get under size limit
        # Estimate size per line
        total_size = sum(len(line.encode('utf-8')) for line in filtered_lines)
        
        while total_size > MAX_LOG_SIZE_BYTES and len(filtered_lines) > 0:
            # Remove oldest line
            removed = filtered_lines.pop(0)
            total_size -= len(removed.encode('utf-8'))
        
        # Write trimmed lines back to file
        with open(log_path, 'w', encoding='utf-8') as f:
            f.writelines(filtered_lines)
    
    except Exception as e:
        print(f"Error trimming log file {log_path}: {e}")


class ManagedFileHandler(logging.FileHandler):
    """Custom file handler that manages log file size and age before emitting each record."""
    def emit(self, record):
        try:
            # Trim log file before emitting new record
            if self.stream:
                self.stream.flush()
                trim_log_file(self.baseFilename)
            # Call parent class emit
            super().emit(record)
        except Exception:
            self.handleError(record)


def setup_channel_logger():
    logs_dir = "channel_logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    log_filename = os.path.join(logs_dir, "channel_activity.log")
    
    # Trim log file on startup
    trim_log_file(log_filename)

    logger = logging.getLogger("channel_activity")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        logger.handlers.clear()

    file_handler = ManagedFileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    log_format = ISTFormatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S IST"
    )

    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    logger.propagate = False
    
    logger.info("Channel activity logger initialized successfully!")
    return logger


def setup_bot_activity_logger():
    logs_dir = "bot_activity_logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    log_filename = os.path.join(logs_dir, "bot_activity.log")
    
    # Trim log file on startup
    trim_log_file(log_filename)

    logger = logging.getLogger("bot_activity")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        logger.handlers.clear()

    file_handler = ManagedFileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    log_format = ISTFormatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S IST"
    )

    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    logger.propagate = False
    
    logger.info("Bot activity logger initialized successfully!")
    return logger


def setup_ocr_logger():
    logs_dir = "ocr_logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    log_filename = os.path.join(logs_dir, "ocr_activity.log")
    
    # Trim log file on startup
    trim_log_file(log_filename)

    logger = logging.getLogger("ocr_activity")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    file_handler = ManagedFileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.INFO)

    # No timestamps for OCR logs - just clean messages
    log_format = logging.Formatter("%(message)s")
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    logger.propagate = False
    
    logger.info("OCR activity logger initialized successfully!")
    return logger


# Global logger instances
channel_logger = setup_channel_logger()
bot_activity_logger = setup_bot_activity_logger()
ocr_logger = setup_ocr_logger()
