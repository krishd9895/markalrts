import logging
import os
from datetime import datetime, timedelta, timezone

# Indian Standard Time: UTC+5:30 (duplicate from config to avoid import issues)
IST = timezone(timedelta(hours=5, minutes=30))

# Maximum log file size in bytes (2 MB)
MAX_LOG_SIZE = 2 * 1024 * 1024  # 2,097,152 bytes


def manage_log_size(log_filename):
    """
    Checks if log file exceeds MAX_LOG_SIZE and trims it if necessary.
    Keeps the most recent log entries.
    """
    if not os.path.exists(log_filename):
        return
    
    file_size = os.path.getsize(log_filename)
    if file_size <= MAX_LOG_SIZE:
        return
    
    # Log file is too big - trim it
    try:
        with open(log_filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Calculate how many lines to keep (keep about 60% of the file)
        target_size = int(MAX_LOG_SIZE * 0.6)
        approx_lines_to_keep = int(len(lines) * (target_size / file_size))
        
        # Ensure we keep at least 100 lines
        approx_lines_to_keep = max(approx_lines_to_keep, 100)
        
        # Keep the most recent lines
        lines_to_keep = lines[-approx_lines_to_keep:]
        
        # Write back the trimmed content
        with open(log_filename, 'w', encoding='utf-8') as f:
            f.writelines(lines_to_keep)
        
        print(f"Trimmed log file {log_filename} from {file_size} bytes to ~{target_size} bytes")
    except Exception as e:
        print(f"Error trimming log file {log_filename}: {e}")


class SizeManagedFileHandler(logging.FileHandler):
    """
    Custom FileHandler that checks and manages log size before emitting records.
    """
    def emit(self, record):
        try:
            # Check log size before emitting
            if self.stream:
                self.stream.flush()
                manage_log_size(self.baseFilename)
            # Call parent class emit
            super().emit(record)
        except Exception:
            self.handleError(record)


def setup_channel_logger():
    """
    Sets up a logger that writes detailed channel activity to a file ONLY.
    Logs all messages/PDFs seen in channels and whether they match our portfolio.
    No console output - only file logging.
    """
    # Create logs directory if it doesn't exist
    logs_dir = "channel_logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # Create a unique log filename with today's date
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    log_filename = os.path.join(logs_dir, f"channel_activity_{today_str}.log")

    # Configure the logger
    logger = logging.getLogger("channel_activity")
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()

    # Size-managed file handler
    file_handler = SizeManagedFileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # Log format
    log_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S IST"
    )

    file_handler.setFormatter(log_format)

    logger.addHandler(file_handler)

    # Prevent the logger from propagating to the root logger (which might have console handlers)
    logger.propagate = False

    return logger


def setup_activity_logger():
    """
    Sets up a logger that tracks AI analysis and user forwards to a file ONLY.
    Logs:
    - What messages were forwarded to users
    - What was sent to AI
    - Whether AI analysis succeeded or failed
    No console output - only file logging.
    """
    # Create logs directory if it doesn't exist
    logs_dir = "activity_logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # Create a unique log filename with today's date
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    log_filename = os.path.join(logs_dir, f"activity_{today_str}.log")

    # Configure the logger
    logger = logging.getLogger("activity")
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()

    # Size-managed file handler
    file_handler = SizeManagedFileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # Log format
    log_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S IST"
    )

    file_handler.setFormatter(log_format)

    logger.addHandler(file_handler)

    # Prevent the logger from propagating to the root logger (which might have console handlers)
    logger.propagate = False

    return logger


# Global logger instances
channel_logger = setup_channel_logger()
activity_logger = setup_activity_logger()
