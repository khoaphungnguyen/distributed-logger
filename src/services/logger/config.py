import os
import logging
from logging.handlers import RotatingFileHandler

# Get environment config
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_NUM = LOG_LEVELS.get(LOG_LEVEL, logging.INFO)
LOG_FORMAT = os.getenv("LOG_FORMAT", "[%(asctime)s] %(levelname)s: %(message)s")
LOG_FREQUENCY = int(os.getenv("LOG_FREQUENCY", "5"))
LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")
LOG_MAX_SIZE = 1 * 1024 * 1024  # 1MB
LOG_BACKUP_COUNT = 3  # Keep 3 rotated files

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL_NUM)

    formatter = logging.Formatter(LOG_FORMAT)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(LOG_LEVEL_NUM)

    # Rotating file handler
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_SIZE, backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)  # Save all logs to file

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
