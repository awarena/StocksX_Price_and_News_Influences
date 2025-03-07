import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logging.handlers import TimedRotatingFileHandler
import logging
from configs.processing_config import ProcessingConfig

class SizeAndTimeRotatingFileHandler(TimedRotatingFileHandler):
    """Custom file handler that rotates logs based on both size and time."""
    def __init__(self, filename, when='midnight', interval=1, backupCount=0, encoding=None, delay=False, utc=False, maxBytes=0):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc)
        self.maxBytes = maxBytes

    def shouldRollover(self, record):
        """Determine if rollover should occur based on log file size."""
        if self.stream is None:
            self.stream = self._open()
        if self.maxBytes > 0:
            self.stream.seek(0, 2)
            if self.stream.tell() >= self.maxBytes:
                return 1
        return super().shouldRollover(record)
    
class Logger:
    """Handles logging operations."""
    def __init__(self, config: ProcessingConfig):
        """
        Initialize logger with file and console handlers.
        
        Args:
            config (ProcessingConfig): Configuration for logging.
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        # if log path does not exist, create it
        if not os.path.exists(os.path.dirname(config.log_path)):
            os.makedirs(os.path.dirname(config.log_path))
        file_handler = SizeAndTimeRotatingFileHandler(
            config.log_path, when="midnight", interval=1, backupCount=config.log_backup_count, maxBytes=config.log_max_bytes
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)