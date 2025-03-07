from dataclasses import dataclass


@dataclass
class ProcessingConfig:
    """Configuration settings for data processing"""
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: int = 5
    data_path: str = "data/raw_data/parquets/"
    metadata_path: str = "data/metadata/"
    log_path: str = "logs/stock_ingestion_logs/update_log.txt"
    log_max_bytes: int = 10 * 1024 * 1024
    log_backup_count: int = 3
    parallelism: int = 200