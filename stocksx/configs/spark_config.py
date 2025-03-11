from dataclasses import dataclass, field
from typing import Dict, Optional

@dataclass
class SparkConfig:
    """Configuration settings for Spark"""
    app_name: str = "StocksX_Price_and_News_Influences"
    arrow_enabled: bool = True
    shuffle_partitions: int = 16
    parallelism: int = 16
    executor_memory: str = "8g"
    driver_memory: str = "8g"
    network_timeout: str = "500s"
    heartbeat_interval: str = "30s"
    worker_timeout: str = "120s" 
    lookup_timeout: str = "120s"
    ask_timeout: str = "60s"
    serializer: str = "org.apache.spark.serializer.KryoSerializer"
    kryo_registration_required: str = "false"
    master: str = "local[*]"
    
    # Add Iceberg configuration
    iceberg_enabled: bool = False
    iceberg_warehouse: str = "stocksx/data/iceberg_warehouse"
    iceberg_catalog: str = "local"

    garbage_collectors: Dict[str, str] = field(default_factory=lambda: {
        "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
        "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors": "G1 Old Generation"
    })