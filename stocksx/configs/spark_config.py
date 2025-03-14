from dataclasses import dataclass, field
import os
from typing import Dict

@dataclass
class SparkConfig:
    """Configuration settings for Spark"""
    # Basic Spark settings
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
    
    # Iceberg configuration
    iceberg_enabled: bool = True
    iceberg_warehouse: str = "data/warehouse/iceberg"
    iceberg_catalog: str = "spark_catalog"
    iceberg_namespace: str = "raw_data"
    
    # Hive metastore settings
    hive_metastore_enabled: bool = True
    hive_metastore_host: str = "localhost"
    hive_metastore_port: int = 6969
    hive_metastore_db: str = "metastore"
    hive_metastore_user: str = "hive"
    hive_metastore_password: str = "password"
    hive_warehouse: str ="data/warehouse/hive"


    # Garbage collector settings
    garbage_collectors: Dict[str, str] = field(default_factory=lambda: {
        "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
        "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors": "G1 Old Generation"
    })
    