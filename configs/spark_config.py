
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SparkConfig:
    """Configuration settings for Spark"""
    app_name: str = "StocksX_Price_and_News_Influences"
    arrow_enabled: bool = True
    shuffle_partitions: int = 16
    parallelism: int = 16
    executor_memory: str = "5g"
    driver_memory: str = "5g"
    network_timeout: str = "800s"
    heartbeat_interval: str = "600"
    worker_timeout: str = "800"
    lookup_timeout: str = "800"
    ask_timeout: str = "800"
    serializer: str = "org.apache.spark.serializer.KryoSerializer"
    kryo_registration_required: str = "false"
    master: str = "local[*]"
    garbage_collectors: Dict[str, str] = field(default_factory=lambda: {
        "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
        "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors": "G1 Old Generation"
    })