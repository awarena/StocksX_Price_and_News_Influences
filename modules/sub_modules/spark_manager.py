import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from configs.spark_config import SparkConfig
from pyspark.sql import SparkSession

class SparkManager:
    """Manages Apache Spark session."""
    def __init__(self, config: SparkConfig):
        """
        Initialize SparkManager with configurations.
        
        Args:
            config (SparkConfig): Configuration settings for Spark.
        """
        self.config = config
        self._session = None

    @property
    def session(self) -> SparkSession:
        """Get or create a Spark session."""
        if self._session is None:
            builder = (SparkSession.builder
                .appName(self.config.app_name)
                .config("spark.sql.execution.arrow.pyspark.enabled", self.config.arrow_enabled)
                .config("spark.sql.shuffle.partitions", self.config.shuffle_partitions)
                .config("spark.default.parallelism", self.config.parallelism)
                .config("spark.executor.memory", self.config.executor_memory)
                .config("spark.driver.memory", self.config.driver_memory)   
                .config("spark.network.timeout", self.config.network_timeout)
                .config("spark.executor.heartbeatInterval", self.config.heartbeat_interval)
                .config("spark.worker.timeout", self.config.worker_timeout)
                .config("spark.akka.timeout", self.config.lookup_timeout)
                .config("spark.akka.askTimeout", self.config.ask_timeout)
                .config("spark.serializer", self.config.serializer)
                .config("spark.kryo.registrationRequired", self.config.kryo_registration_required)
                .config("spark.master", self.config.master)
                )            
            for key, value in self.config.garbage_collectors.items():
                builder = builder.config(key, value)
            self._session = builder.getOrCreate()
        return self._session

    def cleanup(self):
        """Stop the Spark session."""
        if self._session:
            self._session.stop()
            self._session = None