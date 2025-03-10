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
                
            # Add Iceberg-specific configuration if enabled
            if hasattr(self.config, 'iceberg_enabled') and self.config.iceberg_enabled:
                # Required for Iceberg integration
                builder = builder \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                    .config("spark.sql.catalog.spark_catalog.type", "hive") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}.type", "hadoop") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}.warehouse", self.config.iceberg_warehouse) \
                    .config("spark.sql.defaultCatalog", self.config.iceberg_catalog)
                
                # If using local development, you might need this for file access
                if self.config.master.startswith("local"):
                    builder = builder.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            
            # Add garbage collector settings
            for key, value in self.config.garbage_collectors.items():
                builder = builder.config(key, value)
                
            self._session = builder.getOrCreate()
        return self._session

    def cleanup(self):
        """Stop the Spark session."""
        if self._session:
            self._session.stop()
            self._session = None