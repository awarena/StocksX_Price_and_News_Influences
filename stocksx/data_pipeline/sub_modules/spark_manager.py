import os
import sys

from stocksx.configs.spark_config import SparkConfig
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
                # Specify Iceberg packages
                iceberg_packages = [
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
                    "org.apache.iceberg:iceberg-parquet:1.8.1"
                ]
                
                # Join packages into comma-separated string
                packages_string = ",".join(iceberg_packages)
                builder = builder.config("spark.jars.packages", packages_string)
                    
                # Required for Iceberg integration
                builder = builder \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                    .config("spark.sql.catalog.spark_catalog.type", "hive") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}.type", "hadoop") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}.warehouse", self.config.iceberg_warehouse) \
                    .config("spark.sql.defaultCatalog", self.config.iceberg_catalog)
        
            # Add garbage collector settings
            for key, value in self.config.garbage_collectors.items():
                builder = builder.config(key, value)
                
            self._session = builder.getOrCreate()
            # Set Python path for executor processes
            self._session.sparkContext.setSystemProperty(
                "spark.executor.extraPythonPath", 
                "${PYTHONPATH}"  # This will inherit the Python path from the driver
            )
        return self._session

    def cleanup(self):
        """Stop the Spark session."""
        if self._session:
            self._session.stop()
            self._session = None