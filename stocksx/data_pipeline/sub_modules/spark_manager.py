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
            # Base packages list
            packages = []
            
            # Add Iceberg packages if enabled
            if hasattr(self.config, 'iceberg_enabled') and self.config.iceberg_enabled:
                iceberg_packages = [
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
                    "org.apache.iceberg:iceberg-parquet:1.8.1"
                ]
                packages.extend(iceberg_packages)
            
            # Add PostgreSQL JDBC driver if metastore is enabled
            if hasattr(self.config, 'hive_metastore_enabled') and self.config.hive_metastore_enabled:
                packages.append("org.postgresql:postgresql:42.6.0")
            
            # Build the session
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
            
            # Add packages if any exist
            if packages:
                packages_string = ",".join(packages)
                builder = builder.config("spark.jars.packages", packages_string)
                
            # Add Iceberg-specific configuration if enabled
            if hasattr(self.config, 'iceberg_enabled') and self.config.iceberg_enabled:
                # Required for Iceberg integration
                builder = builder \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}.type", "hadoop") \
                    .config(f"spark.sql.catalog.{self.config.iceberg_catalog}.warehouse", self.config.iceberg_warehouse) \
                    .config("spark.sql.defaultCatalog", self.config.iceberg_catalog)\
                    .config("spark.sql.warehouse.dir", self.config.iceberg_warehouse)
            
            # Add PostgreSQL Hive metastore configuration if enabled
            if hasattr(self.config, 'hive_metastore_enabled') and self.config.hive_metastore_enabled:
                # Make warehouse path consistent
                warehouse_dir = self.config.iceberg_warehouse
                # if not warehouse_dir.startswith("file:///"):
                #     warehouse_dir = f"file:///{warehouse_dir.replace(os.sep, '/')}"
                
                # Configure Hive metastore with PostgreSQL
                builder = builder \
                    .config("spark.sql.warehouse.dir", warehouse_dir) \
                    .config("javax.jdo.option.ConnectionURL", 
                           f"jdbc:postgresql://{self.config.hive_metastore_host}:{self.config.hive_metastore_port}/{self.config.hive_metastore_db}") \
                    .config("javax.jdo.option.ConnectionDriverName", 
                           "org.postgresql.Driver") \
                    .config("javax.jdo.option.ConnectionUserName", self.config.hive_metastore_user) \
                    .config("javax.jdo.option.ConnectionPassword", self.config.hive_metastore_password) \
                    .config("hive.metastore.schema.verification", "false") \
                    .enableHiveSupport()
        
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