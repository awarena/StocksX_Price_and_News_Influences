from stocksx.configs.spark_config import SparkConfig
from stocksx.data_pipeline.sub_modules.spark_manager import SparkManager


# Create configuration
config = SparkConfig(
    iceberg_enabled=True,
    iceberg_warehouse="stocksx/data/iceberg_warehouse",
    hive_metastore_enabled=True,
    hive_metastore_host="localhost",
    hive_metastore_port=6969,
    hive_metastore_db="metastore",
    hive_metastore_user="hive",
    hive_metastore_password="password"
)

# Create manager
spark_manager = SparkManager(config)

# Verify configuration
spark_manager.verify_configuration()

# Get session
spark = spark_manager.session

# Try creating a test table
# spark.sql("""
# CREATE TABLE IF NOT EXISTS spark_catalog.default.test_table (
#   id INT, 
#   name STRING
# ) USING iceberg
# """)

# # Check if table was created
# spark.sql("SHOW TABLES IN spark_catalog.default").show()