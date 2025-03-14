import os
import sys
import argparse
from datetime import date
from pandas_market_calendars import get_calendar
from stocksx.configs.spark_config import SparkConfig
from stocksx.data_pipeline.stocks_pipeline.ingestion_stock_iceberg import StockDataManager
from stocksx.utils.logger import Logger
from stocksx.configs.processing_config import ProcessingConfig

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def parse_arguments():
    parser = argparse.ArgumentParser(description="Run the stock data pipeline with optional Spark configuration.")

    # Allow overriding any SparkConfig setting via CLI
    parser.add_argument("--app_name", type=str, help="Spark application name")
    parser.add_argument("--arrow_enabled", type=bool, help="Enable Apache Arrow (True/False)")
    parser.add_argument("--shuffle_partitions", type=int, help="Number of shuffle partitions")
    parser.add_argument("--parallelism", type=int, help="Default parallelism level")
    parser.add_argument("--executor_memory", type=str, help="Executor memory (e.g., '5g')")
    parser.add_argument("--driver_memory", type=str, help="Driver memory (e.g., '5g')")
    parser.add_argument("--network_timeout", type=str, help="Spark network timeout (e.g., '800s')")
    parser.add_argument("--heartbeat_interval", type=str, help="Executor heartbeat interval")
    parser.add_argument("--worker_timeout", type=str, help="Worker timeout")
    parser.add_argument("--lookup_timeout", type=str, help="Lookup timeout")
    parser.add_argument("--ask_timeout", type=str, help="Ask timeout")
    parser.add_argument("--serializer", type=str, help="Spark serializer")
    parser.add_argument("--kryo_registration_required", type=str, help="Require Kryo registration (True/False)")
    parser.add_argument("--master", type=str, help="Spark master (e.g., 'local[*]')")
    parser.add_argument("--iceberg_enabled", type=bool, help="Enable Apache Iceberg (True/False)")
    parser.add_argument("--iceberg_warehouse", type=str, help="Iceberg warehouse path")
    parser.add_argument("--iceberg_catalog", type=str, help="Iceberg catalog name")
    parser.add_argument("--iceberg_namespace", type=str, help="Iceberg namespace name")
    parser.add_argument("--hive_metastore_enabled", type=bool, help="Enable Hive metastore (True/False)")
    parser.add_argument("--hive_metastore_host", type=str, help="Hive metastore host")
    parser.add_argument("--hive_metastore_port", type=int, help="Hive metastore port")
    parser.add_argument("--hive_metastore_db", type=str, help="Hive metastore database")
    parser.add_argument("--hive_metastore_user", type=str, help="Hive metastore username")
    parser.add_argument("--hive_metastore_password", type=str, help="Hive metastore password")
    parser.add_argument("--hive_warehouse", type=str, help="Hive warehouse path")

    return parser.parse_args()

def main():
    if not any(get_calendar("NASDAQ").valid_days(start_date=date.today(), end_date=date.today())):
        print("Today's not trading day, skipping stock data update.")
        processing_config = ProcessingConfig()
        logger = Logger(processing_config)
        logger.info("Today's not trading day, skipping stock data update.")
        return
        
    args = parse_arguments()

    # Load default SparkConfig
    default_config = SparkConfig()

    
    # Create a modified config with overridden values
    custom_spark_config = SparkConfig(
        app_name=args.app_name if args.app_name else default_config.app_name,
        arrow_enabled=args.arrow_enabled if args.arrow_enabled is not None else default_config.arrow_enabled,
        shuffle_partitions=args.shuffle_partitions if args.shuffle_partitions is not None else default_config.shuffle_partitions,
        parallelism=args.parallelism if args.parallelism is not None else default_config.parallelism,
        executor_memory=args.executor_memory if args.executor_memory else default_config.executor_memory,
        driver_memory=args.driver_memory if args.driver_memory else default_config.driver_memory,
        network_timeout=args.network_timeout if args.network_timeout else default_config.network_timeout,
        heartbeat_interval=args.heartbeat_interval if args.heartbeat_interval else default_config.heartbeat_interval,
        worker_timeout=args.worker_timeout if args.worker_timeout else default_config.worker_timeout,
        lookup_timeout=args.lookup_timeout if args.lookup_timeout else default_config.lookup_timeout,
        ask_timeout=args.ask_timeout if args.ask_timeout else default_config.ask_timeout,
        serializer=args.serializer if args.serializer else default_config.serializer,
        kryo_registration_required=args.kryo_registration_required if args.kryo_registration_required else default_config.kryo_registration_required,
        master=args.master if args.master else default_config.master,
        iceberg_enabled=args.iceberg_enabled if args.iceberg_enabled is not None else True,  # Default to True
        iceberg_warehouse=args.iceberg_warehouse if args.iceberg_warehouse else default_config.iceberg_warehouse,
        iceberg_catalog=args.iceberg_catalog if args.iceberg_catalog else default_config.iceberg_catalog,
        iceberg_namespace=args.iceberg_namespace if args.iceberg_namespace else default_config.iceberg_namespace,
        hive_metastore_enabled=args.hive_metastore_enabled if args.hive_metastore_enabled is not None else default_config.hive_metastore_enabled,
        hive_metastore_host=args.hive_metastore_host if args.hive_metastore_host else default_config.hive_metastore_host,
        hive_metastore_port=args.hive_metastore_port if args.hive_metastore_port else default_config.hive_metastore_port,
        hive_metastore_db=args.hive_metastore_db if args.hive_metastore_db else default_config.hive_metastore_db,
        hive_metastore_user=args.hive_metastore_user if args.hive_metastore_user else default_config.hive_metastore_user,
        hive_metastore_password=args.hive_metastore_password if args.hive_metastore_password else default_config.hive_metastore_password,
        hive_warehouse=args.hive_warehouse if args.hive_warehouse else default_config.hive_warehouse,
        garbage_collectors=default_config.garbage_collectors  # Retain default garbage collector settings
    )

    try:
        manager = StockDataManager(spark_config=custom_spark_config)
        print("Starting stock data download...")
        manager.download_all_stock_data()
        print("Stock data download completed successfully.")
        
    except Exception as e:
        print(f"ERROR: Failed to process stock data: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        print("Cleaning up resources...")
        if 'manager' in locals():
            manager.cleanup()

if __name__ == "__main__":
    main()