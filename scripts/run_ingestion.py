import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
from datetime import date

from pandas_market_calendars import get_calendar

from configs.spark_config import SparkConfig
from modules.ingestion_stock import StockDataManager
from modules.sub_modules.logger import Logger

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

    return parser.parse_args()

def main():
    if not any(get_calendar("NASDAQ").valid_days(start_date=date.today(), end_date=date.today())):
        print("Today's not trading day, skipping stock data update.")
        logger = Logger()
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
        garbage_collectors=default_config.garbage_collectors  # Retain default garbage collector settings
    )

    manager = StockDataManager(spark_config=custom_spark_config)
    try:
        manager.download_all_stock_data()
    finally:
        manager.cleanup()

if __name__ == "__main__":
    main()