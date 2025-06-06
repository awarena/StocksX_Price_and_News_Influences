from logging.handlers import TimedRotatingFileHandler
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import yfinance as yf
from datetime import date
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, LongType
import logging
from typing import Optional, Dict, List, Tuple, Any
from pandas_market_calendars import get_calendar
import argparse
from configs.processing_config import ProcessingConfig
from configs.spark_config import SparkConfig


os.environ["PYSPARK_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
# @dataclass
# class ProcessingConfig:
#     """Configuration settings for data processing"""
#     batch_size: int = 100
#     max_retries: int = 3
#     retry_delay: int = 5
#     data_path: str = "fetch_data/raw_data/parquets/"
#     log_path: str = "fetch_data/logs/update_log.txt"
#     log_max_bytes: int = 10 * 1024 * 1024
#     log_backup_count: int = 3
#     parallelism: int = 200

# @dataclass
# class SparkConfig:
#     """Configuration settings for Spark"""
#     app_name: str = "StocksX_Price_and_News_Influences"
#     arrow_enabled: bool = True
#     shuffle_partitions: int = 16
#     parallelism: int = 16
#     executor_memory: str = "5g"
#     driver_memory: str = "5g"
#     network_timeout: str = "800s"
#     heartbeat_interval: str = "600"
#     worker_timeout: str = "800"
#     lookup_timeout: str = "800"
#     ask_timeout: str = "800"
#     serializer: str = "org.apache.spark.serializer.KryoSerializer"
#     kryo_registration_required: str = "false"
#     master: str = "local[*]"
#     garbage_collectors: Dict[str, str] = field(default_factory=lambda: {
#         "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
#         "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors": "G1 Old Generation"
#     })
class SizeAndTimeRotatingFileHandler(TimedRotatingFileHandler):
    """Custom file handler that rotates logs based on both size and time."""
    def __init__(self, filename, when='midnight', interval=1, backupCount=0, encoding=None, delay=False, utc=False, maxBytes=0):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc)
        self.maxBytes = maxBytes

    def shouldRollover(self, record):
        """Determine if rollover should occur based on log file size."""
        if self.stream is None:
            self.stream = self._open()
        if self.maxBytes > 0:
            self.stream.seek(0, 2)
            if self.stream.tell() >= self.maxBytes:
                return 1
        return super().shouldRollover(record)
    
class Logger:
    """Handles logging operations."""
    def __init__(self, config: ProcessingConfig):
        """
        Initialize logger with file and console handlers.
        
        Args:
            config (ProcessingConfig): Configuration for logging.
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        # if log path does not exist, create it
        if not os.path.exists(os.path.dirname(config.log_path)):
            os.makedirs(os.path.dirname(config.log_path))
        file_handler = SizeAndTimeRotatingFileHandler(
            config.log_path, when="midnight", interval=1, backupCount=config.log_backup_count, maxBytes=config.log_max_bytes
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)

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

class DataSchema:
    """Defines schema for stock price and last update data."""
    
    @property
    def stock_prices(self) -> StructType:
        """Returns schema for stock price data.
        
        Returns:
            StructType: Schema containing fields for stock prices.
        """
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("trade_date", DateType(), False),
            StructField("last_update", DateType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", LongType(), True)
        ])

    @property
    def last_update(self) -> StructType:
        """Returns schema for last update tracking.
        
        Returns:
            StructType: Schema containing fields for tracking last updates.
        """
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("last_update", DateType(), False)
        ])
    
    @property
    def stock_prices_processing(self) -> StructType:
        """Returns schema for stock price data during processing.
        
        Returns:
            StructType: Schema containing fields for stock prices during processing.
        """
        return StructType([
            StructField("Date", DateType(), True),
            StructField("Close", FloatType(), True),
            StructField("High", FloatType(), True),
            StructField("Low", FloatType(), True),
            StructField("Open", FloatType(), True),
            StructField("Volume", LongType(), True),
            StructField("symbol", StringType(), True),
            StructField("is_etf", StringType(), True),
        ])
    

class DataStore:
    """Manages data storage and retrieval operations"""
    def __init__(self, spark_manager: SparkManager, schema: DataSchema, config: ProcessingConfig):
        """
        Initializes the DataStore.
        
        Args:
            spark_manager (SparkManager): Manages Spark session.
            schema (DataSchema): Defines schemas for stock data.
            config (ProcessingConfig): Configuration settings for data storage.
        """
        self.spark = spark_manager
        self.schema = schema
        self.config = config
        self.dfs: Dict[str, Any] = {}
        self.accumulated_last_update: Optional[Any] = None


    def load_parquet(self, name: str) -> Any:
        """Loads a Parquet file into a Spark DataFrame.
        
        Args:
            name (str): Name of the Parquet file (without extension).
        
        Returns:
            Any: Spark DataFrame containing the loaded data.
        """
        # Check if datapath exists, if not create it
        if not os.path.exists(self.config.data_path):
            os.makedirs(self.config.data_path)
        path = f"{self.config.data_path}{name}.parquet"
        schema = getattr(self.schema, name)
        if os.path.exists(path):
            if name == "last_update":
                return self.spark.session.read.parquet(path).cache()
            return self.spark.session.read.parquet(path)
        return self.spark.session.createDataFrame([], schema)

    def load_all_dataframes(self):
        """Loads all stock-related Parquet files into memory."""
        self.dfs["stock_prices"] = self.load_parquet("stock_prices")
        self.dfs["last_update"] = self.load_parquet("last_update")

    def write_dataframe(self, df: Any, name: str):
        """Writes a Spark DataFrame to a Parquet file.
        
        Args:
            df (Any): Spark DataFrame to be written.
            name (str): Name of the Parquet file (without extension).
        """
        if name == "last_update":
            df.write.mode("append").parquet(f"{self.config.data_path}{name}.parquet")
        else:
            df.write.mode("append").partitionBy("symbol").parquet(f"{self.config.data_path}{name}.parquet")

    def accumulate_last_update(self, new_data: Any):
        """Accumulates new stock update data before merging.
        
        Args:
            new_data (Any): Spark DataFrame containing new update data.
        """
        if self.accumulated_last_update is None:
            self.accumulated_last_update = new_data
        else:
            self.accumulated_last_update = self.accumulated_last_update.union(new_data)

    def merge_last_update(self):
        """Merge accumulated data into the last_update table"""
        if self.accumulated_last_update is None:
            return

        # coalesce the accumulated data to reduce partitions
        accumulated_df = self.accumulated_last_update.coalesce(8).alias("accumulated")
        last_update_df = self.dfs["last_update"].alias("last_update")
        
        # combine all data (both existing and new)
        all_symbols_df = accumulated_df.union(last_update_df).distinct()

        deduped_df = all_symbols_df.groupBy("symbol").agg(
            F.max("last_update").alias("last_update")
        ).repartition(self.config.parallelism)  # Allow Spark to distribute workload better

        
        # Write the deduplicated DataFrame to the last_update table
        deduped_df.write.mode("overwrite").parquet(f"{self.config.data_path}last_update.parquet")

        # Reload the last_update DataFrame
        self.dfs["last_update"] = self.load_parquet("last_update")
        self.accumulated_last_update = None
        
class StockDataFetcher:
    """Handles stock data fetching operations"""
    def __init__(self, config: ProcessingConfig, logger: Logger):
        """
        Initializes the stock data fetcher.
        
        Args:
            config (ProcessingConfig): Configuration settings for stock data fetching.
            logger (Logger): Logger instance for logging events.
        """
        self.config = config
        self.logger = logger

class StockDataFetcher:
    """Handles stock data fetching operations"""

    def __init__(self, config: ProcessingConfig, logger: Logger):
        """Initializes the stock data fetcher."""
        self.config = config
        self.logger = logger

    @staticmethod
    def fetch_stock_data_batch(symbols_batch: List[Tuple[str, bool, Optional[date]]], config: dict) -> List[dict]:
        """Process a batch of symbols with one API call per period group"""
        
        # Group by period to minimize API calls
        symbols_by_period = {}
        for symbol, is_etf, last_update in symbols_batch:
            if last_update is None:
                period = "max"
            else:
                days = (date.today() - last_update).days
                if days <= 0:  # Skip if already up to date
                    continue
                period = f"{days}d"
                
            if period not in symbols_by_period:
                symbols_by_period[period] = []
            symbols_by_period[period].append((symbol, is_etf))
        
        results = []
        
        # Process each period group with one API call
        for period, symbols in symbols_by_period.items():
            # Extract just the symbols
            symbol_names = [s[0] for s in symbols]
            symbol_dict = {s[0]: s[1] for s in symbols}  # Map symbol to is_etf
            
            # Try up to max_retries
            for attempt in range(config['max_retries']):
                try:
                    # One API call for the whole batch
                    multi_df = yf.download(
                        symbol_names, 
                        period=period, 
                        interval="1d", 
                        progress=False, 
                        auto_adjust=True, 
                        group_by="ticker"
                    )
                    multi_df.to_csv("fetch_data/raw_data/stock_data.csv")
                    # Process each symbol from the result
                    for symbol in symbol_names:
                        if isinstance(multi_df.columns, pd.MultiIndex) and symbol in multi_df.columns.levels[0]:
                            symbol_df = multi_df[symbol].reset_index()
                            if not symbol_df.empty:
                                symbol_df["symbol"] = symbol
                                symbol_df["is_etf"] = symbol_dict[symbol]
                                symbol_df["Date"] = symbol_df["Date"].apply(lambda x: x.date() if isinstance(x, pd.Timestamp) else x)
                                # convert column to long
                                symbol_df["Volume"] = symbol_df["Volume"].astype("Int64")
                                # remove rows that Open  High  Low  Close  Volume are all NaN
                                symbol_df.dropna(subset=["Open", "High", "Low", "Close", "Volume"], how="all", inplace=True)
                                symbol_df.dropna(subset=["Date"], inplace=True)  # Drop rows where Date is NaN
                                if symbol_df.empty:
                                    continue
                                results.extend(symbol_df.to_dict(orient="records"))
                    
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    if attempt == config['max_retries'] - 1:
                        # All retries failed
                        pass
                    time.sleep(config['retry_delay'])
        if not results:
            print("⚠️ Warning: results is EMPTY after processing!")
        return results


class StockDataProcessor:
    """Processes and updates stock data"""
    def __init__(self, 
                 spark_manager: SparkManager,
                 data_store: DataStore,
                 fetcher: StockDataFetcher,
                 logger: Logger,
                 config: ProcessingConfig,
                 processing_schema: DataSchema):
        """
        Initializes the stock data processor.

        Args:
            spark_manager (SparkManager): Manages Spark session.
            data_store (DataStore): Handles data storage and retrieval.
            fetcher (StockDataFetcher): Fetches stock data from external sources.
            logger (Logger): Logger instance for logging events.
            config (ProcessingConfig): Configuration settings for processing.
        """
        self.spark = spark_manager
        self.data_store = data_store
        self.fetcher = fetcher
        self.logger = logger
        self.config = config
        self.processing_schema = processing_schema

    def process_symbol_batch(self, symbol_batch: List[Tuple[str, bool]]):
        """Process symbols in batches while keeping distributed execution"""
        
        config_dict = dict(
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
            batch_size=50  # Recommended batch size for YF API
        )
        config_broadcast = self.spark.session.sparkContext.broadcast(config_dict)
        
        # Create dataframe with symbols and join with last_update
        symbol_df = self.spark.session.createDataFrame(symbol_batch, ["symbol", "is_etf"])
        last_update_df = self.data_store.dfs["last_update"]
        symbol_with_last_update_df = symbol_df.join(last_update_df, "symbol", "left_outer")
        
        # Convert to RDD of symbol tuples
        symbol_rdd = symbol_with_last_update_df.rdd.map(
            lambda row: (row["symbol"], row["is_etf"], row["last_update"])
        )
        
        # Group into smaller batches within each partition
        def process_partition(partition_iter):
            partition_items = list(partition_iter)
            batch_size = 50  # Yahoo Finance API works well with this batch size
            results = []
            
            # Process in batches within this partition
            for i in range(0, len(partition_items), batch_size):
                batch = partition_items[i:i + batch_size]
                batch_results = StockDataFetcher.fetch_stock_data_batch(batch, config_broadcast.value)
                if batch_results:
                    results.extend(batch_results)
            return results
        
        # Process each partition in batches
        stock_data_rdd = symbol_rdd.mapPartitions(process_partition)

        if stock_data_rdd.isEmpty():
            self.logger.info("No new stock data to process.")
            return
        # print(stock_data_rdd.take(5))  # Show first 5 records to check structure

        # Convert Date column to DateType explicitly
        # stock_data_rdd = stock_data_rdd.filter(lambda x: x["Date"] is not None and isinstance(x["Date"], date))

        spark_df = self.spark.session.createDataFrame(
            stock_data_rdd, schema=self.processing_schema.stock_prices_processing
        )
        # print(spark_df.show(5))
        # print(spark_df.schema)
        self._update_dataframes(spark_df)


    def _update_dataframes(self, spark_df: Any):
        """Updates dataframes with new stock data.

        Args:
            spark_df (Any): Spark DataFrame containing new stock data.
        """
        price_data = self._process_price_data(spark_df)
        last_update_data = self._process_last_update_data(spark_df)
        self._write_updates(price_data, last_update_data)

    def _process_price_data(self, spark_df: Any) -> Any:
        """Processes price data for stock updates.

        Args:
            spark_df (Any): Spark DataFrame containing stock data.

        Returns:
            Any: Processed Spark DataFrame with price data.
        """
        # spark_df.printSchema()
        # spark_df.show(5)
        return spark_df.select(
            F.upper(F.col("symbol")).alias("symbol"),
            F.col("Date").alias("trade_date"),
            F.col("Open").alias("open"),
            F.col("High").alias("high"),
            F.col("Low").alias("low"),
            F.col("Close").alias("close"),
            F.col("Volume").alias("volume")
            ).join(self.data_store.dfs["stock_prices"], ["symbol", "trade_date"], "leftanti")

    def _process_last_update_data(self, spark_df: Any) -> Any:
        """Processes last update information for stocks.

        Args:
            spark_df (Any): Spark DataFrame containing stock data.

        Returns:
            Any: Spark DataFrame with last update details.
        """
        return spark_df.select(
            F.upper(F.col("symbol")).alias("symbol"),
            F.lit(date.today()).alias("last_update")
        ).distinct()

    def _write_updates(self, price_data: Any, last_update_data: Any):
        """Writes updated stock data to storage.

        Args:
            price_data (Any): Spark DataFrame containing price updates.
            last_update_data (Any): Spark DataFrame with last update records.
        """
        # print(price_data.show(5))
        # print(price_data.schema)
        if price_data.isEmpty():
            self.logger.info("No new price data to write")
            return
        # Write price data first as it's always an append operation
        self.data_store.write_dataframe(price_data, 'stock_prices')

        # Get reference to last_update table
        last_update_table = self.data_store.dfs["last_update"]
        
        # If the last_update table is empty (first time), write all symbols
        if last_update_table.isEmpty():
            self.data_store.write_dataframe(last_update_data, 'last_update')
        else:
            # Split last_update_data into new and existing symbols
            new_symbols_df = last_update_data.join(last_update_table, "symbol", "leftanti")
            existing_symbols_df = last_update_data.join(last_update_table, "symbol", "semi")
            
            # Write new symbols directly
            if not new_symbols_df.isEmpty():
                self.data_store.write_dataframe(new_symbols_df, 'last_update')
                
            # Accumulate existing symbols for merge operation 
            if not existing_symbols_df.isEmpty():
                self.data_store.accumulate_last_update(existing_symbols_df)

class StockDataManager:
    """Main class that orchestrates the stock data operations"""
    def __init__(self, spark_config: Optional[SparkConfig] = None):
        self.process_config = ProcessingConfig()
        self.spark_config = spark_config if spark_config else SparkConfig()  # Use given config or default
        self.logger = Logger(self.process_config)
        self.spark_manager = SparkManager(self.spark_config)
        self.schema = DataSchema()
        self.data_store = DataStore(self.spark_manager, self.schema, self.process_config)
        self.fetcher = StockDataFetcher(self.process_config, self.logger)
        self.processor = StockDataProcessor(
            self.spark_manager,
            self.data_store,
            self.fetcher,
            self.logger,
            self.process_config,
            self.schema
        )
        
        # Initialize dataframes
        self.data_store.load_all_dataframes()

    def download_all_stock_data(self):
        """Download all stock data using Spark's native parallelism"""
        try:
            url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
            df = pd.read_csv(url, sep="|")
            df = df[df["Test Issue"] == "N"]
            last_update_df = self.data_store.dfs["last_update"]

            outdated_symbols = last_update_df.filter(F.col("last_update") < F.lit(date.today())).select("symbol").distinct()
            outdated_symbols_list = [row["symbol"] for row in outdated_symbols.collect()]
            symbols_in_db = [row["symbol"] for row in last_update_df.select("symbol").distinct().collect()]

            df = df[df["NASDAQ Symbol"].isin(outdated_symbols_list) | ~df["NASDAQ Symbol"].isin(symbols_in_db)]

            # df = df.head(5)
            
            symbols = (
                [(symbol, 'N') for symbol in df[df["ETF"] == "N"]["NASDAQ Symbol"]]
            )
            

            self.logger.info(f"Found {len(symbols)} total symbols to process")
            
            for i in range(0, len(symbols), self.process_config.batch_size):
                batch = symbols[i:i + self.process_config.batch_size]
                self.processor.process_symbol_batch(batch)
                self.logger.info(
                    f"Processed batch {i//self.process_config.batch_size + 1} of "
                    f"{(len(symbols) + self.process_config.batch_size - 1)//self.process_config.batch_size}"
                )
            
            self.data_store.merge_last_update()

            self.logger.info("All stock data updated successfully")
            
        except Exception as e:
            self.logger.error(f"Error in download_all_stock_data: {str(e)}")
            raise

    def cleanup(self):
        """Cleanup all resources"""
        self.spark_manager.cleanup()


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
    if not get_calendar("NASDAQ").valid_days(start_date=date.today(), end_date=date.today()):
        print("Today's not trading day, skipping stock data update.")
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