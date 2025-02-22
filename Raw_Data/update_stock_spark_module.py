from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import os
import pandas as pd
import yfinance as yf
import datetime
from datetime import date
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from yfinance.exceptions import YFInvalidPeriodError

os.environ["PYSPARK_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
@dataclass
class ProcessingConfig:
    """Configuration settings for data processing"""
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: int = 5
    data_path: str = "Raw_Data/parquets/"
    log_path: str = "Raw_Data/logs/update_log.txt"
    log_max_bytes: int = 10 * 1024 * 1024
    log_backup_count: int = 3
    parallelism: int = 200

@dataclass
class SparkConfig:
    """Configuration settings for Spark"""
    app_name: str = "StocksX_Price_and_News_Influences"
    arrow_enabled: bool = True
    shuffle_partitions: int = 200
    parallelism: int = 200
    executor_memory: str = "10g"
    driver_memory: str = "10g"
    garbage_collectors: Dict[str, str] = field(default_factory=lambda: {
        "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
        "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors": "G1 Old Generation"
    })
class SizeAndTimeRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='midnight', interval=1, backupCount=0, encoding=None, delay=False, utc=False, maxBytes=0):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc)
        self.maxBytes = maxBytes

    def shouldRollover(self, record):
        if self.stream is None:  # delay was set...
            self.stream = self._open()
        if self.maxBytes > 0:  # are we rolling over?
            self.stream.seek(0, 2)  # due to non-posix-compliant Windows feature
            if self.stream.tell() >= self.maxBytes:
                return 1
        return super().shouldRollover(record)
    
class Logger:
    """Handles logging operations"""
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create a custom rotating file handler
        file_handler = SizeAndTimeRotatingFileHandler(
            config.log_path, 
            when="midnight", 
            interval=1, 
            backupCount=config.log_backup_count,
            maxBytes=config.log_max_bytes
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Create a stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def info(self, message: str):
        self.logger.info(message)

    def error(self, message: str):
        self.logger.error(message)

    def __reduce__(self):
        return (self.__class__, (self.config,))

class SparkManager:
    """Manages Spark session and configurations"""
    def __init__(self, config: SparkConfig):
        self.config = config
        self._session = None

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            builder = (SparkSession.builder
                .appName(self.config.app_name)
                .config("spark.sql.execution.arrow.pyspark.enabled", self.config.arrow_enabled)
                .config("spark.sql.shuffle.partitions", self.config.shuffle_partitions)
                .config("spark.default.parallelism", self.config.parallelism)
                .config("spark.executor.memory", self.config.executor_memory)
                .config("spark.driver.memory", self.config.driver_memory))               
            for key, value in self.config.garbage_collectors.items():
                builder = builder.config(key, value)
            
            self._session = builder.getOrCreate()
        return self._session

    def cleanup(self):
        if self._session:
            self._session.stop()
            self._session = None

class DataSchema:
    """Manages schema definitions for different data types"""
    
    @property
    def stock_prices(self) -> StructType:
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("trade_date", DateType(), False),
            StructField("last_update", DateType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", IntegerType(), True)
        ])

    @property
    def last_update(self) -> StructType:
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("last_update", DateType(), False)
        ])
    

class DataStore:
    """Manages data storage and retrieval operations"""
    def __init__(self, spark_manager: SparkManager, schema: DataSchema, config: ProcessingConfig):
        self.spark = spark_manager
        self.schema = schema
        self.config = config
        self.dfs: Dict[str, Any] = {}
        self.accumulated_last_update: Optional[Any] = None


    def load_parquet(self, name: str) -> Any:
        path = f"{self.config.data_path}{name}.parquet"
        schema = getattr(self.schema, name)
        if os.path.exists(path):
            return self.spark.session.read.parquet(path)
        return self.spark.session.createDataFrame([], schema)

    def load_all_dataframes(self):
        self.dfs["stock_prices"] = self.load_parquet("stock_prices")
        self.dfs["last_update"] = self.load_parquet("last_update")

    def write_dataframe(self, df: Any, name: str):
        df.write.mode("append").partitionBy("symbol").parquet(f"{self.config.data_path}{name}.parquet")

    def accumulate_last_update(self, new_data: Any):
        """Accumulate new data for the last_update table"""
        if self.accumulated_last_update is None:
            self.accumulated_last_update = new_data
        else:
            self.accumulated_last_update = self.accumulated_last_update.union(new_data)

    def merge_last_update(self):
        """Merge accumulated data into the last_update table"""
        if self.accumulated_last_update is None:
            return

        last_update_df = self.dfs["last_update"]
        
        # Join new data with existing last_update data to find records to update
        updated_df = self.accumulated_last_update.join(last_update_df, "symbol", "left_outer") \
                             .select(
                                 F.coalesce(self.accumulated_last_update["symbol"], last_update_df["symbol"]).alias("symbol"),
                                 F.coalesce(self.accumulated_last_update["last_update"], last_update_df["last_update"]).alias("last_update")
                             )
        
        # Union the updated records with the new records
        merged_df = updated_df.union(self.accumulated_last_update.select("symbol", "last_update"))
        
        # Remove duplicates by keeping the latest last_update for each symbol
        window_spec = Window.partitionBy("symbol").orderBy(F.desc("last_update"))
        deduped_df = merged_df.withColumn("rank", F.row_number().over(window_spec)) \
                              .filter(F.col("rank") == 1) \
                              .drop("rank")
        
        # Write the deduplicated DataFrame to the last_update table
        deduped_df.write.mode("overwrite").partitionBy("symbol").parquet(f"{self.config.data_path}last_update.parquet")
        
        # Reload the last_update DataFrame
        self.dfs["last_update"] = self.load_parquet("last_update")
        self.accumulated_last_update = None
        
class StockDataFetcher:
    """Handles stock data fetching operations"""
    def __init__(self, config: ProcessingConfig, logger: Logger):
        self.config = config
        self.logger = logger

    def fetch_stock_data(self, symbol_info: Tuple[str, str], last_update: Optional[date] = None) -> Optional[pd.DataFrame]:
        symbol, is_etf, last_update = symbol_info
        try:
            period = "max" if last_update is None else f"{(date.today() - last_update).days}d"
            
            for attempt in range(self.config.max_retries):
                try:
                    df = yf.download(symbol, period=period, interval="1d", progress=False, auto_adjust=True)
                    if df.empty:
                        df = yf.download(symbol, period="1d", interval="1d", progress=False, auto_adjust=True)
                        if df.empty:
                            return None
                    
                    df = df.reset_index()
                    df['symbol'] = symbol
                    df['is_etf'] = is_etf

                    # Flatten the columns if they are MultiIndex
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = [col[0] for col in df.columns]

                    return df
                except Exception as e:
                    if attempt == self.config.max_retries - 1:
                        self.logger.error(f"Failed to fetch {symbol} after {self.config.max_retries} attempts: {str(e)}")
                        return None
                    time.sleep(self.config.retry_delay)
        except Exception as e:
            self.logger.error(f"Error processing {symbol}: {str(e)}")
            return None

    def __reduce__(self):
        return (self.__class__, (self.config, self.logger))
class StockDataProcessor:
    """Processes and updates stock data"""
    def __init__(self, 
                 spark_manager: SparkManager,
                 data_store: DataStore,
                 fetcher: StockDataFetcher,
                 logger: Logger,
                 config: ProcessingConfig):
        self.spark = spark_manager
        self.data_store = data_store
        self.fetcher = fetcher
        self.logger = logger
        self.config = config

    def process_symbol_batch(self, symbol_batch: List[Tuple[str, bool]]):
        """Process a batch of symbols using Spark's native parallelism"""
        symbol_df = self.spark.session.createDataFrame(symbol_batch, ["symbol", "is_etf"])
        last_update_df = self.data_store.dfs["last_update"]
        
        # Join symbol_df with last_update_df to get the last_update date for each symbol
        symbol_with_last_update_df = symbol_df.join(last_update_df, "symbol", "left_outer")

        symbol_rdd = symbol_with_last_update_df.rdd.map(lambda row: (row["symbol"], row["is_etf"], row["last_update"]))

        all_data = []
        for data in symbol_rdd.map(self.fetcher.fetch_stock_data).collect():
            if data is not None:
                all_data.append(data)
        
        if not all_data:
            return
        
        combined_df = pd.concat(all_data, ignore_index=True)
        spark_df = self.spark.session.createDataFrame(combined_df)
        
        self._update_dataframes(spark_df)

    def _update_dataframes(self, spark_df: Any):
        """Update all dataframes with new data"""
        price_data = self._process_price_data(spark_df)

        last_update_data = self._process_last_update_data(spark_df)

        self._write_updates(price_data, last_update_data)

    def _process_price_data(self, spark_df: Any) -> Any:
        return spark_df.select(
            F.col("symbol"),
            F.col("Date").alias("trade_date"),
            F.col("Open").alias("open"),
            F.col("High").alias("high"),
            F.col("Low").alias("low"),
            F.col("Close").alias("close"),
            F.col("Volume").alias("volume")
            ).join(self.data_store.dfs["stock_prices"], ["symbol", "trade_date"], "leftanti")

    def _process_last_update_data(self, spark_df: Any) -> Any:
        return spark_df.select(
            F.col("symbol"),
            F.lit(date.today()).alias("last_update")
        ).distinct()

    def _write_updates(self, price_data: Any, last_update_data: Any):
        """Write updates to storage if there are any changes"""
        if price_data.count() > 0:
            self.data_store.write_dataframe(price_data, 'stock_prices')
        
        if last_update_data.count() > 0:
            self.data_store.accumulate_last_update(last_update_data)

class StockDataManager:
    """Main class that orchestrates the stock data operations"""
    def __init__(self):
        self.process_config = ProcessingConfig()
        self.spark_config = SparkConfig()
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
            self.process_config
        )
        
        # Initialize dataframes
        self.data_store.load_all_dataframes()

    def download_all_stock_data(self):
        """Download all stock data using Spark's native parallelism"""
        try:
            url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
            df = pd.read_csv(url, sep="|")
            df = df[df["Test Issue"] == "N"]

            # Take only first 10 symbols for testing
            # df = df.head(10)
            
            symbols = (
                [(symbol, 'N') for symbol in df[df["ETF"] == "N"]["NASDAQ Symbol"]] +
                [(symbol, 'Y') for symbol in df[df["ETF"] == "Y"]["NASDAQ Symbol"]]
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

def main():
    manager = StockDataManager()
    try:
        manager.download_all_stock_data()
    finally:
        manager.cleanup()

if __name__ == "__main__":
    main()