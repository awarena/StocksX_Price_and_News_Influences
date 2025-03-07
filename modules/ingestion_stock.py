import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import yfinance as yf
from datetime import date
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType, LongType
from pyspark import SparkContext as sc
import logging
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from pandas_market_calendars import get_calendar
import argparse
from configs.processing_config import ProcessingConfig
from configs.spark_config import SparkConfig
from modules.sub_modules.spark_manager import SparkManager
from modules.sub_modules.logger import Logger

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
