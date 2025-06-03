import os
import pandas as pd
import yfinance as yf
from datetime import date
import time
from pyspark.sql import functions as F
from typing import Optional, Dict, List, Tuple, Any

from stocksx.configs.processing_config import ProcessingConfig
from stocksx.configs.spark_config import SparkConfig
from stocksx.data_pipeline.sub_modules.spark_manager import SparkManager
from stocksx.utils.logger import Logger
from stocksx.data_pipeline.sub_modules.schema import DataSchema
from stocksx.data_pipeline.sub_modules.iceberg_manager import IcebergManager
  
class DataStore:
    """Manages data storage and retrieval operations"""
    def __init__(self, spark_manager: SparkManager, schema: DataSchema, config: ProcessingConfig, tables: List[str]):
        """Initialize IcebergDataStore.
        
        Args:
            spark_manager: SparkManager instance
            schema: DataSchema instance
            config: ProcessingConfig instance
            tables: List of tables to initialize
        """
        self.spark = spark_manager
        self.schema = schema
        self.config = config
        self.tables = tables
        self.logger = Logger(config)
        
        # Create Iceberg manager
        self.iceberg = IcebergManager(spark_manager, schema, config)
        
        # Store DataFrames and metadata
        self.dfs: Dict[str, Any] = {}
        self.metadata: Dict[str, pd.DataFrame] = {}
        
        # Ensure metadata directory exists
        if not os.path.exists(self.config.metadata_path):
            os.makedirs(self.config.metadata_path)
        
        # Initialize tables
        self.iceberg.initialize_tables(self.tables)

    def load_all_dataframes(self):
        """Load all data into memory."""
        # Load Iceberg tables
        self.dfs["stock_prices"] = self.iceberg.load_table("stock_prices")
        
        # Load metadata as before
        self.metadata["metadata"] = self.load_metadata("metadata")
    
        
    def load_metadata(self, name: str) -> pd.DataFrame:
        """Loads metadata from CSV file directly into pandas DataFrame."""
        csv_path = f"{self.config.metadata_path}{name}.csv"
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            # Convert date columns if present
            if 'last_update' in df.columns:
                df["last_update"] = pd.to_datetime(df["last_update"]).dt.date
            return df
        else:
            # Return empty DataFrame with appropriate columns
            if name == "metadata":
                return pd.DataFrame(columns=["symbol", "last_update", "sector"])
            else:
                return pd.DataFrame()
    
    def get_spark_metadata(self, name: str) -> Any:
        """Converts pandas metadata to Spark DataFrame for compatibility."""
        if name not in self.metadata or self.metadata[name].empty:
            return self.spark.session.createDataFrame([], getattr(self.schema, name))
        return self.spark.session.createDataFrame(self.metadata[name])
    
    def update_metadata(self, pandas_data: pd.DataFrame, name: str):
        """Updates metadata by combining with new data."""
        if pandas_data.empty:
            return
            
        if name == "metadata":
            # Combine existing and new data
            if self.metadata[name].empty:
                combined_df = pandas_data
            else:
                combined_df = pd.concat([self.metadata[name], pandas_data])
            
            # Get most recent update date per symbol
            updated_df = combined_df.groupby("symbol")["last_update"].max().reset_index()
        else:
            updated_df = pandas_data
            
        # Save to metadata CSV
        csv_path = f"{self.config.metadata_path}{name}.csv"
        updated_df.to_csv(csv_path, index=False)
        
        # Update in-memory dataframes
        self.metadata[name] = updated_df

    def write_dataframe(self, df: Any, name: str):
            """Write a DataFrame to storage.
            
            Args:
                df: DataFrame to write
                name: Target name
            """   
            if df is None:
                self.logger.warning(f"DataFrame is None, skipping write to {name}")
                return       
            try:
                # Write to appropriate storage
                if name is not None:
                    # Use Iceberg manager to write to Iceberg table
                    success = self.iceberg.write_to_table(df, name)
                    if success:
                        # Refresh the cached DataFrame
                        self.dfs[name] = self.iceberg.load_table(name)
                        self.logger.info(f"Successfully wrote data to {name}")
                    else:
                        self.logger.error(f"Failed to write data to {name}")
                else:
                    self.logger.warning(f"Write method not implemented for {name}")
            except Exception as e:
                self.logger.error(f"Error writing DataFrame to {name}: {str(e)}")

    def delete_table(self, name: str):
        """Delete an Iceberg table.
        
        Args:
            name: Name of the table to delete
        """
        try:
            self.iceberg.delete_table(name)
        except Exception as e:
            self.logger.error(f"Error deleting table {name}: {str(e)}")
        
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
        processing_config = ProcessingConfig()
        logger = Logger(processing_config)
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
            print("Warning: results is EMPTY after processing!")
            logger.info("Warning: results is EMPTY after processing!")
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
        
        # Create dataframe with symbols and join with metadata
        symbol_pd = pd.DataFrame(symbol_batch, columns=["symbol", "is_etf"])

        # Join with pandas
        symbol_with_metadata_pd = pd.merge(
            symbol_pd, 
            self.data_store.metadata["metadata"], 
            on="symbol", 
            how="left"
        )

        symbol_with_last_update_df = self.spark.session.createDataFrame(symbol_with_metadata_pd).cache()
        
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
        # if stock_data_rdd.isEmpty():
        #     self.logger.info("No new stock data to process.")
        #     return

        # print(stock_data_rdd.take(5))  # Show first 5 records to check structure

        # Convert Date column to DateType explicitly
        # stock_data_rdd = stock_data_rdd.filter(lambda x: x["Date"] is not None and isinstance(x["Date"], date))

        spark_df = self.spark.session.createDataFrame(
            stock_data_rdd, schema=self.processing_schema.get_schema("stock_prices_processing")
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
        metadata = self._process_metadata_data(spark_df)
        self._write_updates(price_data, metadata)

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

    def _process_metadata_data(self, spark_df: Any) -> pd.DataFrame:
        """Processes metadata information for stocks.

        Args:
            spark_df (Any): Spark DataFrame containing stock data.

        Returns:
            pd.DataFrame: Pandas DataFrame with last update details.
        """
        symbols = [row.symbol.upper() for row in spark_df.select("symbol").distinct().collect()]
        # Get current metadata as a dict for fast lookup
        existing_metadata = self.data_store.metadata["metadata"].set_index("symbol")["sector"].to_dict()
        return pd.DataFrame({
            "symbol": symbols,
            "last_update": [date.today()] * len(symbols),
            "sector": [existing_metadata.get(sym, None) for sym in symbols]
        })

    def _write_updates(self, price_data: Any, metadata: Any):
        """Writes updated stock data to storage.

        Args:
            price_data (Any): Spark DataFrame containing price updates.
            last_update_data (Any): Spark DataFrame with last update records.
        """
            
        # Write price data
        self.data_store.write_dataframe(price_data, 'stock_prices')

        # Update last_update metadata directly - no need for batching or accumulation
        if not metadata.empty:
            self.data_store.update_metadata(metadata, 'metadata')

class StockDataManager:
    """Main class that orchestrates the stock data operations"""
    def __init__(self, spark_config: Optional[SparkConfig] = None):
        self.process_config = ProcessingConfig()
        self.spark_config = spark_config if spark_config else SparkConfig()  # Use given config or default
        self.logger = Logger(self.process_config)
        self.spark_manager = SparkManager(self.spark_config)
        self.schema = DataSchema()
        self.data_store = DataStore(self.spark_manager, self.schema, self.process_config, ["stock_prices"])
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

        # Verify configuration
        self.spark_manager.verify_configuration()

    def download_all_stock_data(self):
        """Download all stock data using Spark's native parallelism"""
        try:
            url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
            df = pd.read_csv(url, sep="|")
            df = df[df["Test Issue"] == "N"]
            metadata_pd = self.data_store.metadata["metadata"]

            if not metadata_pd.empty:
                        # Get outdated symbols (those with last_update < today)
                        outdated_symbols = metadata_pd[metadata_pd["last_update"] < date.today()]
                        outdated_symbols_list = outdated_symbols["symbol"].tolist()
                        
                        # Get all symbols in database
                        symbols_in_db = metadata_pd["symbol"].tolist()
                        
                        # Filter NASDAQ symbols
                        df = df[df["NASDAQ Symbol"].isin(outdated_symbols_list) | 
                                ~df["NASDAQ Symbol"].isin(symbols_in_db)]
        

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
            
            self.logger.info("All stock data updated successfully")
            
        except Exception as e:
            self.logger.error(f"Error in download_all_stock_data: {str(e)}")
            raise

    def cleanup(self):
        """Cleanup all resources"""
        self.spark_manager.cleanup()
