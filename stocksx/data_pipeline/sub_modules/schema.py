import os
import sys

from stocksx.data_pipeline.sub_modules.logger import Logger
from stocksx.configs.processing_config import ProcessingConfig
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, LongType, BooleanType


class DataSchema:
    """Defines schema for stock price and last update data."""
    
    def __init__(self):
        """Initialize DataSchema class."""
        self.logger = Logger(ProcessingConfig())
    
    def get_schema(self, table_name: str) -> StructType:
        """Returns schema for the specified table.
        
        Args:
            table_name (str): Name of the table for which schema is requested.
            
        Returns:
            StructType: Schema containing fields for the requested table.
            
        Raises:
            ValueError: If the table name is not recognized.
        """
        # Dictionary mapping table names to their schema methods
        schema_methods = {
            "stock_prices": self._stock_prices_schema,
            "last_update": self._last_update_schema,
            "stock_prices_processing": self._stock_prices_processing_schema,
        }
        
        if table_name not in schema_methods:
            error_msg = f"No schema defined for table: {table_name}"
            self.logger.error(error_msg)
            print(f"ERROR: {error_msg}")
            raise ValueError(error_msg)
            
        return schema_methods[table_name]()
    
    def _stock_prices_schema(self) -> StructType:
        """Returns schema for stock price data.
        
        Returns:
            StructType: Schema containing fields for stock prices.
        """
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("trade_date", DateType(), False),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", LongType(), True)
        ])

    def _last_update_schema(self) -> StructType:
        """Returns schema for last update tracking.
        
        Returns:
            StructType: Schema containing fields for tracking last updates.
        """
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("last_update", DateType(), False)
        ])
    
    def _stock_prices_processing_schema(self) -> StructType:
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
    