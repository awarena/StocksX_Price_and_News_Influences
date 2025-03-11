# test_iceberg.py
import os
import sys
import pandas as pd
from datetime import date, timedelta
from pyspark.sql import functions as F

from stocksx.configs.spark_config import SparkConfig
from stocksx.data_pipeline.sub_modules.spark_manager import SparkManager
from stocksx.data_pipeline.sub_modules.schema import DataSchema
from stocksx.data_pipeline.ingestion_stock_iceberg import DataStore
from stocksx.configs.processing_config import ProcessingConfig

os.environ["PYSPARK_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"

def main():
    """Test Iceberg integration"""
    print("Testing Iceberg integration...")
    
    # Create config with Iceberg enabled
    spark_config = SparkConfig(
        app_name="StocksX_Iceberg_Test",
        iceberg_enabled=True,
        iceberg_warehouse="e:/Projects/StocksX_Price_and_News_Influences/iceberg_warehouse"
    )
    
    # Initialize components
    spark_manager = SparkManager(spark_config)
    print(f"Spark version: {spark_manager.session.version}")
    schema = DataSchema()
    config = ProcessingConfig()
    
    # Create DataStore with Iceberg
    data_store = DataStore(
        spark_manager, 
        schema, 
        config, 
        tables=["stock_prices"]
    )
    
    try:
        # Load dataframes
        data_store.load_all_dataframes()
        
        # Create test data
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        # Sample price data
        sample_data = [
            {
                "symbol": "AAPL", 
                "trade_date": yesterday, 
                "open": 150.0, 
                "high": 155.0, 
                "low": 149.0, 
                "close": 153.0, 
                "volume": 1000000
            },
            {
                "symbol": "MSFT", 
                "trade_date": yesterday, 
                "open": 250.0, 
                "high": 255.0, 
                "low": 248.0, 
                "close": 253.0, 
                "volume": 800000
            }
        ]
        
        # Create DataFrame
        df = spark_manager.session.createDataFrame(sample_data)
        
        # Write to Iceberg
        print("Writing test data...")
        data_store.write_dataframe(df, "stock_prices")
        
        # Read back
        print("\nReading data back:")
        result = data_store.dfs["stock_prices"].filter(
            F.col("trade_date") == F.lit(yesterday)
        )
        result.show()
        
        # update data
        print("\nUpdating data...")
        sample_data[0]["close"] = 155.0
        df = spark_manager.session.createDataFrame(sample_data)
        data_store.write_dataframe(df, "stock_prices")

        # Read updated data
        print("\nReading updated data:")
        result = data_store.dfs["stock_prices"].filter(
            F.col("trade_date") == F.lit(yesterday)
        )
        result.show()

        # Delete table
        print("\nDeleting table...")
        data_store.delete_table("stock_prices")

        print("\nIceberg test completed successfully!")
        
    except Exception as e:
        print(f"Error in Iceberg test: {str(e)}")
        raise
    finally:
        spark_manager.cleanup()

if __name__ == "__main__":
    main()