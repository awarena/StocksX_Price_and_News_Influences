# Init spark session to read data from parquet files
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType
from pyspark.sql import functions as F
from stocksx.configs.spark_config import SparkConfig
from stocksx.data_pipeline.sub_modules.spark_manager import SparkManager
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import time
import random
def main():
    spark_config = SparkConfig(iceberg_enabled=True, iceberg_namespace = "raw_data", 
                            iceberg_warehouse="data/warehouse/iceberg")
    spark_manager = SparkManager(spark_config)
    spark_manager.verify_configuration()
    spark = spark_manager.session

    i = 1

    while i !=0:
        
        # wait 20 minutes before next iteration
        if i!= 1:
            time.sleep(20 * (60 + random.randint(-10, 10)))

        # Load data from the local Iceberg warehouse

        metadata = pd.read_csv("../data/metadata/stock_updates_metadata/metadata.csv")
        missing_sector = metadata[metadata["sector"].isnull() | metadata["sector"].eq("")]["symbol"].tolist()
        from stocksx.utils.sector_fetcher import fetch_sectors_spark

        print(f"Fetching sectors for {len(missing_sector)} missing symbols...")
        
        sector_pd = fetch_sectors_spark(spark_manager.session, missing_sector)
        # remove null and empty sectors
        sector_pd = sector_pd[~sector_pd["sector"].isnull() & ~sector_pd["sector"].eq("")]

        i = sector_pd.shape[0]
        print(f"Found {i} missing sectors, updating metadata...")

        metadata = metadata.merge(
            sector_pd, on='symbol', how='left', suffixes=('', '_new')
        )
        metadata['sector'] = metadata['sector_new'].combine_first(metadata['sector'])
        metadata = metadata.drop(columns=['sector_new'])
        # save metadata with sectors
        metadata.to_csv("../data/metadata/stock_updates_metadata/metadata.csv", index=False)

if __name__ == "__main__":
    main()