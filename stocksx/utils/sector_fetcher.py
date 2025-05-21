import yfinance as yf
import pandas as pd

def fetch_sector_partition(rows):
    results = []
    for row in rows:
        symbol = row.symbol  # Extract symbol from Row
        try:
            info = yf.Ticker(symbol).info
            sector = info.get("sector", None)
            results.append((symbol, sector))
        except Exception:
            results.append((symbol, None))
    return results

def fetch_sectors_spark(spark, symbol_list, num_partitions=100):
    # Create Spark DataFrame
    symbol_df = spark.createDataFrame(pd.DataFrame({"symbol": symbol_list}))
    # Repartition for parallelism
    symbol_df = symbol_df.repartition(num_partitions)
    # Convert to RDD and fetch sectors in parallel
    sector_rdd = symbol_df.rdd.mapPartitions(fetch_sector_partition)    
    # Convert back to DataFrame
    sector_pd = pd.DataFrame(sector_rdd.collect(), columns=["symbol", "sector"])
    return sector_pd