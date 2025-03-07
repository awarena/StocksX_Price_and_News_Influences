import os
import pandas as pd
import yfinance as yf
import datetime
import time
import pandas_market_calendars as mcal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType

os.environ["PYSPARK_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StocksX_Price_and_News_Influences") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

# Paths to Parquet files
DATA_PATH = "Raw_Data/parquets/"
LOG_PATH = "Raw_Data/logs/update_log.txt"

def log_message(message):
    with open(LOG_PATH, "a") as log_file:
        log_file.write(f"{datetime.datetime.now()} - {message}\n")
    print(message)

def load_parquet(path, schema):
    if os.path.exists(path):
        return spark.read.parquet(path)
    return spark.createDataFrame([], schema)

# Define schemas
trading_days_schema = StructType([
    StructField("date_id", IntegerType(), False),
    StructField("trade_date", DateType(), False)
])

symbols_schema = StructType([
    StructField("symbol_id", IntegerType(), False),
    StructField("symbol", StringType(), False),
    StructField("is_etf", StringType(), False),
    StructField("last_update", DateType(), True)
])

stock_prices_schema = StructType([
    StructField("symbol_id", IntegerType(), False),
    StructField("date_id", IntegerType(), False),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", IntegerType(), True)
])

# Load DataFrames
trading_days_df = load_parquet(DATA_PATH + "trading_days.parquet", trading_days_schema)
symbols_df = load_parquet(DATA_PATH + "symbols.parquet", symbols_schema)
stock_prices_df = load_parquet(DATA_PATH + "stock_prices.parquet", stock_prices_schema)

def fetch_stock_data(symbol, is_etf=False):
    last_update = symbols_df.filter(F.col("symbol") == symbol).select("last_update").collect()
    period = "max" if not last_update else f"{(datetime.date.today() - last_update[0][0]).days}d"
    log_message(f"Fetching {symbol} data for {period}")
    
    df = yf.download(symbol, period=period, interval="1d").reset_index()
    if df.empty:
        log_message(f"No new data for {symbol}")
        return None
    
    df = df.rename(columns={"Date": "trade_date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    spark_df = spark.createDataFrame(df)
    # Ensure column names are correctly set
    for col in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(col, col.split(",")[0].strip("()'"))
    
    return spark_df

def update_stock_data(symbol, is_etf):
    global trading_days_df  # Declare trading_days_df as global
    global symbols_df  # Declare symbols_df as global

    new_data_df = fetch_stock_data(symbol, is_etf)
    if new_data_df is None:
        return
    
    # Insert trading days in batch
    new_dates_df = new_data_df.select("trade_date").distinct()
    new_dates_df = new_dates_df.join(trading_days_df, "trade_date", "left_anti")
    new_dates_df = new_dates_df.withColumn("date_id", F.monotonically_increasing_id())
    new_dates_df = new_dates_df.select("date_id", "trade_date")
    trading_days_df = trading_days_df.union(new_dates_df)
    
    # Insert symbols in batch
    new_symbols_df = spark.createDataFrame([(symbol, is_etf, datetime.date.today())], ["symbol", "is_etf", "last_update"])
    new_symbols_df = new_symbols_df.withColumn("last_update", F.col("last_update").cast(DateType()))
    new_symbols_df = new_symbols_df.join(symbols_df, "symbol", "left_anti").withColumn("symbol_id", F.monotonically_increasing_id())
    new_symbols_df = new_symbols_df.withColumn("is_etf", F.col("is_etf").cast(StringType()))
    new_symbols_df = new_symbols_df.select("symbol", "is_etf", "symbol_id", "last_update")  # Ensure correct column order
    symbols_df = symbols_df.union(new_symbols_df)
    
    # Join data with trading days and symbols
    new_data_df = new_data_df.join(trading_days_df, "trade_date", "left")
    new_data_df = new_data_df.withColumn("symbol_id", F.lit(new_symbols_df.select("symbol_id").collect()[0][0]))
    new_data_df = new_data_df.select("symbol_id", "date_id", "open", "high", "low", "close", "volume")
    
    # Write data in batch
    new_data_df.write.mode("append").parquet(DATA_PATH + "stock_prices.parquet")
    new_dates_df.write.mode("append").parquet(DATA_PATH + "trading_days.parquet")
    new_symbols_df.write.mode("append").parquet(DATA_PATH + "symbols.parquet")
    log_message(f"Updated {symbol} successfully.")

def download_all_stock_data():
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    df = pd.read_csv(url, sep="|")
    df = df[df["Test Issue"] == "N"]
    
    stocks = df[df["ETF"] == "N"]["NASDAQ Symbol"].tolist()
    etfs = df[df["ETF"] == "Y"]["NASDAQ Symbol"].tolist()
    
    log_message(f"Found {len(stocks)} stocks and {len(etfs)} ETFs.")
    for symbol in stocks:
        update_stock_data(symbol, False)
    for symbol in etfs:
        update_stock_data(symbol, True)
    log_message("All stock data updated successfully.")

def main():
    download_all_stock_data()
    spark.stop()
    log_message("Stock data update complete.")

if __name__ == "__main__":
    main()
