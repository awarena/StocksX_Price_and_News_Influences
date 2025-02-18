import os
import pandas as pd
import yfinance as yf
import datetime
import pandas_ta as ta
import time
import pandas_market_calendars as mcal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import DateType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StocksX_Price_and_News_Influences") \
    .getOrCreate()

# Paths to Parquet files
TRADING_DAYS_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/parquets/trading_days.parquet"
SYMBOLS_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/parquets/symbols.parquet"
STOCK_PRICES_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/parquets/stock_prices.parquet"
ETF_PRICES_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/parquets/etf_prices.parquet"
METADATA_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/parquets/metadata.parquet"
LOG_PATH = "E:/Projects/AutoRunStockUpdater/logs/update_log.txt"

def log_message(message):
    # Log rotation
    if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > 1e6:
        os.rename(LOG_PATH, f"{os.path.splitext(LOG_PATH)[0]}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.txt")
    with open(LOG_PATH, "a") as log_file:
        log_file.write(f"{datetime.datetime.now()} - {message}\n")
    print(message)

# Load Parquet files into DataFrames
def load_parquet_files():
    trading_days_df = spark.read.parquet(TRADING_DAYS_PATH)
    symbols_df = spark.read.parquet(SYMBOLS_PATH)
    stock_prices_df = spark.read.parquet(STOCK_PRICES_PATH)
    etf_prices_df = spark.read.parquet(ETF_PRICES_PATH)
    metadata_df = spark.read.parquet(METADATA_PATH)
    return trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df

# Save DataFrames to Parquet files
def save_parquet_files(new_trading_days_df, new_symbols_df, new_stock_prices_df, new_etf_prices_df, new_metadata_df):
    if new_trading_days_df.count() > 0:
        new_trading_days_df.write.mode("append").parquet(TRADING_DAYS_PATH)
    if new_symbols_df.count() > 0:
        new_symbols_df.write.mode("append").parquet(SYMBOLS_PATH)
    if new_stock_prices_df.count() > 0:
        new_stock_prices_df.write.mode("append").parquet(STOCK_PRICES_PATH)
    if new_etf_prices_df.count() > 0:
        new_etf_prices_df.write.mode("append").parquet(ETF_PRICES_PATH)
    if new_metadata_df.count() > 0:
        new_metadata_df.write.mode("append").parquet(METADATA_PATH)

# Insert a date if not exists
def insert_trading_day(date, trading_days_df):
    date_str = date.strftime("%Y-%m-%d") if isinstance(date, pd.Timestamp) else str(date)
    trading_days = trading_days_df.filter(col("trade_date") == date_str)
    if trading_days.count() == 0:
        new_row = spark.createDataFrame([(None, date_str)], ["date_id", "trade_date"])
        return new_row
    return spark.createDataFrame([], trading_days_df.schema)

# Insert a symbol if not exists
def insert_symbol(symbol, is_etf, symbols_df):
    symbols = symbols_df.filter(col("symbol") == symbol)
    if symbols.count() == 0:
        new_row = spark.createDataFrame([(None, symbol, is_etf, None)], ["symbol_id", "symbol", "is_etf", "last_update"])
        return new_row
    return spark.createDataFrame([], symbols_df.schema)

# Insert stock/ETF data
def insert_stock_data(df, symbol, is_etf, trading_days_df, symbols_df, stock_prices_df, etf_prices_df):
    new_trading_days_df = spark.createDataFrame([], trading_days_df.schema)
    new_symbols_df = spark.createDataFrame([], symbols_df.schema)
    new_stock_prices_df = spark.createDataFrame([], stock_prices_df.schema)
    new_etf_prices_df = spark.createDataFrame([], etf_prices_df.schema)

    new_symbol_row = insert_symbol(symbol, is_etf, symbols_df)
    if new_symbol_row.count() > 0:
        new_symbols_df = new_symbols_df.union(new_symbol_row)
        symbols_df = symbols_df.union(new_symbol_row)
    symbol_id = symbols_df.filter(col("symbol") == symbol).select("symbol_id").collect()[0][0]
    log_message(f"Inserting data for symbol: {symbol} with symbol_id: {symbol_id}")

    for _, row in df.iterrows():
        new_trading_day_row = insert_trading_day(row["Date"], trading_days_df)
        if new_trading_day_row.count() > 0:
            new_trading_days_df = new_trading_days_df.union(new_trading_day_row)
            trading_days_df = trading_days_df.union(new_trading_day_row)
        date_id = trading_days_df.filter(col("trade_date") == row["Date"]).select("date_id").collect()[0][0]
        new_row = spark.createDataFrame([(symbol_id, date_id, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"])],
                                        ["symbol_id", "date_id", "open", "high", "low", "close", "volume"])
        if is_etf:
            new_etf_prices_df = new_etf_prices_df.union(new_row)
        else:
            new_stock_prices_df = new_stock_prices_df.union(new_row)
        log_message(f"Inserted data for symbol_id: {symbol_id}, date_id: {date_id}, date: {row['Date']}")

    return new_trading_days_df, new_symbols_df, new_stock_prices_df, new_etf_prices_df

# Get the last available trading date
def get_last_update_date(symbol, symbols_df):
    symbols = symbols_df.filter(col("symbol") == symbol)
    if symbols.count() == 0:
        return None
    return symbols.select("last_update").collect()[0][0]

# Fetch and process stock/ETF data
def is_data_complete(df, date):
    date_data = df[df["Date"] == date]
    return not date_data.empty and not date_data.isnull().values.any()

def is_trading_day(date):
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=date, end_date=date)
    return not schedule.empty

# Fetch and process stock/ETF data
def fetch_data(symbol, is_etf=False, attempt=1, backoff=0):
    trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df = load_parquet_files()
    last_update = get_last_update_date(symbol, symbols_df)
    log_message(f"Last update date for {symbol}: {last_update}")
    
    if last_update:
        last_update_date = datetime.datetime.strptime(last_update, "%Y-%m-%d").date()
        days_since_last_update = (datetime.date.today() - last_update_date).days
        period = f"{days_since_last_update}d"
    else:
        period = "max"
    
    log_message(f"Fetching data for {symbol} with period {period}")
    
    if not is_trading_day(datetime.date.today()):
        log_message(f"Market is closed today. Skipping data fetch for {symbol}.")
        return
    
    try:
        df = yf.download(symbol, period=period, interval="1d")
        
        if df.empty:
            log_message(f"No new data found for {symbol}. Skipping...")
            return

        log_message(f"Data fetched for {symbol}: {df.head()}")
        df.reset_index(inplace=True)
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df = df[df["Date"] > last_update] if last_update else df
        
        log_message(f"Filtered data for {symbol} after last update: {df.head()}")
        log_message(f"Fetched {len(df)} rows of data for {symbol}")
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        new_trading_days_df, new_symbols_df, new_stock_prices_df, new_etf_prices_df = insert_stock_data(df, symbol, is_etf, trading_days_df, symbols_df, stock_prices_df, etf_prices_df)
        
        if last_update and not is_data_complete(df, last_update_date):
            log_message(f"Data for {last_update_date} is not complete. Retrying later.")
            return
        
        symbols_df = symbols_df.withColumn("last_update", when(col("symbol") == symbol, lit(datetime.date.today())).otherwise(col("last_update")))
        save_parquet_files(new_trading_days_df, new_symbols_df, new_stock_prices_df, new_etf_prices_df, spark.createDataFrame([], metadata_df.schema))
        
        log_message(f"{symbol} updated successfully.")
        time.sleep(1.5)
        
    except Exception as e:
        log_message(f"Error downloading {symbol}: {e}")

        if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
            backoff_delays = [3600, 7200, 86400]
            if attempt <= 3:
                log_message(f"Retrying {symbol} in 5 seconds (Attempt {attempt}/3)...")
                time.sleep(5)
                return fetch_data(symbol, is_etf, attempt + 1)
            elif backoff < len(backoff_delays):
                wait_time = backoff_delays[backoff]
                log_message(f"Rate limit hit! Waiting {wait_time//3600} hours before retrying {symbol}...")
                time.sleep(wait_time)
                return fetch_data(symbol, is_etf, 1, backoff + 1)
            else:
                log_message(f"Skipping {symbol} after multiple failed attempts.")
                return
        else:
            log_message(f"Skipping {symbol} due to an error: {e}")
            return

def download_all_stock_data():
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    data = pd.read_csv(url, sep="|")

    valid_data = data[data["Test Issue"] == "N"]
    valid_data = valid_data[["NASDAQ Symbol", "Security Name", "Market Category", "ETF", "Round Lot Size"]]
    valid_data.rename(columns={"NASDAQ Symbol": "Symbol"}, inplace=True)

    etfs = valid_data[valid_data["ETF"] == "Y"]
    stocks = valid_data[valid_data["ETF"] == "N"]

    valid_symbols = []

    print("\nDownloading Stocks...")
    for _, row in stocks.iterrows():
        symbol = row["Symbol"]
        fetch_data(symbol, is_etf=False)
        valid_symbols.append(row)

    print("\nDownloading ETFs...")
    for _, row in etfs.iterrows():
        symbol = row["Symbol"]
        fetch_data(symbol, is_etf=True)
        valid_symbols.append(row)

    valid_metadata_df = pd.DataFrame(valid_symbols)
    valid_metadata_df.to_csv("E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/updated_metadata.csv", index=False)
    log_message("All downloads completed.")

    trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df = load_parquet_files()
    metadata_df = metadata_df.filter(~col("symbol").isin(valid_metadata_df["Symbol"].tolist()))
    for _, row in valid_metadata_df.iterrows():
        new_row = spark.createDataFrame([(row["Symbol"], row["Security Name"], row["Market Category"], row["ETF"] == "Y", row["Round Lot Size"])],
                                        ["symbol", "security_name", "market_category", "is_etf", "round_lot_size"])
        metadata_df = metadata_df.union(new_row)
    save_parquet_files(spark.createDataFrame([], trading_days_df.schema), spark.createDataFrame([], symbols_df.schema), spark.createDataFrame([], stock_prices_df.schema), spark.createDataFrame([], etf_prices_df.schema), metadata_df)
    log_message("Metadata updated in the database.")

if __name__ == "__main__":
    download_all_stock_data()