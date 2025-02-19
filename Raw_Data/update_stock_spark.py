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
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType

os.environ["PYSPARK_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StocksX_Price_and_News_Influences") \
    .getOrCreate()

# Paths to Parquet files
TRADING_DAYS_PATH = r"Raw_Data\parquets\trading_days.parquet"
SYMBOLS_PATH = r"Raw_Data/parquets/symbols.parquet"
STOCK_PRICES_PATH = r"Raw_Data\parquets\stock_prices.parquet"
ETF_PRICES_PATH = r"Raw_Data\parquets\etf_prices.parquet"
METADATA_PATH = r"Raw_Data\parquets\metadata.parquet"
LOG_PATH = r"Raw_Data\logs\update_log.txt"

# Ensure the Parquet directory exists
PARQUET_DIR = os.path.dirname(TRADING_DAYS_PATH)

# Define schemas for the DataFrames
trading_days_schema = StructType([
    StructField("date_id", IntegerType(), True),
    StructField("trade_date", DateType(), True)
])

symbols_schema = StructType([
    StructField("symbol_id", IntegerType(), True),
    StructField("symbol", StringType(), True),
    StructField("is_etf", StringType(), True),
    StructField("last_update", DateType(), True)
])

stock_prices_schema = StructType([
    StructField("symbol_id", IntegerType(), True),
    StructField("date_id", IntegerType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", IntegerType(), True)
])

etf_prices_schema = stock_prices_schema

metadata_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("security_name", StringType(), True),
    StructField("market_category", StringType(), True),
    StructField("is_etf", StringType(), True),
    StructField("round_lot_size", IntegerType(), True)
])

if not os.path.exists(PARQUET_DIR):
    os.makedirs(PARQUET_DIR)

def log_message(message):
    # Log rotation
    if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > 1e6:
        os.rename(LOG_PATH, f"{os.path.splitext(LOG_PATH)[0]}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.txt")
    with open(LOG_PATH, "a") as log_file:
        log_file.write(f"{datetime.datetime.now()} - {message}\n")
    print(message)

# Load Parquet files into DataFrames
def load_parquet_files():
    def load_parquet(path, schema):
        if os.path.exists(path):
            return spark.read.parquet(path)
        else:
            return spark.createDataFrame([], schema)  # Create an empty DataFrame with the defined schema

    trading_days_df = load_parquet(TRADING_DAYS_PATH, trading_days_schema)
    symbols_df = load_parquet(SYMBOLS_PATH, symbols_schema)
    stock_prices_df = load_parquet(STOCK_PRICES_PATH, stock_prices_schema)
    etf_prices_df = load_parquet(ETF_PRICES_PATH, etf_prices_schema)
    metadata_df = load_parquet(METADATA_PATH, metadata_schema)
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
import concurrent.futures
import threading
import time
import requests

# Global variables for backoff strategy
backoff_lock = threading.Lock()
global_backoff = 0  # Shared across all threads
yahoo_down = False  # Flag to track Yahoo Finance outage

def fetch_data(symbol, is_etf=False, attempt=1):
    global global_backoff, yahoo_down

    with backoff_lock:
        if global_backoff > 0:
            log_message(f"Global backoff active. Waiting {global_backoff} seconds before retrying {symbol}...")
            time.sleep(global_backoff)

    if yahoo_down:
        log_message(f"Yahoo Finance is down. Skipping {symbol} for now.")
        return False  # Mark for retry later

    trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df = load_parquet_files()
    last_update = get_last_update_date(symbol, symbols_df)

    if last_update:
        last_update_date = datetime.datetime.strptime(last_update, "%Y-%m-%d").date()
        days_since_last_update = (datetime.date.today() - last_update_date).days
        period = f"{days_since_last_update}d"
    else:
        period = "max"

    log_message(f"Fetching data for {symbol} with period {period}")

    if not is_trading_day(datetime.date.today()):
        log_message(f"Market is closed today. Skipping data fetch for {symbol}.")
        return False  # Mark for retry later

    try:
        df = yf.download(symbol, period=period, interval="1d")

        if df.empty:
            log_message(f"No new data found for {symbol}. Possible rate limit.")
            raise Exception("Empty DataFrame - Possible Rate Limit")  

        df.reset_index(inplace=True)
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        insert_stock_data(df, symbol, is_etf, trading_days_df, symbols_df, stock_prices_df, etf_prices_df)
        save_parquet_files(trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df)

        log_message(f"{symbol} updated successfully.")
        return True  # Success

    except requests.exceptions.RequestException as e:
        log_message(f"Network error while fetching {symbol}: {e}")
        yahoo_down = True  # Mark Yahoo Finance as down
        return False  # Retry later

    except Exception as e:
        log_message(f"Error downloading {symbol}: {e}")

        if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
            with backoff_lock:
                backoff_times = [60, 300, 3600, 10800]  # 1 min, 5 min, 1 hour, 3 hours
                global_backoff = backoff_times[min(attempt - 1, len(backoff_times) - 1)]
            log_message(f"Rate limit detected! Applying backoff: {global_backoff} seconds.")
            return False  # Retry later
        elif "yahoo" in str(e).lower() or "service unavailable" in str(e).lower():
            yahoo_down = True  # Mark Yahoo Finance as down
            return False  # Retry later
        else:
            log_message(f"Skipping {symbol} due to an error: {e}")
            return None  # Permanent failure

def check_yahoo_status():
    """ Check if Yahoo Finance is back online """
    global yahoo_down
    test_symbol = "AAPL"
    
    try:
        log_message("Checking Yahoo Finance availability...")
        yf.download(test_symbol, period="1d", interval="1d")
        yahoo_down = False
        log_message("Yahoo Finance is back online!")
    except Exception:
        yahoo_down = True
        log_message("Yahoo Finance is still down.")

def download_all_stock_data():
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    data = pd.read_csv(url, sep="|")

    valid_data = data[data["Test Issue"] == "N"]
    valid_data = valid_data[["NASDAQ Symbol", "Security Name", "Market Category", "ETF", "Round Lot Size"]]
    valid_data.rename(columns={"NASDAQ Symbol": "Symbol"}, inplace=True)

    etfs = valid_data[valid_data["ETF"] == "Y"]
    stocks = valid_data[valid_data["ETF"] == "N"]

    valid_symbols = []
    failed_symbols = []

    print("\nStarting multi-threaded stock data download...")

    def process_row(row, is_etf):
        """ Helper function to fetch data and store the row """
        result = fetch_data(row["Symbol"], is_etf)
        if result is True:
            return row  # Success
        elif result is False:
            failed_symbols.append(row)  # Add to retry list
        return None  # Skip permanently failed rows

    max_threads = 10
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        stock_futures = {executor.submit(process_row, row, False): row for _, row in stocks.iterrows()}
        etf_futures = {executor.submit(process_row, row, True): row for _, row in etfs.iterrows()}

        for future in concurrent.futures.as_completed(stock_futures):
            row = future.result()
            if row:
                valid_symbols.append(row)

        for future in concurrent.futures.as_completed(etf_futures):
            row = future.result()
            if row:
                valid_symbols.append(row)

    # Retry failed downloads when Yahoo Finance is back
    while failed_symbols:
        log_message(f"Retrying {len(failed_symbols)} failed downloads after backoff...")

        if yahoo_down:
            check_yahoo_status()  # Periodically check if Yahoo Finance is back
            time.sleep(1800)  # Wait 30 minutes before checking again
            continue  # Skip retrying until Yahoo Finance is up

        time.sleep(global_backoff)  # Wait before retrying
        global_backoff = 0  # Reset after waiting

        new_failed = []
        with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
            retry_futures = {executor.submit(process_row, row, row["ETF"] == "Y"): row for row in failed_symbols}
            for future in concurrent.futures.as_completed(retry_futures):
                row = future.result()
                if row:
                    valid_symbols.append(row)
                else:
                    new_failed.append(row)  # Still failed

        failed_symbols = new_failed  # Update retry list

    valid_metadata_df = pd.DataFrame(valid_symbols)
    valid_metadata_df.to_csv("E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/updated_metadata.csv", index=False)
    log_message("All downloads completed.")

    # Update Spark metadata
    trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df = load_parquet_files()
    metadata_df = metadata_df.filter(~col("symbol").isin(valid_metadata_df["Symbol"].tolist()))

    for _, row in valid_metadata_df.iterrows():
        new_row = spark.createDataFrame([(row["Symbol"], row["Security Name"], row["Market Category"], row["ETF"] == "Y", row["Round Lot Size"])],
                                        ["symbol", "security_name", "market_category", "is_etf", "round_lot_size"])
        metadata_df = metadata_df.union(new_row)

    save_parquet_files(spark.createDataFrame([], trading_days_df.schema), 
                       spark.createDataFrame([], symbols_df.schema), 
                       spark.createDataFrame([], stock_prices_df.schema), 
                       spark.createDataFrame([], etf_prices_df.schema), 
                       metadata_df)

    log_message("Metadata updated in the database.")

if __name__ == "__main__":
    download_all_stock_data()
