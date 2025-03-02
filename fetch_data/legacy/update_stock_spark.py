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
SYMBOLS_PATH = r"Raw_Data\parquets\symbols.parquet"
STOCK_PRICES_PATH = r"Raw_Data\parquets\stock_prices.parquet"
ETF_PRICES_PATH = r"Raw_Data\parquets\etf_prices.parquet"
METADATA_PATH = r"Raw_Data\parquets\metadata.parquet"
LOG_PATH = r"Raw_Data\logs\update_log.txt"

# Ensure the Parquet directory exists
PARQUET_DIR = os.path.dirname(TRADING_DAYS_PATH)

# Define schemas for the DataFrames
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
def save_parquet_files(new_trading_days_df=spark.createDataFrame([], schema = trading_days_schema), new_symbols_df=spark.createDataFrame([], schema = symbols_schema), 
                       new_stock_prices_df=spark.createDataFrame([], schema = stock_prices_schema), new_etf_prices_df=spark.createDataFrame([], schema = etf_prices_schema), 
                       new_metadata_df=spark.createDataFrame([], schema = metadata_schema)):
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
from pyspark.sql import functions as F
from pyspark.sql import Row

def get_next_id(df, id_column):
    """Returns the next available ID by finding the max and incrementing by 1."""
    max_id = df.agg(F.max(id_column)).collect()[0][0]
    return (max_id if max_id is not None else 0) + 1

# Insert a trading day if it doesn't exist and return the date_id
def insert_trading_day(date, trading_days_df):
    """
    Insert a trading day if it doesn't exist and return the date_id
    
    Args:
        date: Either datetime.date, datetime.datetime, pd.Timestamp, or str
        trading_days_df: Spark DataFrame containing trading days
    
    Returns:
        Spark DataFrame with new trading day or empty DataFrame if date exists
    """
    # Standardize date format
    if isinstance(date, str):
        try:
            date = pd.Timestamp(date).date()
        except ValueError as e:
            log_message(f"Invalid date format: {e}")
            return spark.createDataFrame([], trading_days_df.schema)
    elif isinstance(date, pd.Timestamp):
        date = date.date()
    elif isinstance(date, datetime.datetime):
        date = date.date()
    
    date_str = date.strftime("%Y-%m-%d")
    
    if trading_days_df.isEmpty():
        next_date_id = 1
        new_row = Row(date_id=next_date_id, trade_date=date_str)
        return spark.createDataFrame([new_row], ["date_id", "trade_date"])
    # Check if the date already exists
    existing_date = trading_days_df.filter(F.col("trade_date") == date_str).count()
    
    if existing_date == 0:
        next_date_id = get_next_id(trading_days_df, "date_id")
        new_row = Row(date_id=next_date_id, trade_date=date_str)
        return spark.createDataFrame([new_row], ["date_id", "trade_date"])
    
    return spark.createDataFrame([], trading_days_df.schema)

# Insert a symbol if it doesn't exist
def insert_symbol(symbol, is_etf, symbols_df):

    if symbols_df.isEmpty():
        next_symbol_id = 1
        new_row = spark.createDataFrame([(next_symbol_id, symbol, is_etf, datetime.date.today())], symbols_schema)
        return new_row
    
    symbols = symbols_df.filter(F.col("symbol") == symbol)

    if symbols.count() == 0:
        next_symbol_id = get_next_id(symbols_df, "symbol_id")
        new_row = spark.createDataFrame([(next_symbol_id, symbol, is_etf, datetime.date.today())], symbols_schema)
        return new_row

    return spark.createDataFrame([], symbols_df.schema)

# Insert stock/ETF data for each row, ensuring missing dates and symbols are added
def insert_stock_data(df, symbol, is_etf, trading_days_df, symbols_df, stock_prices_df, etf_prices_df):
    new_trading_days_df = spark.createDataFrame([], trading_days_df.schema)
    new_symbols_df = spark.createDataFrame([], symbols_df.schema)
    new_stock_prices_df = spark.createDataFrame([], stock_prices_df.schema)
    new_etf_prices_df = spark.createDataFrame([], etf_prices_df.schema)
    print(symbol)
    print(type(symbol))
    # Insert the symbol if it does not exist
    new_symbol_row = insert_symbol(symbol, is_etf, symbols_df)
    if new_symbol_row.count() > 0:
        new_symbols_df = new_symbols_df.union(new_symbol_row)
        symbols_df = symbols_df.union(new_symbol_row)
    # Get symbol_id
    symbol_id = symbols_df.filter(F.col("symbol") == symbol).select("symbol_id").collect()[0][0]
    log_message(f"Inserting data for symbol: {symbol} with symbol_id: {symbol_id}")
    
    for _, row in df.iterrows():
        new_trading_day_row = insert_trading_day(row["Date"], trading_days_df)
        if new_trading_day_row.count() > 0:
            new_trading_days_df = new_trading_days_df.union(new_trading_day_row)
            trading_days_df = trading_days_df.union(new_trading_day_row)

        # Get the date_id for the inserted or existing date
        date_id = trading_days_df.filter(F.col("trade_date") == row["Date"]).select("date_id").collect()[0][0]

        # Insert the stock/ETF data
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
    try:
        # Ensure symbol is string
        symbol = str(symbol).strip()
        
        # Use equals instead of == for comparison
        symbols = symbols_df.filter(F.col("symbol").eqNullSafe(symbol))
        
        if symbols.count() == 0:
            return None
            
        last_update = symbols.select("last_update").collect()[0][0]
        if last_update is None:
            return None
        
        if isinstance(last_update, pd.Series):
            last_update = last_update.iloc[0]  # Convert Series to scalar

        # Return as datetime.date object for consistency
        if isinstance(last_update, datetime.datetime):
            return last_update.date()
        elif isinstance(last_update, str):
            return datetime.datetime.strptime(last_update, "%Y-%m-%d").date()
        return last_update
        
    except Exception as e:
        log_message(f"Error in get_last_update_date: {str(e)}\n{traceback.format_exc()}")
        return None


def is_data_complete(df, date):
    date_data = df[df["Date"] == date]
    return not date_data.empty and not date_data.isnull().values.any()

def is_trading_day(date):
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=date, end_date=date)
    return not schedule.empty

# Fetch and process stock/ETF data
import requests
import traceback

# Global variables for backoff strategy
global_backoff = 0 
yahoo_down = False

def fetch_data(symbol, is_etf=False, attempt=1):
    global global_backoff, yahoo_down
    if global_backoff > 0:
        log_message(f"Global backoff active. Waiting {global_backoff} seconds before retrying {symbol}...")
        time.sleep(global_backoff)

    if yahoo_down:
        log_message(f"Yahoo Finance is down. Skipping {symbol} for now.")
        return False  # Mark for retry later

    trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df = load_parquet_files()

    if symbols_df.isEmpty():
        log_message(f"symbols_df is empty. Skipping lookup for {symbol} on first run.")
        last_update = None  # Assume this is the first run
    else:
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
        return None  # Mark for retry later
    
    try:
        df = yf.download(symbol, period=period, interval="1d")
        if df.empty:
            log_message(f"No new data found for {symbol}. Possible rate limit.")
            raise Exception("Empty DataFrame - Possible Rate Limit")  
        df.reset_index(inplace=True)
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        print(df.head())
        print(df.dtypes)
        new_trading_days_df, new_symbols_df, new_stock_prices_df, new_etf_prices_df = insert_stock_data(df, symbol, is_etf, trading_days_df, symbols_df, stock_prices_df, etf_prices_df)
        save_parquet_files(new_trading_days_df, new_symbols_df, new_stock_prices_df, new_etf_prices_df)

        log_message(f"{symbol} updated successfully.")
        return True  # Success

    except requests.exceptions.RequestException as e:
        log_message(f"Network error while fetching {symbol}: {e}")
        yahoo_down = True  # Mark Yahoo Finance as down
        return False  # Retry later

    except Exception as e:
        log_message(f"Error downloading {symbol}: {e}")

        if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
            backoff_times = [3600, 7200, 10800, 18000]  # 1 hour, 2 hours, 3 hours, 5 hours
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
        # Set timeout to avoid hanging
        log_message("Checking Yahoo Finance availability...")
        response = requests.get("https://finance.yahoo.com", timeout=10)
        if response.status_code == 200:
            # Verify data access
            test_data = yf.download(test_symbol, period="1d", interval="1d", timeout=10)
            if not test_data.empty:
                yahoo_down = False
                log_message("Yahoo Finance is back online!")
                return True
        yahoo_down = True
        log_message("Yahoo Finance is still down.")
        return False
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
        yahoo_down = True
        log_message("Yahoo Finance connection timeout.")
        return False

def validate_nasdaq_data(data):
    """
    Validate NASDAQ data before processing
    
    Returns:
        tuple: (valid_data, error_message)
    """
    if data.empty:
        return None, "Empty NASDAQ data received"
        
    required_columns = ["NASDAQ Symbol", "Security Name", "Market Category", "ETF", "Round Lot Size"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        return None, f"Missing required columns: {missing_columns}"
        
    
    # Validate ETF column
    valid_data = data[data["ETF"].isin(['Y', 'N'])]
    
    return valid_data, None

def validate_yahoo_data(df, symbol):
    """
    Validate Yahoo Finance data before processing
    
    Returns:
        tuple: (valid_data, error_message)
    """
    if df.empty:
        return None, f"Empty data received for {symbol}"
        
    # Check for required columns
    required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return None, f"Missing required columns: {missing_columns}"
        
    # Remove rows with any NaN values
    valid_data = df.dropna()
    
    # Validate price data
    price_columns = ["Open", "High", "Low", "Close"]
    invalid_prices = (valid_data[price_columns] <= 0).any(axis=None)
    if invalid_prices:
        return None, f"Invalid price data found for {symbol}"
        
    # Validate volume data
    invalid_volume = (valid_data["Volume"] < 0).any()
    if invalid_volume:
        return None, f"Invalid volume data found for {symbol}"
        
    # Validate date order
    if not valid_data["Date"].is_monotonic_increasing:
        return None, f"Dates are not in ascending order for {symbol}"
        
    return valid_data, None

def download_all_stock_data():
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    try:
        data = pd.read_csv(url, sep="|")
        valid_data, error = validate_nasdaq_data(data)
        if error:
            log_message(f"NASDAQ data validation failed: {error}")
            return

        valid_data = data[data["Test Issue"] == "N"]

        etfs = valid_data[valid_data["ETF"] == "Y"]
        stocks = valid_data[valid_data["ETF"] == "N"]

        valid_symbols = []
        failed_symbols = []

        print("\nStarting stock data download...")
        
        # preload
        trading_days_df, symbols_df, stock_prices_df, etf_prices_df, metadata_df = load_parquet_files()

        for _, row in stocks.iterrows():
            symbol = row["NASDAQ Symbol"]
            result = fetch_data(symbol, False)
            if result is True:
                valid_symbols.append(row)
            elif result is False:
                failed_symbols.append(row)

        for _, row in etfs.iterrows():
            symbol = row["NASDAQ Symbol"]
            result = fetch_data(symbol, True)
            if result is True:
                valid_symbols.append(row)
            elif result is False:
                failed_symbols.append(row)

        MAX_RETRIES = 5  # Limit retries to prevent infinite loop
        retry_attempts = 0

        while failed_symbols and retry_attempts < MAX_RETRIES:
            retry_attempts += 1
            log_message(f"Retrying {len(failed_symbols)} failed downloads (Attempt {retry_attempts}/{MAX_RETRIES}) after backoff...")

            if yahoo_down:
                check_yahoo_status()
                time.sleep(1800)  # Wait 30 minutes before checking again
                continue  # Skip retrying until Yahoo Finance is up

            time.sleep(global_backoff)  # Wait before retrying
            global_backoff = 0  # Reset after waiting

            new_failed = []
            for row in failed_symbols:
                result = fetch_data(row["NASDAQ Symbol"], row["ETF"] == "Y", retry_attempts)
                if result is True:
                    valid_symbols.append(row)
                else:
                    new_failed.append(row)  # Still failed

            failed_symbols = new_failed  # Update retry list

        if failed_symbols:
            log_message(f"Failed to fetch {len(failed_symbols)} symbols after {MAX_RETRIES} attempts. Skipping them.")


        valid_metadata_df = pd.DataFrame(valid_symbols)
        valid_metadata_df.to_csv("E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/updated_metadata.csv", index=False)
        log_message("All downloads completed.")

        # metadata_df = metadata_df.filter(~col("symbol").isin(valid_metadata_df["Symbol"].tolist()))
        # Convert Spark DataFrame column to a list
        existing_symbols = metadata_df.select("symbol").rdd.flatMap(lambda x: x).collect()

        # Filter valid_metadata_df to exclude rows with symbols in existing_symbols
        filtered_valid_metadata_df = valid_metadata_df[~valid_metadata_df["Symbol"].isin(existing_symbols)]
        new_metadata_df = spark.createDataFrame([], metadata_df.schema)
        for _, row in filtered_valid_metadata_df.iterrows():
            new_row = spark.createDataFrame([(row["Symbol"], row["Security Name"], row["Market Category"], row["ETF"] == "Y", row["Round Lot Size"])],
                                            ["symbol", "security_name", "market_category", "is_etf", "round_lot_size"])
            new_metadata_df = new_metadata_df.union(new_row)

        save_parquet_files(new_metadata_df = new_metadata_df)

        log_message("Metadata updated in the database.")
    except Exception as e:
        log_message(f"Error downloading NASDAQ data: {e}")
        return

def main():
    spark = None
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("StocksX_Price_and_News_Influences") \
            .getOrCreate()
            
        download_all_stock_data()
        
    except Exception as e:
        log_message(f"Critical error in main execution: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            log_message("Spark session closed.")

if __name__ == "__main__":
    main()