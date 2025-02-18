import os
import sqlite3
import pandas as pd
import yfinance as yf
import datetime
import pandas_ta as ta

DB_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/stocks.db"
LOG_PATH = "E:/Projects/AutoRunStockUpdater/logs/update_log.txt"

def log_message(message):
    with open(LOG_PATH, "a") as log_file:
        log_file.write(f"{datetime.datetime.now()} - {message}\n")
    print(message)

# Ensure database exists
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn

# Create tables if they don’t exist
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS trading_days (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date DATE UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS symbols (
        symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE NOT NULL,
        is_etf BOOLEAN NOT NULL DEFAULT 0,
        last_update DATE
    );

    CREATE TABLE IF NOT EXISTS stock_prices (
        symbol_id INTEGER NOT NULL,
        date_id INTEGER NOT NULL,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol_id, date_id),
        FOREIGN KEY (symbol_id) REFERENCES symbols(symbol_id),
        FOREIGN KEY (date_id) REFERENCES trading_days(date_id)
    );

    CREATE TABLE IF NOT EXISTS etf_prices (
        symbol_id INTEGER NOT NULL,
        date_id INTEGER NOT NULL,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol_id, date_id),
        FOREIGN KEY (symbol_id) REFERENCES symbols(symbol_id),
        FOREIGN KEY (date_id) REFERENCES trading_days(date_id)
    );

    CREATE TABLE IF NOT EXISTS metadata (
        symbol TEXT PRIMARY KEY,
        security_name TEXT,
        market_category TEXT,
        is_etf BOOLEAN,
        round_lot_size INTEGER
    );
    """)
    conn.commit()
    conn.close()

# Insert a date if not exists
def insert_trading_day(date, conn):
    """Insert date into trading_days if not exists and return date_id."""
    cursor = conn.cursor()
    
    # ✅ Ensure date is stored as a string (YYYY-MM-DD)
    date_str = date.strftime("%Y-%m-%d") if isinstance(date, pd.Timestamp) else str(date)

    cursor.execute("INSERT OR IGNORE INTO trading_days (trade_date) VALUES (?)", (date_str,))
    cursor.execute("SELECT date_id FROM trading_days WHERE trade_date = ?", (date_str,))
    
    return cursor.fetchone()[0]


# Insert a symbol if not exists
def insert_symbol(symbol, is_etf, conn):
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO symbols (symbol, is_etf) VALUES (?, ?)", (symbol, is_etf))
    cursor.execute("SELECT symbol_id, last_update FROM symbols WHERE symbol = ?", (symbol,))
    result = cursor.fetchone()
    if result is None:
        raise ValueError(f"Symbol {symbol} not found in the database after insertion.")
    return result[0], result[1]

# Insert stock/ETF data
def insert_stock_data(df, symbol, is_etf, conn):
    table_name = "etf_prices" if is_etf else "stock_prices"
    symbol_id, _ = insert_symbol(symbol, is_etf, conn)
    log_message(f"Inserting data for symbol: {symbol} with symbol_id: {symbol_id}")

    for _, row in df.iterrows():
        date_id = insert_trading_day(row["Date"], conn)
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT OR REPLACE INTO {table_name} 
            (symbol_id, date_id, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (symbol_id, date_id, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))
        log_message(f"Inserted data for symbol_id: {symbol_id}, date_id: {date_id}, date: {row['Date']}")
    conn.commit()


# Get the last available trading date
def get_last_update_date(symbol):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT last_update FROM symbols WHERE symbol = ?", (symbol,))
    result = cursor.fetchone()
    conn.close()
    if result is None:
        return None
    return result[0]

# Fetch and process stock/ETF data
import time
def is_data_complete(df, date):
    """Check if the data for the given date is complete."""
    date_data = df[df["Date"] == date]
    return not date_data.empty and not date_data.isnull().values.any()

import pandas_market_calendars as mcal

def is_trading_day(date):
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=date, end_date=date)
    return not schedule.empty

# Fetch and process stock/ETF data
def fetch_data(symbol, is_etf=False, attempt=1, backoff=0):
    conn = get_db_connection()
    last_update = get_last_update_date(symbol)
    log_message(f"Last update date for {symbol}: {last_update}")
    
    if last_update:
        last_update_date = datetime.datetime.strptime(last_update, "%Y-%m-%d").date()
        days_since_last_update = (datetime.date.today() - last_update_date).days
        period = f"{days_since_last_update}d"
    else:
        period = "max"
    
    log_message(f"Fetching data for {symbol} with period {period}")
    
    # Check if today is a trading day
    if not is_trading_day(datetime.date.today()):
        log_message(f"Market is closed today. Skipping data fetch for {symbol}.")
        conn.close()
        return
    
    try:
        df = yf.download(symbol, period=period, interval="1d")
        
        if df.empty:
            log_message(f"No new data found for {symbol}. Skipping...")
            conn.close()
            return

        log_message(f"Data fetched for {symbol}: {df.head()}")
        df.reset_index(inplace=True)
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df = df[df["Date"] > last_update] if last_update else df
        
        log_message(f"Filtered data for {symbol} after last update: {df.head()}")
        log_message(f"Fetched {len(df)} rows of data for {symbol}")
        df["Date"] = pd.to_datetime(df["Date"]).dt.date  # Ensure Date is in YYYY-MM-DD format

        insert_stock_data(df, symbol, is_etf, conn)
        
        # Check if the data for the last_update date is complete before updating
        if last_update and not is_data_complete(df, last_update_date):
            log_message(f"Data for {last_update_date} is not complete. Retrying later.")
            conn.close()
            return
        
        # Update the last_update date for the symbol
        cursor = conn.cursor()
        cursor.execute("UPDATE symbols SET last_update = ? WHERE symbol = ?", (datetime.date.today(), symbol))
        conn.commit()
        
        conn.close()
        log_message(f"{symbol} updated successfully.")
        
        # Ensure a 1.5-second interval between requests
        time.sleep(1.5)
        
    except Exception as e:
        log_message(f"Error downloading {symbol}: {e}")

        if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
            backoff_delays = [3600, 7200, 86400]  # 1 hour, 2 hours, 1 day
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

    # Clean & keep relevant columns for metadata
    valid_data = data[data["Test Issue"] == "N"]
    valid_data = valid_data[["NASDAQ Symbol", "Security Name", "Market Category", "ETF", "Round Lot Size"]]
    valid_data.rename(columns={"NASDAQ Symbol": "Symbol"}, inplace=True)

    # Separate ETFs and Stocks
    etfs = valid_data[valid_data["ETF"] == "Y"]
    stocks = valid_data[valid_data["ETF"] == "N"]

    valid_symbols = []

    # Download Stocks
    print("\nDownloading Stocks...")
    for _, row in stocks.iterrows():
        symbol = row["Symbol"]
        fetch_data(symbol, is_etf=False)
        valid_symbols.append(row)

    # Download ETFs
    print("\nDownloading ETFs...")
    for _, row in etfs.iterrows():
        symbol = row["Symbol"]
        fetch_data(symbol, is_etf=True)
        valid_symbols.append(row)

    # Save updated metadata only for successfully downloaded symbols
    valid_metadata_df = pd.DataFrame(valid_symbols)
    valid_metadata_df.to_csv("E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/updated_metadata.csv", index=False)
    log_message("All downloads completed.")

    # Insert metadata into the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM metadata")  # Clear existing metadata
    for _, row in valid_metadata_df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO metadata (symbol, security_name, market_category, is_etf, round_lot_size)
            VALUES (?, ?, ?, ?, ?)
        """, (row["Symbol"], row["Security Name"], row["Market Category"], row["ETF"] == "Y", row["Round Lot Size"]))
    conn.commit()
    conn.close()
    log_message("Metadata updated in the database.")

if __name__ == "__main__":
    initialize_database()
    download_all_stock_data()
