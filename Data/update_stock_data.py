import os
import time
import pandas as pd
import yfinance as yf
import datetime
import pandas_ta as ta

LAST_UPDATE_FILE = "./last_update.txt"
STOCKS_DIR = "./stocks"
ETFS_DIR = "./etfs"
METADATA_FILE = "symbols_valid_meta.csv"

# Ensure directories exist
os.makedirs(STOCKS_DIR, exist_ok=True)
os.makedirs(ETFS_DIR, exist_ok=True)

COLUMN_ORDER = ["Date", "Open", "High", "Low", "Close", "Volume", "SMA_20", "EMA_20", "BB_upper", 
                "BB_middle", "BB_lower", "MACD", "MACD_signal", "MACD_hist", "STOCH_k", "STOCH_d", "ATR", "RSI_14"]

def days_since_last_update():
    if not os.path.exists(LAST_UPDATE_FILE):
        return float('inf')  # First run
    
    with open(LAST_UPDATE_FILE, "r") as file:
        last_update = datetime.datetime.strptime(file.read().strip(), "%Y-%m-%d")
    
    return (datetime.datetime.today() - last_update).days

def update_last_run_date():
    with open(LAST_UPDATE_FILE, "w") as file:
        file.write(datetime.datetime.today().strftime("%Y-%m-%d"))

def fetch_data(symbol, directory, attempt=1, backoff=0):
    """Download and merge stock data with consistent format and retry logic."""
    csv_path = os.path.join(directory, f"{symbol}.csv")
    period = "max" if not os.path.exists(csv_path) else "3d"

    try:
        df = yf.download(symbol, period=period, interval="1d")
        if df.empty:
            print(f"⚠️ No data found for {symbol}. Skipping...")
            return False  # Mark as invalid

        # Standardize column names
        df = df[["Open", "High", "Low", "Close", "Volume"]].reset_index()
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]

        # Calculate technical indicators
        df["SMA_20"] = ta.sma(df["Close"], length=20)
        df["EMA_20"] = ta.ema(df["Close"], length=20)
        bb = ta.bbands(df["Close"], length=20)
        if not bb.empty:
            df["BB_upper"] = bb["BBU_20_2.0"]
            df["BB_middle"] = bb["BBM_20_2.0"]
            df["BB_lower"] = bb["BBL_20_2.0"]
        macd = ta.macd(df["Close"])
        if not macd.empty:
            df["MACD"] = macd["MACD_12_26_9"]
            df["MACD_signal"] = macd["MACDs_12_26_9"]
            df["MACD_hist"] = macd["MACDh_12_26_9"]
        stoch = ta.stoch(df["High"], df["Low"], df["Close"])
        if not stoch.empty:
            df["STOCH_k"] = stoch["STOCHk_14_3_3"]
            df["STOCH_d"] = stoch["STOCHd_14_3_3"]
        df["ATR"] = ta.atr(df["High"], df["Low"], df["Close"], length=14)
        df["RSI_14"] = ta.rsi(df["Close"], length=14)

        # Merge with existing data if available
        if os.path.exists(csv_path):
            df_existing = pd.read_csv(csv_path, parse_dates=["Date"])
            df_existing = df_existing[COLUMN_ORDER]  # Ensure correct order

            # Drop duplicate dates to avoid conflicts
            df = pd.concat([df_existing, df]).drop_duplicates(subset=["Date"]).sort_values("Date")

        df = df[COLUMN_ORDER]  # Ensure final column order
        df.to_csv(csv_path, index=False)
        print(f"✅ {symbol} updated")
        time.sleep(1.5)  # Slow down requests to avoid rate limits
        return True  # Mark as valid

    except Exception as e:
        print(f"⚠️ Error downloading {symbol}: {e}")

        if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
            backoff_delays = [3600, 7200, 86400]  # 1 hour, 2 hours, 1 day
            if attempt <= 3:
                print(f"🔄 Retrying {symbol} in 5 seconds (Attempt {attempt}/3)...")
                time.sleep(5)
                return fetch_data(symbol, directory, attempt + 1)
            elif backoff < len(backoff_delays):
                wait_time = backoff_delays[backoff]
                print(f"⏳ Rate limit hit! Waiting {wait_time//3600} hours before retrying {symbol}...")
                time.sleep(wait_time)
                return fetch_data(symbol, directory, 1, backoff + 1)
            else:
                print(f"❌ Skipping {symbol} after multiple failed attempts.")
                return False

def download_stock_data():
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
    print("\n📉 Downloading Stocks...")
    for _, row in stocks.iterrows():
        symbol = row["Symbol"]
        if fetch_data(symbol, STOCKS_DIR):
            valid_symbols.append(row)

    # Download ETFs
    print("\n📊 Downloading ETFs...")
    for _, row in etfs.iterrows():
        symbol = row["Symbol"]
        if fetch_data(symbol, ETFS_DIR):
            valid_symbols.append(row)

    # Save updated metadata only for successfully downloaded symbols
    valid_metadata_df = pd.DataFrame(valid_symbols)
    valid_metadata_df.to_csv(METADATA_FILE, index=False)

    update_last_run_date()
    print(f"✅ All downloads completed. Metadata saved to {METADATA_FILE}")

if __name__ == "__main__":
    if days_since_last_update() >= 3:
        download_stock_data()
    else:
        print("⏳ Not updating yet. Less than 3 days since last update.")