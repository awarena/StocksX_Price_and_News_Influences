import sqlite3
import pandas as pd
import os

os.environ["PYSPARK_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D:/Tools/anaconda3/envs/tf270_stocks/python.exe"

DB_PATH = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/stocks.db"

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)

# List tables
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("Tables in database:", tables)

# Load all tables into Pandas DataFrames
dfs = {}
for table in tables["name"]:
    dfs[table] = pd.read_sql_query(f"SELECT * FROM {table};", conn)

conn.close()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQLite to Spark Migration") \
    .config("spark.driver.extraClassPath", "sqlite-jdbc-3.34.0.jar") \
    .config("spark.driver.memory", "10g") \
    .config("spark.executor.memory", "10g") \
    .getOrCreate()

# Set log level to ERROR to suppress warnings
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import DataFrame

# Convert each Pandas DataFrame to a Spark DataFrame
spark_dfs = {table: spark.createDataFrame(dfs[table]) for table in dfs}

output_path = "E:/Projects/StocksX_Price_and_News_Influences/Raw_Data/parquets/"

for table, df in spark_dfs.items():
    df.write.mode("overwrite").parquet(f"{output_path}/{table}.parquet")