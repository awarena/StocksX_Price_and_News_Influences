# 1. Instructions
## Setup Instructions (all necessary files are in the `setup` folder)
### Environment Setup
1. Run `setup.bat` if Windows, `setup.sh` if Linux
### Hadoop Setup (For Windows only)
1. Extract the `hadoop-3.3.6` binary folder.
2. Go to `Edit the system environment variables` -> `Environment Variables`
3. Under `System variables`, click `New`
4. Enter variable name as `HADOOP_HOME`, and set variable value to your hadoop binary folder. For example, `C:\hadoop-3.3.6`.
5. Still under `System variables`, find and open `Path`
6. Add `%HADOOP_HOME%\bin` to `Path`
### Hive setup (For Windows only)
1. Extract the `apache-hive-4.0.1-bin.zip` binary folder.
2. Go to `Edit the system environment variables` -> `Environment Variables`
3. Under `System variables`, click `New`
4. Enter variable name as `HIVE_HOME`, and set variable value to your hive binary folder. For example, `C:\apache-hive-4.0.1-bin`.
5. Still under `System variables`, find and open `Path`
6. Add `%HIVE_HOME%\bin` to `Path`
## Running Instructions
1. Activate `tf270_stocks` environemnt.
2. CD to the project directory
3. Run `python scripts/run_ingestion.py --iceberg_namespace "raw_data"`

*Note: Due to the usage of yfinance, metadata couldn't be downloaded in bulk and therefore has to be individual HTTP call. Because of this limitation, sector fetcher was not integrated into the data ingestion pipeline but instead in a separate optional notebook. (Will turn it into a script later)*