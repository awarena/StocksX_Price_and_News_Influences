# 1. Instructions
## Setup Instructions (all necessary files are in the `setup` folder)
### Environment Setup
1. Run `conda env create -f tf270_stocks.yml` in anaconda shell to install the environment.
### Hadoop Setup (For Windows only)
1. Extract the `hadoop-3.3.6` binary folder.
2. Go to `Edit the system environment variables` -> `Environment Variables`
3. Under `System variables`, click `New`
4. Enter variable name as `HADOOP_HOME`, and set variable value to your hadoop binary folder. For example, `C:\haddop-3.3.6`.
5. Still under `System variables`, find and open `Path`
6. Add `%HADOOP_HOME%\bin` to `Path`
## Running Instructions
1. Open `update_stock_spark_module.py` in `fetch_data`.
2. Set `os.environ["PYSPARK_PYTHON"]` and `os.environ["PYSPARK_DRIVER_PYTHON"]` to the path to your python.exe file inside the previously installed conda environment.
3. Activate `tf270_stocks` environemnt.
4. Run `update_stock_spark_module.py`.
