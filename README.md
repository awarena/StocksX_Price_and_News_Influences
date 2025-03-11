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
## Running Instructions
1. Activate `tf270_stocks` environemnt.
2. Run `run_ingestion.py`.