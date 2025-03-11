@echo off
ECHO Setting up StocksX environment...

:: Create or update conda environment
call conda env update -f tf270_stocks.yml

:: Activate the environment
call conda activate tf270_stocks

:: Install the package in development mode
pip install -e .

ECHO StocksX environment setup complete!