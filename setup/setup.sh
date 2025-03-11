#!/bin/bash

# Create or update conda environment
conda env update -f environment.yml

# Activate the environment
conda activate stocksx_env

# Install the package in development mode
pip install -e .

echo "StocksX environment setup complete!"