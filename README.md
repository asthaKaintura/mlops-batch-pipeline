# MLOps Batch Pipeline

## Overview
A minimal MLOps-style batch pipeline in Python for time-series signal generation.

## Features
- Config-driven execution (YAML)
- Data validation
- Rolling mean + signal generation
- Logging and metrics output
- Docker support

## How to Run
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
