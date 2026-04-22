import argparse
import yaml
import pandas as pd
import numpy as np
import json
import logging
import time
import os


# CLI ARGUMENTS

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)
    return parser.parse_args()


# LOGGING SETUP

def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


# CONFIG VALIDATION

def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    required_fields = ["seed", "window", "version"]
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing config field: {field}")

    return config


# DATA VALIDATION

def load_data(input_path):
    import os
    import pandas as pd
    import csv

    if not os.path.exists(input_path):
        raise FileNotFoundError("Input CSV not found")

    try:
        df = pd.read_csv(input_path, quoting=csv.QUOTE_NONE)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("CSV file is empty")

    # Clean column names
    df.columns = df.columns.str.replace('"', '').str.lower()

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df


# MAIN PROCESSING

def process(df, window):
    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    # keep NaNs for first rows → no signal
    df["signal"] = np.where(
        df["close"] > df["rolling_mean"], 1, 0
    )

    # exclude NaNs from signal calculation
    valid_signals = df["signal"][~df["rolling_mean"].isna()]

    return df, valid_signals


# MAIN

def main():
    args = parse_args()
    setup_logging(args.log_file)

    start_time = time.time()

    try:
        logging.info("Loading config...")
        config = load_config(args.config)

        np.random.seed(config["seed"])

        logging.info("Loading dataset...")
        df = load_data(args.input)

        logging.info("Processing data...")
        df, valid_signals = process(df, config["window"])

        rows_processed = len(df)
        signal_rate = float(valid_signals.mean()) if len(valid_signals) > 0 else 0.0

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "rows_processed": rows_processed,
            "signal_rate": signal_rate,
            "latency_ms": latency_ms,
            "version": config["version"]
        }

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=4)

        logging.info("Run completed successfully")
        logging.info(f"Metrics: {metrics}")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()