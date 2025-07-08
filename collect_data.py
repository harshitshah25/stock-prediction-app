import argparse
import yaml
import logging
import os
import sys
from datetime import datetime
import yfinance as yf
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

def setup_logging(log_file='data_collection.log'):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def load_config(config_path):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def parse_args():
    parser = argparse.ArgumentParser(description='Stock Data Collection Script')

    parser.add_argument('--config', type=str, default='config.yaml', help='Path to config file')
    parser.add_argument('--tickers', nargs='+', help='List of stock tickers')
    parser.add_argument('--start_date', type=str, help='Start date YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, help='End date YYYY-MM-DD')
    parser.add_argument('--output_dir', type=str, help='Output directory')

    return parser.parse_args()

def merge_config_and_args(config, args):
    final_config = config.copy()

    if args.tickers:
        final_config['tickers'] = args.tickers
    if args.start_date:
        final_config['start_date'] = args.start_date
    if args.end_date:
        final_config['end_date'] = args.end_date
    if args.output_dir:
        final_config['output_dir'] = args.output_dir

    return final_config


def download_data(tickers, start_date, end_date, alpaca_api_key, alpaca_api_secret, logger):
    logger.info(f"Connecting to Alpaca Markets API...")
    client = StockHistoricalDataClient(alpaca_api_key, alpaca_api_secret)

    all_data = []

    for ticker in tickers:
        logger.info(f"Downloading data for {ticker}")

        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=end_date
            )
            bars = client.get_stock_bars(request_params)
            df = bars.df.reset_index()

            # Standardize columns
            df = df.rename(columns={
                'symbol': 'TICKER',
                'timestamp': 'DATE',
                'open': 'Price_Open',
                'close': 'Price_Close',
                'high': 'Price_High',
                'low': 'Price_Low',
                'volume': 'Volume'
            })

            df = df[['TICKER', 'DATE', 'Price_Open', 'Price_Close', 'Price_High', 'Price_Low', 'Volume']]
            all_data.append(df)

        except Exception as e:
            logger.error(f"Error downloading {ticker}: {e}")

    if all_data:
        final_data = pd.concat(all_data, ignore_index=True)
        logger.info("Data download complete.")
        return final_data
    else:
        logger.error("No data downloaded.")
        sys.exit(1)

def save_data(data, output_dir, logger):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'stock_data.csv')
    try:
        data.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        sys.exit(1)




def main():
    # Setup logging
    logger = setup_logging()

    # Parse CLI
    args = parse_args()
    logger.info("Arguments parsed.")

    # Load config file
    config = load_config(args.config)
    logger.info(f"Config loaded from {args.config}")

    # Merge CLI args
    final_config = merge_config_and_args(config, args)
    logger.info(f"Final configuration: {final_config}")

    # Validate dates
    try:
        start_date = datetime.strptime(final_config['start_date'], "%Y-%m-%d").date()
        end_date = datetime.strptime(final_config['end_date'], "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)

    alpaca_api_key = final_config['alpaca']['api_key']
    alpaca_api_secret = final_config['alpaca']['api_secret']

    data = download_data(
        final_config['tickers'],
        final_config['start_date'],
        final_config['end_date'],
        alpaca_api_key,
        alpaca_api_secret,
        logger
    )

    # Save data
    save_data(data, final_config['output_dir'], logger)


if __name__ == "__main__":
    main()
