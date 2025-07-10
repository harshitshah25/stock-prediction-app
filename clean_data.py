import argparse
import logging
import sys
import pandas as pd
import os

def setup_logging(log_file='data_cleaning.log'):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

def parse_args():
    parser = argparse.ArgumentParser(description='Stock Data Cleaning Script')

    parser.add_argument('--input', type=str, default='data/stock_data.csv', help='Path to raw input CSV')
    parser.add_argument('--output', type=str, default='data/stock_data_cleaned.csv', help='Path for cleaned output CSV')

    return parser.parse_args()

def clean_data(input_path, output_path, logger):
    logger.info(f"Reading raw data from {input_path}")

    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        sys.exit(1)

    logger.info(f"Initial rows: {len(df)}")

    # Parse DATE
    logger.info("Parsing DATE column to datetime")
    try:
        df['DATE'] = pd.to_datetime(df['DATE'])
    except Exception as e:
        logger.error(f"Invalid DATE parsing: {e}")
        sys.exit(1)

    # Drop missing values
    logger.info("Dropping rows with missing values")
    df = df.dropna()
    logger.info(f"Rows after dropping NAs: {len(df)}")

    # Remove obviously invalid prices (e.g. negative or zero)
    logger.info("Filtering out invalid price data")
    price_cols = ['Price_Open', 'Price_Close', 'Price_High', 'Price_Low', 'Volume']
    for col in price_cols:
        df = df[df[col] > 0]

    logger.info(f"Rows after filtering negatives/zeros: {len(df)}")

    # Sort by TICKER and DATE
    logger.info("Sorting by TICKER and DATE")
    df = df.sort_values(by=['TICKER', 'DATE'])

    # Save cleaned CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Cleaned data saved to {output_path}")

def main():
    logger = setup_logging()
    args = parse_args()
    logger.info(f"Arguments parsed: {args}")

    clean_data(args.input, args.output, logger)


if __name__ == "__main__":
    main()
