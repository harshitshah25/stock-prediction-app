import argparse
import logging
import sys
import pandas as pd
import os


def setup_logging(log_file='feature_engineering.log'):
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
    parser = argparse.ArgumentParser(description='Stock Feature Engineering Script')

    parser.add_argument('--input', type=str, default='data/stock_data_cleaned.csv', help='Path to cleaned input CSV')
    parser.add_argument('--output', type=str, default='data/stock_data_features.csv', help='Path for features output CSV')

    return parser.parse_args()


def engineer_features(input_path, output_path, logger):
    logger.info(f"Loading cleaned data from {input_path}")
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        sys.exit(1)

    logger.info(f"Initial rows: {len(df)}")
    df['DATE'] = pd.to_datetime(df['DATE'])

    # Group by TICKER for rolling features
    logger.info("Engineering features per TICKER")
    feature_frames = []

    for ticker, group in df.groupby('TICKER'):
        group = group.sort_values('DATE').copy()

        # 1-day return
        group['Return_1d'] = group['Price_Close'].pct_change()

        # Moving averages
        group['MA_5d'] = group['Price_Close'].rolling(window=5).mean()
        group['MA_10d'] = group['Price_Close'].rolling(window=10).mean()

        # Rolling volatility
        group['Volatility_10d'] = group['Return_1d'].rolling(window=10).std()

        feature_frames.append(group)

    # Combine all tickers
    final_df = pd.concat(feature_frames, ignore_index=True)

    # Drop rows with any NaNs (from rolling windows)
    final_df = final_df.dropna()

    logger.info(f"Rows after feature engineering and dropping NaNs: {len(final_df)}")

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)
    logger.info(f"Features saved to {output_path}")


def main():
    logger = setup_logging()
    args = parse_args()
    logger.info(f"Arguments parsed: {args}")

    engineer_features(args.input, args.output, logger)


if __name__ == "__main__":
    main()
