import databento as db
import pandas as pd
from datetime import datetime

# Initialize Data Bento client
client = db.Historical("db-hLqXJHUbgC3mVyJ9T6mEjVwgN57f4")

# Example tickers and date range
tickers = ['AAPL', 'GOOG', 'MSFT']
start_date = '2023-01-01'
end_date = '2023-09-01'

for ticker in tickers:
    dataset = client.timeseries.get_range(
        dataset="equity-usa",
        symbols=ticker,
        start_date=start_date,
        end_date=end_date,
        schema='ohlcv'
    )
    dataset.to_csv(f'{ticker}_data.csv')

def convert_to_lean_format(csv_file, ticker, frequency='daily'):
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    
    if frequency == 'daily':
        output_file = f"data/equity/usa/daily/{ticker}.csv"
    elif frequency == 'hourly':
        output_file = f"data/equity/usa/hourly/{ticker}.csv"
    else:
        output_file = f"data/equity/usa/minute/{ticker}.zip"
    
    df.to_csv(output_file, index=False)

# Example conversion
convert_to_lean_format('AAPL_data.csv', 'AAPL', frequency='daily')


def download_and_convert(tickers, start_date, end_date, frequency='daily'):
    for ticker in tickers:
        # Download data from Data Bento
        dataset = client.timeseries.get_range(
            dataset="equity-usa",
            symbols=ticker,
            start_date=start_date,
            end_date=end_date,
            schema='ohlcv'
        )
        csv_file = f'{ticker}_data.csv'
        dataset.to_csv(csv_file)

        # Convert to QuantConnect format
        convert_to_lean_format(csv_file, ticker, frequency)

# Example ticker list and date range
ticker_list = ['AAPL', 'GOOG', 'MSFT']
download_and_convert(ticker_list, '2023-01-01', '2023-09-01', 'daily')
