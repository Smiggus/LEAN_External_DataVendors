import databento as db
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import BigInteger, DateTime, Float
import zipfile
from sqlalchemy.types import TIMESTAMP
import pytz

''' 
Python Script to download data from Data Bento and convert it to LEAN format.
If the data already exists, then it will be fetched from the PostgreSQL database instead of databento
'''

def get_data_from_databento(ticker, start_date, end_date):
    """
    Fetches OHLCV data for a given ticker from Data Bento for the specified date range.
    """
    client = db.Historical(os.getenv('databento_api_key'))
    dataset = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        symbols=ticker,
        start=start_date.strftime('%Y-%m-%d'),
        end=end_date.strftime('%Y-%m-%d'),
        schema='ohlcv-1d'
    )
    df = dataset.to_df()

    # Identify uint64 columns. If a number is a decimal then convert to float64.
    uint64_cols = df.select_dtypes(include=['uint64']).columns.tolist()
    if uint64_cols:
        print(f"Converting uint64 columns to int64: {uint64_cols}")
        # Ensure values are within int64 range
        for col in uint64_cols:
            if df[col].max() > np.iinfo('int64').max:
                print(f"Warning: Values in column {col} exceed int64 range. Converting to float.")
                df[col] = df[col].astype('float64')
            else:
                df[col] = df[col].astype('int64')

    # Ensure ts_event is not the index
    if df.index.name == 'ts_event':
        df.reset_index(inplace=True)

    return df

def upload_to_postgresql(df, ticker, schema='databento_ohlcv'):
    """
    Uploads a DataFrame to a PostgreSQL database using SQLAlchemy.
    Ensures that ts_event is included as a standard column.
    """
    # Ensure ts_event is a column and not the index
    if 'ts_event' not in df.columns:
        df.reset_index(inplace=True)

    # Fetch credentials from environment variables
    pguser = os.getenv('pguser')
    pgpass = os.getenv('pgpass')
    pghost = os.getenv('pghost')

    # Database connection URL using environment variables
    db_url = f'postgresql://{pguser}:{pgpass}@{pghost}/FinancialData'
    engine = create_engine(db_url)

    try:
        # Define data types for SQL columns
        dtype = {
            'ts_event': TIMESTAMP(timezone=True),
            'open': Float(),
            'high': Float(),
            'low': Float(),
            'close': Float(),
            'volume': BigInteger(),
            # Add other columns if necessary
        }

        # Write the DataFrame to the PostgreSQL table
        df.to_sql(ticker, engine, schema=schema, if_exists='replace', index=False, dtype=dtype)
        print(f"Data for {ticker} uploaded successfully to {schema}.{ticker}.")
    except SQLAlchemyError as e:
        print(f"Error uploading data for {ticker} to PostgreSQL: {e}")
    finally:
        engine.dispose()

def get_existing_dates_from_postgresql(ticker, schema='databento_ohlcv'):
    """
    Retrieves the existing dates for a given ticker from PostgreSQL database.
    """
    # Fetch credentials from environment variables
    pguser = os.getenv('pguser')
    pgpass = os.getenv('pgpass')
    pghost = os.getenv('pghost')

    # Database connection URL using environment variables
    db_url = f'postgresql://{pguser}:{pgpass}@{pghost}/FinancialData'
    engine = create_engine(db_url)

    try:
        # Check if table exists
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema=schema)
        if ticker in tables:
            # Table exists, retrieve existing dates
            query = f'SELECT DISTINCT ts_event::date FROM "{schema}"."{ticker}"'
            df_existing = pd.read_sql(query, con=engine)
            df_existing['ts_event'] = pd.to_datetime(df_existing['ts_event'],utc=True)
            dates = pd.DatetimeIndex(df_existing['ts_event'].sort_values())
            return dates
        else:
            return None
    except Exception as e:
        print(f"Error retrieving existing dates for {ticker}: {e}")
        return None
    finally:
        engine.dispose()

def get_data_from_postgresql(ticker, start_date=None, end_date=None, schema='databento_ohlcv'):
    """
    Retrieves data for a given ticker from PostgreSQL database, optionally within a date range.
    """
    # Fetch credentials from environment variables
    pguser = os.getenv('pguser')
    pgpass = os.getenv('pgpass')
    pghost = os.getenv('pghost')

    # Database connection URL using environment variables
    db_url = f'postgresql://{pguser}:{pgpass}@{pghost}/FinancialData'
    engine = create_engine(db_url)

    try:
        # Build the query
        query = f'SELECT * FROM "{schema}"."{ticker}"'
        if start_date is not None and end_date is not None:
            # Convert date string to datetime object
            if not isinstance(start_date, datetime):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if not isinstance(end_date, datetime):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            query += f" WHERE ts_event BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'"
        df = pd.read_sql(query, con=engine)
        
        # Ensure ts_event is parsed as timezone-aware datetime
        df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)
        
        return df
    except Exception as e:
        print(f"Error retrieving data for {ticker} from PostgreSQL: {e}")
        return None
    finally:
        engine.dispose()

def convert_to_lean_format(df, ticker, frequency='daily'):
    # Convert 'ts_event' to America/New_York timezone and required date format
    df['date'] = df['ts_event'].dt.tz_convert('America/New_York').dt.strftime('%Y%m%d %H:%M')

    # Select required columns without copying
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]

    # Use .loc for assignment to avoid the warning
    df.loc[:, ['open', 'high', 'low', 'close']] = (df[['open', 'high', 'low', 'close']] * 10000).astype(int)

    # Data type sorting for directory saving for csv
    if frequency == 'daily':
        output_dir = 'data/equity/usa/daily/'
        output_file = f"{output_dir}{ticker.lower()}.csv"
    elif frequency == 'hourly':
        output_dir = 'data/equity/usa/hourly/'
        output_file = f"{output_dir}{ticker.lower()}.csv"
    else:
        output_dir = 'data/equity/usa/minute/'
        output_file = f"{output_dir}{ticker.lower()}.csv"

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    df.to_csv(output_file, index=False, header=False)

    # Zip the CSV file
    zip_file = os.path.join(output_dir, f'{ticker.lower()}.zip')
    with zipfile.ZipFile(zip_file, 'w') as zf:
        zf.write(output_file, arcname=f'{ticker.lower()}.csv')

    # Optionally, remove the csv file after zipping (if you want the zip to be the only output)
    # os.remove(output_file)

    print(f"{output_file} has been successfully zipped into {zip_file}.")

def download_and_append_data(ticker, start_date, end_date, frequency='daily'):
    """
    Downloads and appends stock data from Data Bento API if necessary, then converts the data to LEAN format.
    """
    # Convert start_date and end_date to datetime objects
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Localize the datetime to NY time (exchange time zone)
    start_date = start_date.tz_localize('America/New_York').astimezone(pytz.utc)
    end_date = end_date.tz_localize('America/New_York').astimezone(pytz.utc)

    # Fetch existing dates from PostgreSQL
    dates = get_existing_dates_from_postgresql(ticker)
    
    # Ensure dates are in UTC
    if dates is not None:
        dates = dates.tz_convert('UTC')
    
    # If dates is None or date range is not covered
    if dates is None or start_date.date() < dates[0].date() or end_date.date() > dates[-1].date():
        print(f'Fetching data for {ticker} from {start_date.date()} to {end_date.date()}')

        # Attempt to fetch new data from Data Bento
        try:
            # Add buffer (delta) to the date range to handle overlaps
            delta = timedelta(days=3)
            df_new = get_data_from_databento(ticker, start_date - delta, end_date + delta)

            # Ensure ts_event is not set as index
            if df_new.index.name == 'ts_event':
                df_new.reset_index(inplace=True)

            # If data exists in the database, append new data
            if dates is not None:
                # Fetch existing data
                df_existing = get_data_from_postgresql(ticker)
                df_existing['ts_event'] = pd.to_datetime(df_existing['ts_event'], utc=True)
                # Ensure ts_event is not set as index
                if df_existing.index.name == 'ts_event':
                    df_existing.reset_index(inplace=True)

                # Combine data and remove duplicates based on ts_event
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined.drop_duplicates(subset='ts_event', keep='last', inplace=True)
                df_combined['ts_event'] = pd.to_datetime(df_combined['ts_event'], utc=True)
                df_combined.sort_values(by='ts_event', inplace=True)
                # Upload the amended data to PostgreSQL
                upload_to_postgresql(df_combined, ticker)
            else:
                # Upload the new data to PostgreSQL
                upload_to_postgresql(df_new, ticker)
            print(f'Ticker {ticker} data updated in PostgreSQL')

        except Exception as e:
            print(f'Error fetching data for {ticker}: {e}')
            return None
    else:
        print(f'Ticker {ticker} already up-to-date')

    # Fetch the data from PostgreSQL for the required date range
    df = get_data_from_postgresql(ticker, start_date, end_date)
    if df is not None and not df.empty:
        # Ensure ts_event is not set as index
        if df.index.name == 'ts_event':
            df.reset_index(inplace=True)

        # Ensure ts_event is timezone-aware
        df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)

        # Convert to LEAN format
        convert_to_lean_format(df, ticker, frequency)
        print(f'Data for {ticker} fetched from postgres, converted to LEAN format.')
    else:
        print(f"No data available for {ticker} to convert.")
        return None

    return ticker

# Example ticker list and date range
if __name__ == '__main__':
    ticker_list = ['QQQ']
    for ticker in ticker_list:
        download_and_append_data(ticker, '2023-09-01', '2023-12-31', frequency='daily')
