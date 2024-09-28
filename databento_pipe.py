import databento as db
import pandas as pd
from datetime import datetime
import zipfile
import os

''' Python Script to download data form data bento and convert it to LEAN format 
    Requires manual saving/moving to the lean data directory within Algos folder
    
    To add:
    Overwrite if file exists logic - Take from the medium article tbh - TBD 28SEP24
    '''

def convert_to_lean_format(csv_file, ticker, frequency='daily'):
    '''
    Converts a CSV file containing stock data into a format compatible with LEAN Local CLI Framework.
    Args:
        csv_file (str): The path to the input CSV file containing stock data.
        ticker (str): The stock ticker symbol.
        frequency (str, optional): The frequency of the data. Can be 'daily', 'hourly', or 'minute'. Defaults to 'daily'.
    Returns:
        None: The function saves the converted data to a file in the appropriate directory based on the frequency. Located within project directory.
    '''
    
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df.pop('ts_event')).dt.strftime('%Y%m%d %H:%M')
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    
    # Conversion from dollars to deci-cents
    df[['open', 'high', 'low', 'close']] = (df[['open', 'high', 'low', 'close']] * 10000).astype(int)
    
    # Data type sorting for directory saving for csv
    if frequency == 'daily':
        output_dir = 'data/equity/usa/daily/'
        output_file = f"{output_dir}{ticker.lower()}.csv"
    elif frequency == 'hourly':
        output_dir = 'data/equity/usa/hourly/'
        output_file = f"{output_dir}{ticker.lower()}.csv"
    else:
        output_dir = 'data/equity/usa/minute/'
        output_file = f"{output_dir}{(ticker.lower())}.csv"
    
    df.to_csv(output_file, index=False, header=False)
    
    # Zip File
    zip_file = os.path.join(output_dir, f'{ticker.lower()}.zip')
    #zip_file = os.path.join("./", f'{ticker.lower()}.zip')
    #zip_fileblank = f'{ticker.lower()}.zip'
    # Create a zip file and add the csv file to it
    with zipfile.ZipFile(zip_file, 'w') as zf:
        zf.write(output_file, arcname=f'{ticker.lower()}.csv')

    # Optionally, remove the csv file after zipping (if you want the zip to be the only output)
    #os.remove(csv_file)

    print(f"{output_file} has been successfully zipped into {zip_file}.")
    
def download_and_convert(tickers, start_date, end_date, frequency='daily'):
    '''
    Downloads stock data from Data Bento and converts it to a format compatible with the LEAN Local CLI Framework.
    Parameters:
    tickers (list of str): List of stock ticker symbols to download data for.
    start_date (str): The start date for the data range in 'YYYY-MM-DD' format.
    end_date (str): The end date for the data range in 'YYYY-MM-DD' format.
    frequency (str, optional): The frequency of the data. Default is 'daily'.
    Returns:
    None
    '''

    # Initialize Data Bento client
    client = db.Historical(os.getenv('databento_api_key'))
    for ticker in tickers:
        # CSV File variable
        csv_file = f'databento/downloads/{ticker}_data.csv'
        
        # Remove the existing file if it exists
        if os.path.exists(csv_file):
            # open the file and get the dates
            os.remove(csv_file)
        
        dataset = client.timeseries.get_range(
            dataset='XNAS.ITCH',
            symbols=ticker,
            start=start_date,
            end=end_date,
            schema='ohlcv-1d'
        )
        csv_file = f'databento/downloads/{ticker}_data.csv'
        dataset.to_csv(csv_file)

        # Convert to QuantConnect format
        convert_to_lean_format(csv_file, ticker, frequency)

# Example ticker list and date range
if __name__ == '__main__':
    ticker_list = ['QQQ']
    download_and_convert(ticker_list, '2024-06-01', '2024-09-01', 'daily')
