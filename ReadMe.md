# LEAN External Data

This repository contains scripts and notebooks for handling external data feeds for the LEAN algorithmic trading engine.

## Notebooks

### `databento_test.ipynb`
This Jupyter notebook is designed to test and demonstrate the functionality of the Databento API. It includes examples of how to fetch and process data using the API.

### `lean_converter.ipynb`
This Jupyter notebook is designed to convert data fetched from the Databento API into a format that is compatible with LEAN. It includes examples of how to clean and format the data for use in the algorithmic trading engine.

## Scripts
### `databento_pipe.py`
This script is designed to download stock data from the Data Bento API, convert it to a format compatible with the LEAN Local CLI Framework, and save it in the appropriate directory. The script includes functions to:
1. Fetch OHLCV data for a given ticker and date range from Data Bento.
2. Convert datetime columns from UTC to New York time.
3. Convert the downloaded data into a format compatible with LEAN.
4. Download and append stock data if necessary, then convert the data to QuantConnect format.
Functions:
    - get_data_from_databento(ticker, start_date, end_date): Fetches OHLCV data for a given ticker from Data Bento for the specified date range.
    - convert_utc_to_ny(df, datetime_column='ts_event'): Converts a datetime column from UTC to New York time (Eastern Time).
    - convert_to_lean_format(csv_file, ticker, frequency='daily'): Converts a CSV file containing stock data into a format compatible with LEAN Local CLI Framework.
    - download_and_append_data(ticker, start_date, end_date, folder='databento/downloads', frequency='daily'): Downloads and appends stock data from Data Bento API if necessary, then converts the data to QuantConnect format.
Example usage:
    - The script can be run directly to download and process data for a list of tickers within a specified date range.

### `databento_sql.py`
For an explanation of the script, please refer to the databento_sql_ReadMe file

## Directories

### Data Storage
The data fetched by `databento_test.ipynb` is saved in the following directories:
- `/databento/downloads/{ticker}.csv`: Contains raw data files fetched directly from the Databento API.
- `/data/equity/usa/daily/{ticker}.csv`: Contains processed data files that have been cleaned and formatted for use in LEAN.

## Environment Variables

### `.env` File
The `.env` file should contain the following variable:
- `databento_api_key`: Your Databento API key for accessing the data.

Ensure that this file is properly configured before running the notebook to avoid authentication issues.

## Getting Started

1. Clone the repository.
2. Install the required dependencies.
3. Set up the `.env` file with your Databento API key.
4. Run `databento_test.ipynb` to fetch and process data.

For more detailed instructions, refer to the comments within the notebook.

## Upcoming Improvements:
- [In Progress] WRDS API Support - Pending new WRDS SQL Query
- Add support for equity options, fx and futures data.
- Add support for different types of equity data (e.g. higher resolution, fundamental data, etc.)
- Add support for further databento formats


# Notes 
Last update: 26SEP24
Daily data is working using databento_test.ipynb
To do next is to convert to a .py file, add the questdb integration and test it
Finally dagsterize this pipeline

# Usage Instructions
After running databento_test on your desired ticker
Drop the ticker.zip into /data/equity/usa/daily/
Run the following command in the terminal
```lean backtest "localdatatest_spy" --data-provider-historical Local```