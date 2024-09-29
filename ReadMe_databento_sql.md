# Data Bento to LEAN Data Conversion Script

This project provides a Python script that automates the process of downloading OHLCV (Open, High, Low, Close, Volume) data from the Data Bento API, storing it in a PostgreSQL database, and converting it into a format compatible with the LEAN backtesting engine by QuantConnect.

## Overview

The script performs the following tasks:

1. **Data Retrieval:**
   - Checks if the requested data for a specific ticker and date range already exists in the PostgreSQL database (`databento_ohlcv` schema).
   - If data is missing or outdated, it fetches the required data from the Data Bento API.
   - New data is merged with existing data in the database to ensure completeness.

2. **Database Storage:**
   - Stores the OHLCV data in PostgreSQL with proper data types, including timezone-aware timestamps.
   - Ensures data integrity by handling duplicates and sorting based on event timestamps.

3. **Data Conversion:**
   - Retrieves the data from PostgreSQL for the specified date range.
   - Converts the data into the LEAN data format, which includes:
     - Converting prices to **deci-cents** (prices multiplied by 10,000 and cast to integers).
     - Formatting timestamps to match LEAN's expected date format (`YYYYMMDD HH:MM` in New York time).
   - Saves the converted data as compressed CSV files in the appropriate directory structure required by LEAN.

## Key Features

- **Timezone Handling:**
  - All timestamps (`ts_event`) are managed as timezone-aware datetime objects in UTC.
  - Date comparisons and sorting are performed consistently in UTC to prevent errors.
  - When converting to LEAN format, timestamps are converted to `America/New_York` timezone to match exchange time.

- **Database Interaction:**
  - Utilizes SQLAlchemy for database connections and operations.
  - Stores timestamps in PostgreSQL as `TIMESTAMP WITH TIME ZONE` to preserve timezone information.
  - Handles data type conversions to ensure compatibility with PostgreSQL (e.g., converting `uint64` to `int64` or `float64`).

- **Data Integrity:**
  - Merges new and existing data while removing duplicates based on the `ts_event` timestamp.
  - Sorts data by `ts_event` to maintain chronological order.
  - Ensures that all data uploaded to the database and used in conversions includes the necessary columns.

- **LEAN Format Compliance:**
  - Generates data files that adhere to LEAN's data format requirements.
  - Organizes output files into the correct directory structure (`data/equity/usa/daily/`).
  - Compresses the output CSV files into ZIP archives for efficient storage and use with LEAN.

## Usage

1. **Environment Setup:**
   - Install required Python packages: `databento`, `pandas`, `numpy`, `sqlalchemy`, etc.
   - Set environment variables for database and API access:
     - `databento_api_key`
     - `pguser`, `pgpass`, `pghost`

2. **Running the Script:**
   - Specify the tickers and date ranges you wish to process.
   - Run the script using a Python interpreter.
   - Example:
     ```python
     if __name__ == '__main__':
         ticker_list = ['QQQ']
         for ticker in ticker_list:
             download_and_append_data(ticker, '2023-09-01', '2023-12-31', frequency='daily')
     ```

3. **Output:**
   - The script will output compressed CSV files in the `data/equity/usa/daily/` directory.
   - These files are ready for use with the LEAN backtesting engine.

## Important Notes

- **Timezones:**
  - Input `start_date` and `end_date` are assumed to be in `America/New_York` timezone.
  - All internal processing converts dates to UTC for consistency.
  - When interacting with external APIs and databases, ensure that datetime objects are timezone-aware.

- **Data Types:**
  - Be cautious of data types when uploading to PostgreSQL. Unsigned integers (`uint64`) are converted to signed integers (`int64`) or floats to maintain compatibility.

- **Error Handling:**
  - The script includes error handling to catch and report issues during data retrieval, database operations, and data conversion.
  - Ensure that exceptions are monitored and resolved as needed.

- **Dependencies:**
  - The script relies on several external libraries. Ensure they are installed and up to date.
  - The `databento` library requires an API key, which must be set as an environment variable.

## Project Structure

- **Main Script:** Contains the core functions for data retrieval, database interaction, and data conversion.
- **Functions:**
  - `get_data_from_databento()`: Fetches data from the Data Bento API.
  - `upload_to_postgresql()`: Uploads data to PostgreSQL.
  - `get_existing_dates_from_postgresql()`: Retrieves existing dates from the database to check for missing data.
  - `get_data_from_postgresql()`: Fetches data from the database for conversion.
  - `convert_to_lean_format()`: Converts data to LEAN-compatible format.
  - `download_and_append_data()`: Orchestrates the overall process for a given ticker and date range.

## Reminders

- **Environment Variables:** Ensure all required environment variables are set before running the script.
- **Database Access:** Confirm that the PostgreSQL database is accessible and that the user has the necessary permissions.
- **Data Integrity:** Regularly check the data in the database and output files to ensure accuracy.
- **LEAN Compatibility:** Verify that the output data files are correctly recognized by the LEAN engine.

## Conclusion

This script automates the end-to-end process of acquiring, storing, and preparing financial data for backtesting with LEAN. By handling timezone complexities and data type conversions, it ensures data integrity and consistency throughout the workflow.