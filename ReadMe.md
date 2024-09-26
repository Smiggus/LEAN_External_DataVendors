# LEAN External Data

This repository contains scripts and notebooks for handling external data feeds for the LEAN algorithmic trading engine.

## Notebooks

### `databento_test.ipynb`
This Jupyter notebook is designed to test and demonstrate the functionality of the Databento API. It includes examples of how to fetch and process data using the API.

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
- Add support for more data feeds and APIs. (WRDS is next)
- Add support for equity options, fx and futures data.
- Add support for different types of equity data (e.g. higher resolution, fundamental data, etc.)
- Add support for further databento formats

Last update: 26SEP24
