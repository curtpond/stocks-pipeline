# stocks-pipeline
Ingest and store financial stock data for time series modeling

## Overview
This project fetches financial data, including tech stock prices and GDP data, and loads it into Snowflake for analysis. The pipeline handles data ingestion, transformation, and storage in a structured format.

## Features
- Fetches tech stock data from Alpha Vantage API
- Retrieves GDP data
- Loads data into Snowflake database
- Handles data transformations (date formatting, column standardization)
- Secure credential management through environment variables

## Prerequisites
- Python 3.11+
- Virtual environment (`stocks/`)
- Snowflake account
- Alpha Vantage API key

## Installation
1. Clone the repository
2. Create and activate the virtual environment:
   ```bash
   python -m venv stocks
   source stocks/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install snowflake-connector-python[pandas]
   pip install pandas
   ```

## Configuration
Create a `.env` file with the following variables:
```env
ALPHA_VANTAGE_API_KEY=your_api_key
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
```

## Project Structure
- `fetch_stock_data.py`: Fetches stock data from Alpha Vantage
- `snowflake_loader.py`: Handles data loading into Snowflake
- `.env`: Configuration file for credentials

## Data Schema
### TECH_STOCK_DATA Table
- SYMBOL (VARCHAR)
- DATE (DATE)
- OPEN (FLOAT)
- HIGH (FLOAT)
- LOW (FLOAT)
- CLOSE (FLOAT)
- VOLUME (INTEGER)
- RSI (FLOAT)

### GDP_DATA Table
- DATE (DATE)
- GDP (FLOAT)
- LOAD_TIMESTAMP (TIMESTAMP)

## Usage
1. Fetch stock data:
   ```bash
   python fetch_stock_data.py
   ```
2. Load data into Snowflake:
   ```bash
   python snowflake_loader.py
   ```

## Security Notes
- The `.env` file is excluded from version control
- SSL verification is configurable for Snowflake connections
- Sensitive credentials are never hardcoded

## Last Updated
2025-05-11
