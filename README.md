# stocks-pipeline

Ingest and store financial stock data for time series modeling

## Overview

This project fetches financial data, including tech stock prices and GDP data, loads it into Snowflake, and transforms it using dbt for analysis. The pipeline handles data ingestion, transformation, and storage in a structured format.

## Features

- Fetches tech stock data from Alpha Vantage API
- Retrieves GDP data
- Loads data into Snowflake database
- Transforms data using dbt for analysis
  - Calculates daily returns and moving averages
  - Combines stock metrics with GDP data
  - Implements data quality tests
- Handles data transformations (date formatting, column standardization)
- Secure credential management through environment variables

## Prerequisites

- Python 3.11+
- Virtual environment (`stocks/`)
- Snowflake account
- Alpha Vantage API key
- Prefect server (for pipeline orchestration)

## Installation

1. Clone the repository

2. Create and activate the virtual environment:

   ```bash
   python -m venv stocks
   source stocks/bin/activate
   pip install -r requirements.txt
   ```

## Configuration

1. Create a `.env` file in the project root:

   ```bash
   # Alpha Vantage API Key
   ALPHA_VANTAGE_API_KEY=your_api_key

   # Snowflake Connection Parameters
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_DATABASE=STOCKS_DB
   SNOWFLAKE_SCHEMA=MARKET_DATA
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   ```

## Project Structure

- `scripts/`
  - `fetch_stock_data.py`: Fetches stock data from Alpha Vantage
  - `snowflake_loader.py`: Handles data loading into Snowflake
  - `tech_analysis.py`: Performs technical analysis on stock data
  - `test_endpoints.py`: Tests API endpoints
- `flows/`
  - `stock_pipeline.py`: Main Prefect pipeline orchestration
  - `deployment.py`: Prefect deployment configuration
- `SQL/`
  - `setup_snowflake.sql`: SQL scripts for Snowflake setup and configuration
- `stocks_transformations/` (dbt project)
  - `models/`
    - `staging/`: Initial data cleaning and standardization
    - `intermediate/`: Calculated metrics and indicators
    - `mart/`: Final analytical models
  - `dbt_project.yml`: dbt project configuration
- `.env`: Configuration file for credentials

## Data Schema

### Source Tables

#### TECH_STOCK_DATA Table

- SYMBOL (VARCHAR)
- DATE (DATE)
- OPEN (FLOAT)
- HIGH (FLOAT)
- LOW (FLOAT)
- CLOSE (FLOAT)
- VOLUME (INTEGER)
- RSI (FLOAT)

#### GDP_DATA Table

- DATE (DATE)
- GDP (FLOAT)
- LOAD_TIMESTAMP (TIMESTAMP)

### dbt Transformations

#### Staging Models

##### Tech Stocks Model

- Cleaned and standardized tech stock data
- Renamed columns for clarity
- Added data quality tests

##### GDP Model

- Cleaned GDP data
- Standardized date formats
- Added data quality tests

#### Intermediate Models

##### Stock Metrics

- Daily returns calculation
- Technical indicators:
  - 7-day moving average
  - 30-day moving average
  - 7-day volume moving average
- RSI (Relative Strength Index)

#### Mart Models

##### Market Analysis

- Combined stock metrics with GDP data
- Quarterly GDP alignment with daily stock data
- Complete market analysis view

## Usage

1. Fetch stock data:

   ```bash
   python scripts/fetch_stock_data.py
   ```

2. Load data into Snowflake:

   ```bash
   python scripts/snowflake_loader.py
   ```

3. Transform data with dbt:

   ```bash
   cd stocks_transformations
   dbt deps     # Install dependencies
   dbt debug    # Verify connection
   dbt run      # Run all transformations
   dbt test     # Run data quality tests
   ```

4. View dbt documentation:

   ```bash
   cd stocks_transformations
   dbt docs generate  # Generate documentation
   dbt docs serve     # Start documentation server at http://localhost:8080
   ```

### dbt Commands Reference

Here are the most commonly used dbt commands:

- `dbt run` - Execute all transformations
- `dbt run --models staging` - Run only staging models
- `dbt run --models mart.market_analysis` - Run specific model
- `dbt test` - Run all tests
- `dbt build` - Run and test all models
- `dbt docs generate` - Generate documentation
- `dbt docs serve` - Serve documentation locally

### Pipeline Orchestration

The data pipeline is orchestrated using Prefect with the following components:

#### Tasks

1. `fetch_stock_data`: Retrieves data from Alpha Vantage API
2. `load_to_snowflake`: Loads raw data into Snowflake
3. `run_dbt_transformations`: Executes dbt models and tests

Each task is designed to be idempotent and includes error handling for robustness.

#### Pipeline Schedule

The pipeline is configured to run automatically with the following schedule:

- **Frequency**: Daily at midnight
- **Monitoring**: Available through Prefect UI at [http://127.0.0.1:4200](http://127.0.0.1:4200)
- **Logs**: Detailed execution logs for each task
- **Error Handling**: Automatic retries on failure

#### Running the Pipeline

1. Start Prefect server:

   ```bash
   prefect server start
   ```

2. Deploy the pipeline:

   ```bash
   cd flows
   python deployment.py
   ```

3. Start a worker (in a new terminal):

   ```bash
   prefect worker start -p default-agent-pool
   ```

4. Trigger a manual run (optional):

   ```bash
   prefect deployment run 'Stock Data Pipeline/stock-pipeline-daily'
   ```

#### Monitoring

1. Open Prefect UI at [http://127.0.0.1:4200](http://127.0.0.1:4200)
2. View:
   - Flow runs and their status
   - Task execution logs
   - Schedule information
   - Error details if any

3. Start a worker:

   ```bash
   prefect worker start -p default-agent-pool
   ```

The pipeline is scheduled to run daily at midnight, automatically fetching new data and updating the transformations.

## Security Notes

- The `.env` file is excluded from version control
- SSL verification is configurable for Snowflake connections
- Credentials are stored securely in environment variables

## Last Updated

2025-05-11
