import os
import requests
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlphaVantageAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://www.alphavantage.co/query'
        
    def fetch_daily_stock_data(self, symbol, output_size='full'):
        """
        Fetch daily stock data for a given symbol.
        output_size: 'full' for 20+ years of data, 'compact' for last 100 days
        """
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'outputsize': output_size,
                'apikey': self.api_key
            }
            
            logger.info(f"Fetching daily stock data for {symbol}")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if "Error Message" in data:
                raise ValueError(f"API Error: {data['Error Message']}")
                
            # Extract time series data
            time_series = data.get('Time Series (Daily)')
            if not time_series:
                raise ValueError("No time series data found in response")
                
            # Convert to DataFrame
            df = pd.DataFrame.from_dict(time_series, orient='index')
            
            # Clean up column names
            df.columns = [col.split('. ')[1] for col in df.columns]
            
            # Convert string values to float
            for col in df.columns:
                df[col] = pd.to_numeric(df[col])
                
            # Add date as a column
            df['date'] = pd.to_datetime(df.index)
            df.reset_index(drop=True, inplace=True)
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error occurred: {e}")
            raise
        except ValueError as e:
            logger.error(f"Data processing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

def save_to_csv(df, symbol, output_dir='data'):
    """Save DataFrame to CSV file with timestamp"""
    # Create data directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{symbol}_daily_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Save to CSV
    df.to_csv(filepath, index=False)
    logger.info(f"Data saved to {filepath}")
    return filepath

def main():
    # Get API key from environment variable
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("Please set ALPHA_VANTAGE_API_KEY environment variable")
    
    # Initialize API client
    client = AlphaVantageAPI(api_key)
    
    # Example usage - fetch AAPL stock data
    symbol = 'AAPL'  # Can be modified for different stocks
    try:
        df = client.fetch_daily_stock_data(symbol)
        filepath = save_to_csv(df, symbol)
        logger.info(f"Successfully processed {symbol} stock data")
        logger.info(f"Total records: {len(df)}")
    except Exception as e:
        logger.error(f"Failed to process {symbol} stock data: {e}")
        raise

if __name__ == "__main__":
    main()
