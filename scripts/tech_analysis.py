import os
import requests
import pandas as pd
import time
from datetime import datetime
import logging
from typing import List, Dict, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TechAnalysis:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = 'https://www.alphavantage.co/query'
        # Define tech companies to analyze
        self.tech_symbols = [
            'AAPL',  # Apple
            'MSFT',  # Microsoft
            'GOOGL', # Alphabet
            'AMZN',  # Amazon
            'META',  # Meta
            'NVDA',  # NVIDIA
            'TSLA',  # Tesla
            'AMD',   # AMD
            'INTC',  # Intel
            'CRM'    # Salesforce
        ]
        
    def _make_api_request(self, params: Dict) -> Dict:
        """Make API request with rate limiting"""
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            # Check for API error messages
            data = response.json()
            if "Error Message" in data:
                raise ValueError(f"API Error: {data['Error Message']}")
                
            # Alpha Vantage has a rate limit of 5 calls per minute for free tier
            time.sleep(12)  # Wait 12 seconds between calls to stay within limits
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error occurred: {e}")
            raise
            
    def get_daily(self, symbol: str) -> pd.DataFrame:
        """Fetch daily time series data"""
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': 'compact',  # Last 100 data points
            'apikey': self.api_key
        }
        
        logger.info(f"Fetching daily adjusted data for {symbol}")
        data = self._make_api_request(params)
        
        # Extract time series data
        time_series = data.get('Time Series (Daily)')
        if not time_series:
            raise ValueError("No time series data found in response")
            
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.columns = [col.split('. ')[1] for col in df.columns]
        
        # Convert string values to float
        for col in df.columns:
            df[col] = pd.to_numeric(df[col])
            
        df['date'] = pd.to_datetime(df.index)
        df.reset_index(drop=True, inplace=True)
        return df
        
    def get_rsi(self, symbol: str, interval: str = 'daily', time_period: int = 14) -> pd.DataFrame:
        """Fetch RSI (Relative Strength Index) data"""
        params = {
            'function': 'RSI',
            'symbol': symbol,
            'interval': interval,
            'time_period': time_period,
            'series_type': 'close',
            'apikey': self.api_key
        }
        
        logger.info(f"Fetching RSI data for {symbol}")
        data = self._make_api_request(params)
        
        # Extract technical indicator data
        technical_data = data.get('Technical Analysis: RSI')
        if not technical_data:
            raise ValueError("No RSI data found in response")
            
        df = pd.DataFrame.from_dict(technical_data, orient='index')
        df.columns = ['RSI']
        df['RSI'] = pd.to_numeric(df['RSI'])
        df['date'] = pd.to_datetime(df.index)
        df.reset_index(drop=True, inplace=True)
        return df
        
    def get_macd(self, symbol: str, interval: str = 'daily') -> pd.DataFrame:
        """Fetch MACD (Moving Average Convergence/Divergence) data"""
        params = {
            'function': 'MACD',
            'symbol': symbol,
            'interval': interval,
            'series_type': 'close',
            'apikey': self.api_key
        }
        
        logger.info(f"Fetching MACD data for {symbol}")
        data = self._make_api_request(params)
        
        # Extract technical indicator data
        technical_data = data.get('Technical Analysis: MACD')
        if not technical_data:
            raise ValueError("No MACD data found in response")
            
        df = pd.DataFrame.from_dict(technical_data, orient='index')
        df.columns = ['MACD', 'MACD_Hist', 'MACD_Signal']
        
        # Convert string values to float
        for col in df.columns:
            df[col] = pd.to_numeric(df[col])
            
        df['date'] = pd.to_datetime(df.index)
        df.reset_index(drop=True, inplace=True)
        return df
        
    def get_real_gdp(self) -> pd.DataFrame:
        """Fetch real GDP data"""
        params = {
            'function': 'REAL_GDP',
            'interval': 'quarterly',
            'apikey': self.api_key
        }
        
        logger.info("Fetching Real GDP data")
        data = self._make_api_request(params)
        
        # Extract data
        gdp_data = data.get('data')
        if not gdp_data:
            raise ValueError("No GDP data found in response")
            
        df = pd.DataFrame(gdp_data)
        df.columns = ['date', 'GDP']
        df['GDP'] = pd.to_numeric(df['GDP'])
        df['date'] = pd.to_datetime(df['date'])
        return df
        
    def combine_stock_data(self, symbol: str) -> pd.DataFrame:
        """Combine all data sources for a single stock"""
        try:
            # Fetch available data
            daily_df = self.get_daily(symbol)
            rsi_df = self.get_rsi(symbol)
            # Skip MACD as it's a premium feature
            
            # Merge dataframes on date
            combined_df = daily_df.merge(rsi_df, on='date', how='left')
            
            # Add symbol column
            combined_df['symbol'] = symbol
            
            return combined_df
            
        except Exception as e:
            logger.error(f"Error processing data for {symbol}: {e}")
            raise
            
    def analyze_tech_sector(self) -> None:
        """Analyze entire tech sector"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = 'tech_analysis'
        os.makedirs(output_dir, exist_ok=True)
        
        # Fetch GDP data once
        try:
            gdp_df = self.get_real_gdp()
            gdp_df.to_csv(f'{output_dir}/gdp_data_{timestamp}.csv', index=False)
            logger.info("Saved GDP data")
        except Exception as e:
            logger.error(f"Error fetching GDP data: {e}")
        
        # Process each tech stock
        all_stocks_data = []
        for symbol in self.tech_symbols:
            try:
                stock_df = self.combine_stock_data(symbol)
                all_stocks_data.append(stock_df)
                
                # Save individual stock data
                stock_df.to_csv(f'{output_dir}/{symbol}_analysis_{timestamp}.csv', index=False)
                logger.info(f"Saved analysis data for {symbol}")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue
        
        # Combine all stock data into one file
        if all_stocks_data:
            combined_df = pd.concat(all_stocks_data, ignore_index=True)
            combined_df.to_csv(f'{output_dir}/tech_sector_analysis_{timestamp}.csv', index=False)
            logger.info("Saved combined tech sector analysis")

def main():
    # Get API key from environment variable
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("Please set ALPHA_VANTAGE_API_KEY environment variable")
    
    # Initialize analysis
    analyzer = TechAnalysis(api_key)
    
    # Run tech sector analysis
    analyzer.analyze_tech_sector()

if __name__ == "__main__":
    main()
