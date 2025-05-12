import os
import requests
import json

def test_endpoint(function, **additional_params):
    """Test an Alpha Vantage endpoint"""
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    base_url = 'https://www.alphavantage.co/query'
    
    # Base parameters
    params = {
        'function': function,
        'apikey': api_key,
        **additional_params
    }
    
    print(f"\nTesting endpoint: {function}")
    print(f"Parameters: {json.dumps(params, indent=2)}")
    
    response = requests.get(base_url, params=params)
    data = response.json()
    
    # Check for error messages
    if "Error Message" in data:
        print(f"Error: {data['Error Message']}")
    elif "Note" in data:
        print(f"Note: {data['Note']}")
    else:
        print("Success! First few keys in response:")
        print(list(data.keys())[:3])
    
    return data

# Test each endpoint
def main():
    # Test TIME_SERIES_DAILY_ADJUSTED
    test_endpoint(
        'TIME_SERIES_DAILY_ADJUSTED',
        symbol='AAPL',
        outputsize='compact'
    )
    
    # Test RSI
    test_endpoint(
        'RSI',
        symbol='AAPL',
        interval='daily',
        time_period='14',
        series_type='close'
    )
    
    # Test MACD
    test_endpoint(
        'MACD',
        symbol='AAPL',
        interval='daily',
        series_type='close'
    )
    
    # Test REAL_GDP
    test_endpoint(
        'REAL_GDP',
        interval='quarterly'
    )

if __name__ == "__main__":
    main()
