import os
import glob
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from datetime import datetime
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SnowflakeLoader:
    def __init__(self):
        """Initialize Snowflake connection using environment variables"""
        load_dotenv()  # Load environment variables from .env file
        
        # Required Snowflake connection parameters
        self.account = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = os.getenv('SNOWFLAKE_USER')
        self.password = os.getenv('SNOWFLAKE_PASSWORD')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        
        # Validate required parameters
        required_params = {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'schema': self.schema,
            'warehouse': self.warehouse
        }
        
        missing_params = [k for k, v in required_params.items() if not v]
        if missing_params:
            raise ValueError(f"Missing required Snowflake parameters: {', '.join(missing_params)}")
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            # Set connection parameters
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                ocsp_response_cache_filename=None,  # Disable OCSP cache
                validate_default_parameters=True,  # Validate connection parameters
                protocol='https',  # Force HTTPS
                insecure_mode=True  # Temporarily disable SSL verification for testing
            )
            logger.info("Successfully connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise

    def create_tables(self, conn):
        """Create required tables if they don't exist"""
        try:
            cursor = conn.cursor()
            
            # Create stock data table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS TECH_STOCK_DATA (
                SYMBOL VARCHAR(10),
                DATE DATE,
                OPEN FLOAT,
                HIGH FLOAT,
                LOW FLOAT,
                CLOSE FLOAT,
                VOLUME INTEGER,
                RSI FLOAT,
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (SYMBOL, DATE)
            )
            """)
            
            # Create GDP data table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS GDP_DATA (
                DATE DATE,
                GDP FLOAT,
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (DATE)
            )
            """)
            
            logger.info("Tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
        finally:
            cursor.close()

    def load_tech_stock_data(self, conn, file_path):
        """Load tech stock data from CSV into Snowflake"""
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Convert column names to uppercase
            df.columns = df.columns.str.upper()
            
            # Convert date column to datetime and format it as YYYY-MM-DD
            df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d')
            
            # Write to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name='TECH_STOCK_DATA',
                database=self.database,
                schema=self.schema
            )
            
            logger.info(f"Loaded {nrows} rows from {file_path}")
            return nrows
            
        except Exception as e:
            logger.error(f"Error loading tech stock data: {e}")
            raise

    def load_gdp_data(self, conn, file_path):
        """Load GDP data from CSV into Snowflake"""
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Convert column names to uppercase
            df.columns = df.columns.str.upper()
            
            # Convert date column to datetime and format it as YYYY-MM-DD
            df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d')
            
            # Write to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name='GDP_DATA',
                database=self.database,
                schema=self.schema
            )
            
            logger.info(f"Loaded {nrows} rows from {file_path}")
            return nrows
            
        except Exception as e:
            logger.error(f"Error loading GDP data: {e}")
            raise

def main():
    try:
        # Initialize loader
        loader = SnowflakeLoader()
        
        # Connect to Snowflake
        conn = loader.connect()
        
        # Create tables
        loader.create_tables(conn)
        
        # Load tech sector analysis
        tech_analysis_file = max(glob.glob('tech_analysis/tech_sector_analysis_*.csv'))
        loader.load_tech_stock_data(conn, tech_analysis_file)
        
        # Load GDP data
        gdp_file = max(glob.glob('tech_analysis/gdp_data_*.csv'))
        loader.load_gdp_data(conn, gdp_file)
        
        logger.info("Data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
