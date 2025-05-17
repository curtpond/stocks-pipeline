{{config(
    materialized = 'table',
    schema = 'MARKET_DATA'
)}}

select
    SYMBOL,
    DATE,
    OPEN as opening_price,
    HIGH as high_price,
    LOW as low_price,
    CLOSE as closing_price,
    VOLUME,
    RSI as relative_strength_index
from STOCKS_DB.MARKET_DATA.TECH_STOCK_DATA
