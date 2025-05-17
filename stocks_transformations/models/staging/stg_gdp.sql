{{config(
    materialized = 'table',
    schema = 'MARKET_DATA'
)}}

select
    DATE as gdp_date,
    GDP as gdp_value,
    LOAD_TIMESTAMP
from STOCKS_DB.MARKET_DATA.GDP_DATA
