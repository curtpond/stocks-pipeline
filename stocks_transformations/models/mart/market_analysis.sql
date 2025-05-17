{{config(
    materialized = 'table',
    schema = 'MARKET_DATA'
)}}

select
    sm.*,
    gd.gdp_value as latest_gdp
from STOCKS_DB.MARKET_DATA.INT_STOCK_METRICS sm
left join STOCKS_DB.MARKET_DATA.STG_GDP gd
    on date_trunc('quarter', sm.date) = date_trunc('quarter', gd.gdp_date)
