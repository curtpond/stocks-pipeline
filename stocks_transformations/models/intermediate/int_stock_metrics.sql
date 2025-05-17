{{config(
    materialized = 'table',
    schema = 'MARKET_DATA'
)}}

select
    *,
    (closing_price - lag(closing_price) over (partition by symbol order by date)) / lag(closing_price) over (partition by symbol order by date) as daily_return,
    avg(closing_price) over (partition by symbol order by date rows between 7 preceding and current row) as ma_7_day,
    avg(closing_price) over (partition by symbol order by date rows between 30 preceding and current row) as ma_30_day,
    avg(volume) over (partition by symbol order by date rows between 7 preceding and current row) as volume_ma_7_day
from STOCKS_DB.MARKET_DATA.STG_TECH_STOCKS
