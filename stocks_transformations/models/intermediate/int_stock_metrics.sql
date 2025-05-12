with stock_data as (
    select * from {{ ref('stg_tech_stocks') }}
),

metrics as (
    select
        *,
        (closing_price - lag(closing_price) over (partition by symbol order by date)) / lag(closing_price) over (partition by symbol order by date) as daily_return,
        avg(closing_price) over (partition by symbol order by date rows between 7 preceding and current row) as ma_7_day,
        avg(closing_price) over (partition by symbol order by date rows between 30 preceding and current row) as ma_30_day,
        avg(volume) over (partition by symbol order by date rows between 7 preceding and current row) as volume_ma_7_day
    from stock_data
)

select * from metrics
