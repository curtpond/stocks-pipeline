with stock_metrics as (
    select * from {{ ref('int_stock_metrics') }}
),

gdp_data as (
    select * from {{ ref('stg_gdp') }}
),

final as (
    select
        sm.*,
        gd.gdp_value as latest_gdp
    from stock_metrics sm
    left join gdp_data gd
        on date_trunc('quarter', sm.date) = date_trunc('quarter', gd.gdp_date)
)

select * from final
