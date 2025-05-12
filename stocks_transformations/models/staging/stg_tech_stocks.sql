with source as (
    select * from {{ source('market_data', 'TECH_STOCK_DATA') }}
),

renamed as (
    select
        SYMBOL,
        DATE,
        OPEN as opening_price,
        HIGH as high_price,
        LOW as low_price,
        CLOSE as closing_price,
        VOLUME,
        RSI as relative_strength_index
    from source
)

select * from renamed
