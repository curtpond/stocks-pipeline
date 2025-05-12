with source as (
    select * from {{ source('market_data', 'GDP_DATA') }}
),

renamed as (
    select
        DATE as gdp_date,
        GDP as gdp_value,
        LOAD_TIMESTAMP
    from source
)

select * from renamed
