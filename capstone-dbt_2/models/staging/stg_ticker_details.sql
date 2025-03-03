{{ config(
    materialized = 'table'
) }}

WITH source AS (
    SELECT
        ticker,
        COALESCE(company_name, 'Unknown') AS company_name,
        COALESCE(sector, 'Unknown') AS sector,
        COALESCE(industry, 'Unknown') AS industry,
        {{ dbt_utils.generate_surrogate_key(['ticker']) }} AS ticker_id
    FROM {{ source('bigchungus0148', 'ticker_details') }}
)

SELECT * FROM source
