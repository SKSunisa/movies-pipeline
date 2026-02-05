{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_countries
-- PURPOSE: Country lookup dimension
-- INPUT: {{ ref('stg_movies_enriched') }}
-- OUTPUT: Unique list of countries with surrogate keys
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_enriched') }}
),

-- STEP 1: Split countries (pipe-separated)
split_countries as (
    select
        movie_id,
        trim(country.value) as country_name
    from movies,
    lateral flatten(split(country_list, '|')) as country
    -- Note: stg_movies_enriched has "country_list" (not country_raw)
    -- Split "United States|United Kingdom" → 2 rows
    where country_list is not null
),

-- STEP 2: Get unique countries
unique_countries as (
    select distinct
        country_name
    from split_countries
    where country_name is not null
      and trim(country_name) != ''
),

-- STEP 3: Add surrogate key
final as (
    select
        row_number() over (order by country_name) as country_id,
        country_name,
        current_timestamp() as dim_created_at
    from unique_countries
)

select * from final
order by country_name

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - ~15-20 rows (unique countries)
-- - country_id: 1, 2, 3, ...
-- - country_name: Brazil, France, Italy, Japan, United States, ...
-- ==============================================================================