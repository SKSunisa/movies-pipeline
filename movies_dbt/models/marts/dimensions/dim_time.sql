{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_time
-- PURPOSE: Time dimension (year-based for movies)
-- INPUT: {{ ref('stg_movies_enriched') }}
-- OUTPUT: Year-level time dimension
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_enriched') }}
),

-- Get unique years
unique_years as (
    select distinct
        release_year as year
    from movies
    where release_year is not null
),

-- Add time attributes
final as (
    select
        year as time_id,
        -- PK: Year itself (1940, 1950, ...)
        
        year,
        -- Full year: 1994, 2010
        
        floor(year / 10) * 10 as decade,
        -- Decade: 1990, 2000, 2010
        
        case 
            when year < 1960 then 'Classic Era'
            when year < 1980 then 'New Hollywood Era'
            when year < 2000 then 'Blockbuster Era'
            else 'Modern Era'
        end as era,
        -- Movie era
        
        case 
            when year < 2000 then '20th Century'
            else '21st Century'
        end as century,
        -- Century
        
        -- Additional time attributes
        case 
            when year < 1950 then 'Pre-1950s'
            when year < 1960 then '1950s'
            when year < 1970 then '1960s'
            when year < 1980 then '1970s'
            when year < 1990 then '1980s'
            when year < 2000 then '1990s'
            when year < 2010 then '2000s'
            else '2010s+'
        end as decade_label,
        
        current_timestamp() as dim_created_at
        
    from unique_years
)

select * from final
order by year

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - ~70 rows (one per unique year: 1931-2019)
-- - time_id = year
-- - decade, era, century attributes
-- ==============================================================================