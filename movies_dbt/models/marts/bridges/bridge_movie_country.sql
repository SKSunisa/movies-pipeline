{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: bridge_movie_country
-- PURPOSE: Many-to-Many bridge between movies and countries
-- INPUT: {{ ref('dim_movies') }}, {{ ref('dim_countries') }}
-- OUTPUT: Movie-Country relationships
-- =============================================================================

with movies as (
    select
        movie_id,
        country_list
    from {{ ref('dim_movies') }}    
),

countries as (
    select
        country_id,
        country_name
    from {{ ref('dim_countries') }}
),

-- Split countries for each movie
split_countries as (
    select
        movie_id,
        trim(country.value) as country_name
    from movies,
    lateral flatten(split(country_list, '|')) as country
    where country_list is not null
),

-- Join with dim_countries to get country_id
final as (
    select
        sc.movie_id,
        c.country_id,
        sc.country_name,
        current_timestamp() as bridge_created_at
    from split_countries sc
    inner join countries c
        on sc.country_name = c.country_name
    where sc.country_name is not null
      and trim(sc.country_name) != ''
)

select * from final
order by movie_id, country_id

-- ==============================================================================
-- OUTPUT:
-- 126 rows (movies × countries)
-- - One row per movie-country combination
-- 
-- EXAMPLE:
-- movie_id | country_id | country_name
-- ---------|------------|-------------------
-- 3        | 45         | United States
-- 3        | 46         | United Kingdom
-- ==============================================================================
