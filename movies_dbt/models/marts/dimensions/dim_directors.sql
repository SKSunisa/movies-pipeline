{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_directors
-- PURPOSE: Director lookup dimension
-- INPUT: {{ ref('stg_movies_cleaned') }}  -- ใช้ cleaned เพราะต้องใช้ director_raw
-- OUTPUT: Unique list of directors with surrogate keys
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_cleaned') }}
),

-- STEP 1: Split directors (some movies have multiple directors)
split_directors as (
    select
        movie_id,
        trim(director.value) as director_name
    from movies,
    lateral flatten(split(director_raw, '|')) as director
    -- Split directors separated by "|"
    -- Example: "Joel Coen|Ethan Coen" → 2 rows
    where director_raw is not null
),

-- STEP 2: Get unique directors
unique_directors as (
    select distinct
        director_name
    from split_directors
    where director_name is not null
      and trim(director_name) != ''
),

-- STEP 3: Add surrogate key
final as (
    select
        row_number() over (order by director_name) as director_id,
        director_name,
        current_timestamp() as dim_created_at
    from unique_directors
)

select * from final
order by director_name

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - ~80-90 rows (unique directors)
-- - director_id: 1, 2, 3, ...
-- - director_name: Alfred Hitchcock, Christopher Nolan, ...
-- ==============================================================================