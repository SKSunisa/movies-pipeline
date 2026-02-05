{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_genres
-- PURPOSE: Genre lookup dimension
-- INPUT: {{ ref('stg_movies_enriched') }}
-- OUTPUT: Unique list of genres with surrogate keys
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_enriched') }}
),

-- STEP 1: Split genres_raw into individual genres
split_genres as (
    select
        movie_id,
        trim(genre.value) as genre_name
    from movies,
    lateral flatten(split(genres_raw, '|')) as genre
    -- LATERAL FLATTEN: Split "Action|Crime|Drama" → 3 rows
    -- SPLIT: Split by "|"
    -- FLATTEN: Convert array → rows
    where genres_raw is not null
),

-- STEP 2: Get unique genres
unique_genres as (
    select distinct
        genre_name
    from split_genres
    where genre_name is not null
      and trim(genre_name) != ''
),

-- STEP 3: Add surrogate key
final as (
    select
        row_number() over (order by genre_name) as genre_id,
        -- Surrogate key: 1, 2, 3, ...
        
        genre_name,
        -- Genre name: Action, Drama, Sci-Fi, etc.
        
        current_timestamp() as dim_created_at
        
    from unique_genres
)

select * from final
order by genre_name

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - ~20 rows (unique genres)
-- - genre_id: 1, 2, 3, ...
-- - genre_name: Action, Adventure, Animation, ...
-- ==============================================================================