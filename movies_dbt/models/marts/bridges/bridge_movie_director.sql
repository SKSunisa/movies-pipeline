{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: bridge_movie_director
-- PURPOSE: Many-to-Many bridge between movies and directors
-- INPUT: {{ ref('stg_movies_cleaned') }}, {{ ref('dim_directors') }}
-- OUTPUT: Movie-Director relationships
-- NOTE: Must use stg_movies_cleaned to access director_raw
-- =============================================================================

with movies as (
    select
        movie_id,
        director_raw        
    from {{ ref('stg_movies_cleaned') }}    
),

directors as (
    select
        director_id,
        director_name
    from {{ ref('dim_directors') }}
),

-- Split directors for each movie (some movies have multiple directors)
split_directors as (
    select
        movie_id,
        trim(director.value) as director_name
    from movies,
    lateral flatten(split(director_raw, '|')) as director
    where director_raw is not null
),

-- Join with dim_directors to get director_id
final as (
    select
        sd.movie_id,
        d.director_id,
        sd.director_name,
        current_timestamp() as bridge_created_at
    from split_directors sd
    inner join directors d
        on trim(lower(sd.director_name)) = trim(lower(d.director_name))
    where sd.director_name is not null
      and trim(sd.director_name) != ''
)

select * from final
order by movie_id, director_id

-- ==============================================================================
-- OUTPUT:
-- 100 rows (most movies have 1 director, some have 2)
-- - One row per movie-director combination
-- 
-- EXAMPLE:
-- movie_id | director_id | director_name
-- ---------|-------------|---------------------
-- 3        | 23          | Christopher Nolan
-- 65       | 35          | Joel Coen
-- 65       | 36          | Ethan Coen
-- ==============================================================================