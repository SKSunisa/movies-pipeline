{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: bridge_movie_genre
-- PURPOSE: Many-to-Many bridge between movies and genres
-- INPUT: {{ ref('dim_movies') }}, {{ ref('dim_genres') }}
-- OUTPUT: Movie-Genre relationships
-- =============================================================================

with movies as (
    select
        movie_id,
        genres_raw
    from {{ ref('dim_movies') }}
),

genres as (
    select
        genre_id,
        genre_name
    from {{ ref('dim_genres') }}
),

-- Split genres for each movie
split_genres as (
    select
        movie_id,
        trim(genre.value) as genre_name
    from movies,
    lateral flatten(split(genres_raw, '|')) as genre
),

-- Join with dim_genres to get genre_id
final as (
    select
        sg.movie_id,
        g.genre_id,
        sg.genre_name,
        current_timestamp() as bridge_created_at
    from split_genres sg
    join genres g
        on sg.genre_name = g.genre_name
    where sg.genre_name is not null
      and trim(sg.genre_name) != ''
)

select * from final
order by movie_id, genre_id

-- ==============================================================================
-- OUTPUT:
-- 238 rows (movies × genres)
-- - One row per movie-genre combination
--
-- EXAMPLE:
-- movie_id | genre_id | genre_name
-- ---------|----------|------------
-- 1        | 5        | Crime
-- 1        | 8        | Drama
-- 2        | 5        | Crime
-- 2        | 8        | Drama
-- ==============================================================================
