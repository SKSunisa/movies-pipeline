{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: bridge_movie_language
-- PURPOSE: Many-to-Many bridge between movies and languages
-- INPUT: {{ ref('dim_movies') }}, {{ ref('dim_languages') }}
-- OUTPUT: Movie-Language relationships
-- =============================================================================

with movies as (
    select
        movie_id,
        language_list       
    from {{ ref('dim_movies') }}
),

languages as (
    select
        language_id,
        language_name
    from {{ ref('dim_languages') }}
),

-- Split languages for each movie
split_languages as (
    select
        movie_id,
        trim(language.value) as language_name
    from movies,
    lateral flatten(split(language_list, '|')) as language    
    where language_list is not null                           
),

-- Join with dim_languages to get language_id
final as (
    select
        sl.movie_id,
        l.language_id,
        sl.language_name,
        current_timestamp() as bridge_created_at
    from split_languages sl
    inner join languages l                  
        on sl.language_name = l.language_name
    where sl.language_name is not null
      and trim(sl.language_name) != ''
)

select * from final
order by movie_id, language_id

-- ==============================================================================
-- EXPECTED OUTPUT:
-- 103 rows (movies × languages)
-- - One row per movie-language combination
-- 
-- EXAMPLE:
-- movie_id | language_id | language_name
-- ---------|-------------|------------------
-- 7        | 8           | English
-- 7        | 15          | German
-- 7        | 32          | Polish
-- ==============================================================================