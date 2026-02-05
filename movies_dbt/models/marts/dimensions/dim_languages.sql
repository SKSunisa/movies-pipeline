{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_languages
-- PURPOSE: Language lookup dimension
-- INPUT: {{ ref('stg_movies_enriched') }}
-- OUTPUT: Unique list of languages with surrogate keys
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_enriched') }}
),

-- STEP 1: Split languages (pipe-separated)
split_languages as (
    select
        movie_id,
        trim(language.value) as language_name
    from movies,
    lateral flatten(split(language_list, '|')) as language
    -- Note: stg_movies_enriched has "language_list" (not language_raw)
    -- Split "English|German|Polish" → 3 rows
    where language_list is not null
),

-- STEP 2: Get unique languages
unique_languages as (
    select distinct
        language_name
    from split_languages
    where language_name is not null
      and trim(language_name) != ''
),

-- STEP 3: Add surrogate key
final as (
    select
        row_number() over (order by language_name) as language_id,
        language_name,
        current_timestamp() as dim_created_at
    from unique_languages
)

select * from final
order by language_name

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - ~10-15 rows (unique languages)
-- - language_id: 1, 2, 3, ...
-- - language_name: English, French, German, Italian, Japanese, ...
-- ==============================================================================