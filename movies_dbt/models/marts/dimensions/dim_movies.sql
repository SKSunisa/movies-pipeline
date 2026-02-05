{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_movies
-- PURPOSE: Movie dimension with all attributes
-- INPUT: {{ ref('stg_movies_enriched') }}
-- OUTPUT: Complete movie information ready for fact tables
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_enriched') }}
),

final as (
    select
        -- ======================================================================
        -- PRIMARY KEY
        -- ======================================================================
        movie_id,
        
        -- ======================================================================
        -- MOVIE ATTRIBUTES
        -- ======================================================================
        movie_title,
        release_year,
        decade,
        era,
        
        -- ======================================================================
        -- RATINGS
        -- ======================================================================
        imdb_rating,
        rating_category,
        rotten_tomatoes_pct,
        metacritic_score,
        
        -- ======================================================================
        -- MOVIE DETAILS
        -- ======================================================================
        runtime_mins,
        runtime_category,
        oscars_won,
        oscar_category,
        box_office_millions,
        box_office_category,
        
        -- ======================================================================
        -- SINGLE-VALUE TEXT FIELDS (from enriched)
        -- Note: These have 'Unknown' for NULL values
        -- ======================================================================
        director as primary_director,
        -- Note: This is the COALESCE(director_raw, 'Unknown') from enriched
        -- For multiple directors, will need to go back to stg_movies_cleaned
        
        country as primary_country,
        -- Note: This is the COALESCE(country_raw, 'Unknown') from enriched
        
        language as primary_language,
        -- Note: This is the COALESCE(language_raw, 'Unknown') from enriched
        
        -- ======================================================================
        -- MULTI-VALUE FIELDS (Raw - for bridge tables in Phase 11)
        -- These will be used for splitting into bridge tables
        -- ======================================================================
        genres_raw,
        -- Example: "Action|Crime|Drama"
        
        actors_raw,
        -- Example: "Christian Bale|Heath Ledger"
        
        country_list,
        -- Note: This is country_raw from stg_movies_cleaned
        -- Example: "United States|United Kingdom"
        
        language_list,
        -- Note: This is language_raw from stg_movies_cleaned
        -- Example: "English|German|Polish"
        
        -- ======================================================================
        -- AUDIT FIELDS
        -- ======================================================================
        loaded_at,
        dbt_updated_at,
        current_timestamp() as dim_created_at
        
    from movies
)

select * from final
order by movie_id

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - 95 rows (all movies from stg_movies_enriched)
-- - Ready for fact table joins
-- - Contains both single values (primary_*) and raw multi-values (*_list, *_raw)
-- ==============================================================================