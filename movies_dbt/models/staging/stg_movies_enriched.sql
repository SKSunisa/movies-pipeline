{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- =============================================================================
-- MODEL: stg_movies_enriched
-- PURPOSE: Layer 2 - Handle NULL values and create business logic columns
-- INPUT: {{ ref('stg_movies_cleaned') }}
-- OUTPUT: Ready-to-use data with defaults and derived categories
-- ROWS: 95 (same as input)
-- =============================================================================

with base as (
    -- ==========================================================================
    -- CTE 1: BASE
    -- Retrieve data from stg_movies_cleaned
    -- ==========================================================================
    select * from {{ ref('stg_movies_cleaned') }}
),

enriched as (
    select
        -- ======================================================================
        -- PRIMARY KEY & CORE INFO (as-is)
        -- ======================================================================
        movie_id,
        movie_title,
        release_year,
        
        -- ======================================================================
        -- TEXT FIELDS - Replace NULL with 'Unknown'
        -- Why: Makes JOIN and GROUP BY operations easier
        -- ======================================================================
        coalesce(director_raw, 'Unknown') as director,
        -- Example: NULL → "Unknown"
        -- Example: "Christopher Nolan" → "Christopher Nolan"
        
        coalesce(country_raw, 'Unknown') as country,
        -- Example: NULL → "Unknown"
        
        coalesce(language_raw, 'Unknown') as language,
        -- Example: NULL → "Unknown"
        
        -- ======================================================================
        -- MULTI-VALUE FIELDS - Keep _raw for later splitting
        -- Store raw values for splitting in Phase 10 (dimensional models)
        -- ======================================================================
        genres_raw,
        actors_raw,
        country_raw as country_list,      -- Keep raw for splitting in Phase 10
        language_raw as language_list,    -- Keep raw for splitting in Phase 10
        
        -- ======================================================================
        -- NUMERIC FIELDS - Replace NULL with 0 (where appropriate)
        -- ======================================================================
        coalesce(runtime_mins, 0) as runtime_mins,
        -- Example: NULL → 0
        -- Why: If runtime is unknown, treat as 0
        
        coalesce(oscars_won, 0) as oscars_won,
        -- Example: NULL → 0
        -- Why: No data = didn't win any Oscars
        
        -- ======================================================================
        -- RATINGS - Keep NULL (no data ≠ score of 0)
        -- ======================================================================
        imdb_rating,
        -- NULL = no data available
        -- 0 = terrible score
        -- Must distinguish between the two!
        
        rotten_tomatoes_pct,
        -- 3% missing → Keep NULL
        
        metacritic_score,
        -- 50% missing → Keep NULL
        
        -- ======================================================================
        -- FINANCIAL - Replace NULL with 0
        -- ======================================================================
        coalesce(box_office_millions, 0) as box_office_millions,
        -- Example: NULL → 0
        -- Why: 0 = no box office data (not the same as losing money!)
        
        -- ======================================================================
        -- DERIVED COLUMNS - Business Categories
        -- ======================================================================
        
        -- Rating Category (based on IMDb score)
        case 
            when imdb_rating >= 9.0 then 'Masterpiece'
            when imdb_rating >= 8.5 then 'Excellent'
            when imdb_rating >= 8.0 then 'Very Good'
            when imdb_rating >= 7.5 then 'Good'
            when imdb_rating is not null then 'Average'
            else 'No Rating'
        end as rating_category,
        -- Examples:
        -- - 9.3 (Shawshank) → "Masterpiece"
        -- - 8.8 (Inception) → "Excellent"
        -- - 8.1 (Oldboy) → "Very Good"
        -- - 7.7 (Social Network) → "Good"
        
        -- Box Office Category
        case 
            when box_office_millions >= 1000 then 'Blockbuster'
            when box_office_millions >= 500 then 'Major Hit'
            when box_office_millions >= 100 then 'Hit'
            when box_office_millions > 0 then 'Modest'
            else 'Unknown'
        end as box_office_category,
        -- Examples:
        -- - 1119.9 (LOTR: ROTK) → "Blockbuster"
        -- - 678.2 (Forrest Gump) → "Major Hit"
        -- - 322.1 (Schindler's List) → "Hit"
        -- - 58 (Shawshank) → "Modest"
        
        -- Awards Category (based on Oscar wins)
        case 
            when oscars_won >= 5 then 'Oscar Winner (5+)'
            when oscars_won >= 3 then 'Oscar Winner (3-4)'
            when oscars_won >= 1 then 'Oscar Winner (1-2)'
            else 'No Oscar'
        end as oscar_category,
        -- Examples:
        -- - 11 (LOTR: ROTK) → "Oscar Winner (5+)"
        -- - 7 (Schindler's List) → "Oscar Winner (5+)"
        -- - 4 (Inception) → "Oscar Winner (3-4)"
        -- - 2 (Dark Knight) → "Oscar Winner (1-2)"
        -- - 0 (Shawshank) → "No Oscar"
        
        -- Decade
        floor(release_year / 10) * 10 as decade,
        -- Examples: 
        -- - 1994 → 1990
        -- - 2008 → 2000
        -- - 1957 → 1950
        
        -- Era Classification
        case 
            when release_year >= 2010 then 'Modern Era (2010s+)'
            when release_year >= 2000 then '2000s'
            when release_year >= 1990 then '1990s'
            when release_year >= 1980 then '1980s'
            when release_year >= 1970 then '1970s'
            when release_year >= 1960 then '1960s'
            when release_year >= 1950 then '1950s'
            when release_year >= 1940 then '1940s'
            when release_year >= 1930 then '1930s'
            else 'Classic Era (<1930)'
        end as era,
        -- Examples:
        -- - 2019 (Parasite) → "Modern Era (2010s+)"
        -- - 2001 (LOTR) → "2000s"
        -- - 1994 (Shawshank) → "1990s"
        -- - 1972 (Godfather) → "1970s"
        -- - 1957 (12 Angry Men) → "1950s"
        
        -- Runtime Category
        case 
            when runtime_mins >= 180 then 'Epic (3+ hours)'
            when runtime_mins >= 150 then 'Long (2.5-3 hours)'
            when runtime_mins >= 120 then 'Standard (2-2.5 hours)'
            when runtime_mins >= 90 then 'Short (1.5-2 hours)'
            when runtime_mins > 0 then 'Very Short (<1.5 hours)'
            else 'Unknown'
        end as runtime_category,
        -- Examples:
        -- - 222 mins (Lawrence of Arabia) → "Epic (3+ hours)"
        -- - 195 mins (Schindler's List) → "Long (2.5-3 hours)"
        -- - 142 mins (Shawshank) → "Standard (2-2.5 hours)"
        -- - 96 mins (12 Angry Men) → "Short (1.5-2 hours)"
        
        -- ======================================================================
        -- METADATA
        -- ======================================================================
        loaded_at,
        dbt_updated_at
        
    from base
)

-- ==============================================================================
-- FINAL OUTPUT
-- ==============================================================================
select * from enriched

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - 95 rows (same as input from stg_movies_cleaned)
-- - NULL values handled (replaced with defaults where appropriate)
-- - 6 new derived columns:
--   1. rating_category      → "Masterpiece", "Excellent", etc.
--   2. box_office_category  → "Blockbuster", "Hit", etc.
--   3. oscar_category       → "Oscar Winner (5+)", etc.
--   4. decade               → 1930, 1940, ..., 2010
--   5. era                  → "Modern Era", "2000s", etc.
--   6. runtime_category     → "Epic", "Long", etc.
-- - Ready for dimensional modeling in Phase 10
-- ==============================================================================