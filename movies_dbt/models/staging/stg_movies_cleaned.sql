{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- =============================================================================
-- MODEL: stg_movies_cleaned
-- PURPOSE: Layer 1 - Basic data cleaning + Remove duplicates
-- INPUT: {{ source('raw', 'movies_raw') }}
-- OUTPUT: Clean columns, trimmed strings, NULL preserved, NO duplicates
-- ROWS: 95 (removed 5 duplicate pairs = 10 rows)
-- =============================================================================

with source as (
    -- ==========================================================================
    -- CTE 1: SOURCE
    -- Retrieve data from RAW table
    -- Use source() function to reference the source data
    -- ==========================================================================
    select * from {{ source('raw', 'movies_raw') }}
),

basic_cleaning as (
    -- ==========================================================================
    -- CTE 2: BASIC_CLEANING
    -- Perform 5 basic cleaning operations:
    -- 1. Rename columns → snake_case
    -- 2. Trim whitespace → remove extra spaces
    -- 3. Keep NULLs → don't convert to 0
    -- 4. Convert empty strings to NULL → use nullif()
    -- 5. Remove duplicates → use QUALIFY
    -- ==========================================================================
    select
        -- ======================================================================
        -- PRIMARY KEY
        -- ======================================================================
        rank as movie_id,
        -- Why: Use rank as PK because it's unique (1-100)
        -- rank 1 = The Shawshank Redemption
        -- rank 2 = The Godfather
        
        -- ======================================================================
        -- MOVIE INFORMATION
        -- ======================================================================
        trim(regexp_replace(title, ' \\(dup\\d*\\)', '')) as movie_title,
        -- Why regexp_replace + trim: Remove "(dup)", "(dup2)", "(dup3)" first, then trim spaces
        -- Pattern: \\(dup\\d*\\) = "(dup" + 0+ digits + ")"
        -- Example: "Rashomon (dup)" → "Rashomon"
        -- Example: "Paths of Glory (dup2)" → "Paths of Glory"
        -- Example: "  Inception  " → "Inception"
        -- Prevention: "Inception" ≠ " Inception "
        
        year as release_year,
        -- Rename to clarify this is the release year
        
        -- ======================================================================
        -- RATINGS & PERFORMANCE
        -- Keep NULL as-is (don't convert to 0)
        -- ======================================================================
        imdb_rating,
        -- NULL ≠ 0
        -- NULL = no data available
        -- 0 = score of 0 (terrible!)
        -- They're different! Must keep NULL
        
        rotten_tomatoes_pct,
        -- Why _pct: indicates it's a percentage (0-100)
        -- From "Rotten Tomatoes %" → rotten_tomatoes_pct
        
        metacritic_score,
        -- 50% missing! Will handle in stg_movies_enriched (STEP 2)
        
        -- ======================================================================
        -- MOVIE DETAILS 
        -- Keep NULL as-is
        -- ======================================================================
        runtime_mins,
        -- Why _mins: indicates unit is minutes
        -- From "Runtime (mins)" → runtime_mins
        
        oscars_won,
        -- From "Oscars Won" → oscars_won
        
        box_office_millions,
        -- Why _millions: indicates unit is millions of dollars
        -- From "Box Office ($M)" → box_office_millions
        
        -- ======================================================================
        -- MULTI-VALUE FIELDS
        -- Trim + Convert empty strings to NULL
        -- Don't split (will split in Phase 10)
        -- ======================================================================
        nullif(trim(genres), '') as genres_raw,
        -- Why nullif(): Convert empty string "" to NULL
        -- Why _raw: Indicates not yet split (still contains "|")
        -- Example: "Action|Crime|Drama" → Keep as-is
        -- Example: "" → NULL
        -- Will split in Phase 10 into:
        --   - Action
        --   - Crime
        --   - Drama
        
        nullif(trim(director), '') as director_raw,
        -- Example: "Christopher Nolan" → Trim spaces
        -- Example: "" → NULL
        
        nullif(trim(main_actors), '') as actors_raw,
        -- Example: "Christian Bale|Heath Ledger" → Not split yet
        -- Example: "" → NULL
        
        nullif(trim(country), '') as country_raw,
        -- Example: "United States|United Kingdom" → Not split yet
        -- Example: "" → NULL
        
        nullif(trim(language), '') as language_raw,
        -- Example: "English|German|Polish" → Not split yet
        -- Example: "" → NULL
        
        -- ======================================================================
        -- METADATA
        -- ======================================================================
        loaded_at,
        -- Keep to know when this data was loaded (from Phase 8)
        
        current_timestamp() as dbt_updated_at
        -- Record when dbt ran this transformation
        
    from source
    
    -- ==========================================================================
    -- DATA QUALITY FILTER
    -- Filter out invalid rows
    -- ==========================================================================
    where rank is not null      -- Why: rank = PK, must not be NULL
      and title is not null     -- Why: Every movie must have a title
      and year is not null      -- Why: Every movie must have a year
      
    -- ==========================================================================
    -- REMOVE DUPLICATES
    -- Use QUALIFY + ROW_NUMBER() to remove duplicates
    -- ==========================================================================
    qualify row_number() over (
        partition by trim(regexp_replace(title, ' \\(dup\\d*\\)', '')), year 
        order by rank
    ) = 1
    -- Explanation:
    -- - regexp_replace(title, ' \\(dup\\d*\\)', ''): Remove "(dup)", "(dup2)", "(dup3)"
    --   Pattern: \\(dup\\d*\\) = opening parenthesis + "dup" + 0+ digits + closing parenthesis
    --   "Rashomon (dup)" → "Rashomon"
    --   "Paths of Glory (dup2)" → "Paths of Glory"
    -- - trim(...): Remove leading/trailing spaces
    -- - partition by ..., year: Group by (clean movie title, year)
    --   Example: "Rashomon", 1950 → same group
    --   Example: "Rashomon (dup)", 1950 → same group
    --   Example: "Paths of Glory (dup2)", 1957 → same group
    -- - order by rank: Sort by rank ascending (best rank first)
    --   Example: rank 45 comes before rank 79
    -- - = 1: Keep only rank 1 of each group (best rank)
    --
    -- Duplicates removed (examples):
    -- 1. Rashomon (1950): Keep rank 45, Remove rank 79 "(dup)"
    -- 2. Paths of Glory (1957): Keep rank 76, Remove rank 92 "(dup2)"
    -- 3. The Bridge on the River Kwai (1957): Keep rank 73, Remove rank 97
    -- 4. The Third Man (1949): Keep rank 47, Remove rank 71 "(dup)"
    -- 5. The Great Dictator (1940): Keep rank 43, Remove rank 75 "(dup)"
    --
    -- Result: 100 rows → 95 rows (removed 5 duplicate pairs = 10 rows)
)

-- ==============================================================================
-- FINAL OUTPUT
-- ==============================================================================
select * from basic_cleaning

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - 95 rows (removed 5 duplicate pairs = 10 rows)
-- - Columns in snake_case
-- - No excess whitespace
-- - Empty strings converted to NULL
-- - NULL values preserved (not converted to 0)
-- - No duplicates
-- ==============================================================================