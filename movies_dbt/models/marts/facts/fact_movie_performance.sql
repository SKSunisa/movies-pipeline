{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: fact_movie_performance
-- PURPOSE: Fact table with movie performance metrics
-- GRAIN: One row per movie
-- INPUT: {{ ref('dim_movies') }}, {{ ref('dim_time') }}
-- OUTPUT: Movie performance facts
-- =============================================================================

with movies as (
    select * from {{ ref('dim_movies') }}
),

time_dim as (
    select * from {{ ref('dim_time') }}
),

final as (
    select
        -- ======================================================================
        -- FOREIGN KEYS (Dimension References)
        -- ======================================================================
        m.movie_id,
        -- FK to dim_movies

        m.release_year as time_id,
        -- FK to dim_time (year-based)

        -- ======================================================================
        -- DEGENERATE DIMENSIONS (Attributes stored in fact)
        -- ======================================================================
        m.movie_title,
        -- Usually dimension, but included for convenience

        -- ======================================================================
        -- METRICS: RATINGS
        -- ======================================================================
        m.imdb_rating,
        -- IMDb score (0-10)
        -- NULL if not available

        m.rotten_tomatoes_pct,
        -- Rotten Tomatoes (0-100%)

        m.metacritic_score,
        -- Metacritic (0-100)

        -- Average rating (normalized to 0-100 scale)
        case
            when m.imdb_rating is not null
             and m.rotten_tomatoes_pct is not null
             and m.metacritic_score is not null
            then (
                (m.imdb_rating * 10) +
                m.rotten_tomatoes_pct +
                m.metacritic_score
            ) / 3.0
            else null
        end as avg_rating_normalized,
        -- Average of all 3 ratings (on 0-100 scale)

        -- ======================================================================
        -- METRICS: PERFORMANCE
        -- ======================================================================
        m.box_office_millions,                    
        -- Box office revenue (millions USD)
        -- NULL if not available (~17% missing)

        m.oscars_won,
        -- Number of Academy Awards won

        m.runtime_mins,
        -- Runtime in minutes

        -- ======================================================================
        -- METRICS: DERIVED
        -- ======================================================================

        -- Revenue per minute
        case
            when m.runtime_mins > 0 and m.box_office_millions is not null   
            then m.box_office_millions / m.runtime_mins                     
            else null
        end as revenue_per_minute,

        -- Box office per Oscar
        case
            when m.oscars_won > 0 and m.box_office_millions is not null     
            then m.box_office_millions / m.oscars_won                       
            else null
        end as box_office_per_oscar,

        -- Rating quality indicator (0-3: how many ratings available)
        (
            case when m.imdb_rating is not null then 1 else 0 end +
            case when m.rotten_tomatoes_pct is not null then 1 else 0 end +
            case when m.metacritic_score is not null then 1 else 0 end
        ) as rating_sources_count,

        -- ======================================================================
        -- FLAGS (Semi-additive facts)
        -- ======================================================================
        case when m.imdb_rating >= 9.0 then 1 else 0 end as is_masterpiece,
        case when m.box_office_millions >= 1000 then 1 else 0 end as is_blockbuster,  
        case when m.oscars_won > 0 then 1 else 0 end as has_oscars,

        -- ======================================================================
        -- DATA QUALITY SCORE (คำนวณใหม่)
        -- ======================================================================
        -- Calculate data completeness on the fly
        round(
            (
                case when m.imdb_rating is not null then 20 else 0 end +
                case when m.rotten_tomatoes_pct is not null then 20 else 0 end +
                case when m.metacritic_score is not null then 20 else 0 end +
                case when m.box_office_millions is not null then 20 else 0 end +  
                case when m.runtime_mins is not null then 20 else 0 end
            ),
            2
        ) as data_quality_score,
        -- Calculate the quality score based on the completeness of the information (0-100)
        -- 5 fields × 20 points each = 100 max

        -- ======================================================================
        -- AUDIT
        -- ======================================================================
        current_timestamp() as fact_created_at

    from movies m
    left join time_dim t
        on m.release_year = t.time_id
)

select * from final
order by movie_id

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - 95 rows (one per movie)
-- - Grain: movie_id
-- - All numeric metrics for analysis
-- ==============================================================================

-- ==============================================================================
-- USAGE EXAMPLES:
--
-- -- Total box office by decade
-- SELECT
--     t.decade,
--     SUM(f.box_office_millions) as total_box_office,
--     AVG(f.imdb_rating) as avg_rating,
--     SUM(f.oscars_won) as total_oscars
-- FROM fact_movie_performance f
-- JOIN dim_time t ON f.time_id = t.time_id
-- GROUP BY t.decade
-- ORDER BY t.decade;
--
-- -- Top 10 movies by box office
-- SELECT
--     m.movie_title,
--     f.box_office_millions,
--     f.imdb_rating,
--     f.oscars_won
-- FROM fact_movie_performance f
-- JOIN dim_movies m ON f.movie_id = m.movie_id
-- ORDER BY f.box_office_millions DESC
-- LIMIT 10;
-- ==============================================================================