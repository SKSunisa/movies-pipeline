-- =============================================================================
-- TEST: assert_rating_consistency
-- PURPOSE: Verify is_masterpiece flag is consistent with IMDb rating
-- =============================================================================

SELECT
    movie_id,
    movie_title,
    imdb_rating,
    is_masterpiece,
    'Rating >= 9.0 but flag = 0' as issue
FROM {{ ref('fact_movie_performance') }}
WHERE imdb_rating >= 9.0 AND is_masterpiece = 0

UNION ALL

SELECT
    movie_id,
    movie_title,
    imdb_rating,
    is_masterpiece,
    'Rating < 9.0 but flag = 1' as issue
FROM {{ ref('fact_movie_performance') }}
WHERE (imdb_rating < 9.0 OR imdb_rating IS NULL) AND is_masterpiece = 1