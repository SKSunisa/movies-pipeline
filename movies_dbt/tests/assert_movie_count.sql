-- =============================================================================
-- TEST: assert_movie_count
-- PURPOSE: Verify we have exactly 95 movies (after removing duplicates)
-- =============================================================================

SELECT COUNT(*) as movie_count
FROM {{ ref('dim_movies') }}
HAVING COUNT(*) != 95