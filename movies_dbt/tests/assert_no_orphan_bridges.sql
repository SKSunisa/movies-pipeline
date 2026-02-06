-- =============================================================================
-- TEST: assert_no_orphan_bridges
-- PURPOSE: Verify all bridge records have valid foreign keys
-- =============================================================================

-- Check bridge_movie_genre
SELECT 'bridge_movie_genre' as bridge_table, COUNT(*) as orphan_count
FROM {{ ref('bridge_movie_genre') }} b
LEFT JOIN {{ ref('dim_movies') }} m ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL
HAVING COUNT(*) > 0

UNION ALL

-- Check bridge_movie_actor
SELECT 'bridge_movie_actor', COUNT(*)
FROM {{ ref('bridge_movie_actor') }} b
LEFT JOIN {{ ref('dim_movies') }} m ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL
HAVING COUNT(*) > 0

UNION ALL

-- Check bridge_movie_director
SELECT 'bridge_movie_director', COUNT(*)
FROM {{ ref('bridge_movie_director') }} b
LEFT JOIN {{ ref('dim_movies') }} m ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL
HAVING COUNT(*) > 0