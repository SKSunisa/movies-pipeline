{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: bridge_movie_actor
-- PURPOSE: Many-to-Many bridge between movies and actors
-- INPUT: {{ ref('dim_movies') }}, {{ ref('dim_actors') }}
-- OUTPUT: Movie-Actor relationships
-- =============================================================================

with movies as (
    select
        movie_id,
        actors_raw        
    from {{ ref('dim_movies') }}
),

actors as (
    select
        actor_id,
        actor_name
    from {{ ref('dim_actors') }}
),

-- Split actors for each movie
split_actors as (
    select
        movie_id,
        trim(actor.value) as actor_name
    from movies,
    lateral flatten(split(actors_raw, '|')) as actor
    where actors_raw is not null          
),

-- Join with dim_actors to get actor_id
final as (
    select
        sa.movie_id,
        a.actor_id,
        sa.actor_name,
        current_timestamp() as bridge_created_at
    from split_actors sa
    inner join actors a                   
        on sa.actor_name = a.actor_name
    where sa.actor_name is not null
      and trim(sa.actor_name) != ''
)

select * from final
order by movie_id, actor_id

-- ==============================================================================
-- OUTPUT:
-- 189 rows (movies × actors)
-- - One row per movie-actor combination
-- 
-- EXAMPLE:
-- movie_id | actor_id | actor_name
-- ---------|----------|------------------
-- 3        | 12       | Christian Bale
-- 3        | 87       | Heath Ledger
-- ==============================================================================