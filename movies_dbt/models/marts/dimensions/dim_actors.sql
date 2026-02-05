{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- =============================================================================
-- MODEL: dim_actors
-- PURPOSE: Actor lookup dimension
-- INPUT: {{ ref('stg_movies_enriched') }}
-- OUTPUT: Unique list of actors with surrogate keys
-- =============================================================================

with movies as (
    select * from {{ ref('stg_movies_enriched') }}
),

-- STEP 1: Split actors (pipe-separated)
split_actors as (
    select
        movie_id,
        trim(actor.value) as actor_name
    from movies,
    lateral flatten(split(actors_raw, '|')) as actor
    -- Split actors separated by "|"
    -- Example: "Christian Bale|Heath Ledger" → 2 rows
    where actors_raw is not null
),

-- STEP 2: Get unique actors
unique_actors as (
    select distinct
        actor_name
    from split_actors
    where actor_name is not null
      and trim(actor_name) != ''
),

-- STEP 3: Add surrogate key
final as (
    select
        row_number() over (order by actor_name) as actor_id,
        actor_name,
        current_timestamp() as dim_created_at
    from unique_actors
)

select * from final
order by actor_name

-- ==============================================================================
-- EXPECTED OUTPUT:
-- - ~150-200 rows (unique actors)
-- - actor_id: 1, 2, 3, ...
-- - actor_name: Al Pacino, Brad Pitt, ...
-- ==============================================================================