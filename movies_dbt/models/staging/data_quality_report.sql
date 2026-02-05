{{ config(materialized='view', schema='staging') }}

with cleaned as (select * from {{ ref('stg_movies_cleaned') }}),
enriched as (select * from {{ ref('stg_movies_enriched') }}),

row_checks as (
    select 'Row Counts' as category, 'Total Rows (Cleaned)' as check_name,
           count(*) as value, 95 as expected,
           case when count(*) = 95 then 'PASS' else 'FAIL' end as status
    from cleaned
    union all
    select 'Row Counts', 'Total Rows (Enriched)', count(*), 95,
           case when count(*) = 95 then 'PASS' else 'FAIL' end
    from enriched
),

null_checks as (
    select 'NULL Checks' as category, 'NULL movie_id' as check_name,
           count(*) as value, 0 as expected,
           case when count(*) = 0 then 'PASS' else 'FAIL' end as status
    from cleaned where movie_id is null
    union all
    select 'NULL Checks', 'NULL movie_title', count(*), 0,
           case when count(*) = 0 then 'PASS' else 'FAIL' end
    from cleaned where movie_title is null
    union all
    select 'NULL Checks', 'NULL release_year', count(*), 0,
           case when count(*) = 0 then 'PASS' else 'FAIL' end
    from cleaned where release_year is null
),

duplicate_checks as (
    select 'Duplicates' as category, 'Duplicate movie_id' as check_name,
           count(*) - count(distinct movie_id) as value, 0 as expected,
           case when count(*) = count(distinct movie_id) then 'PASS' else 'FAIL' end as status
    from cleaned
),

range_checks as (
    select 'Range Checks' as category, 'IMDb Range (0-10)' as check_name,
           count(*) as value, 0 as expected,
           case when count(*) = 0 then 'PASS' else 'FAIL' end as status
    from cleaned
    where imdb_rating is not null and (imdb_rating < 0 or imdb_rating > 10)
    union all
    select 'Range Checks', 'RT Range (0-100)', count(*), 0,
           case when count(*) = 0 then 'PASS' else 'FAIL' end
    from cleaned
    where rotten_tomatoes_pct is not null and (rotten_tomatoes_pct < 0 or rotten_tomatoes_pct > 100)
),

stats as (
    select 'Statistics' as category, 'Min IMDb' as check_name,
           round(min(imdb_rating), 2) as value, 7.0 as expected, 'INFO' as status
    from cleaned where imdb_rating is not null
    union all
    select 'Statistics', 'Max IMDb', round(max(imdb_rating), 2), 10.0, 'INFO'
    from cleaned where imdb_rating is not null
    union all
    select 'Statistics', 'Avg IMDb', round(avg(imdb_rating), 2), 8.0, 'INFO'
    from cleaned where imdb_rating is not null
),

enrichment_checks as (
    select 'Enrichment' as category, 'Rating Category' as check_name,
           count(*) as value, 95 as expected,
           case when count(*) = 95 then 'PASS' else 'FAIL' end as status
    from enriched where rating_category is not null
    union all
    select 'Enrichment', 'Decade Populated', count(*), 95,
           case when count(*) = 95 then 'PASS' else 'FAIL' end
    from enriched where decade is not null
)

select * from row_checks
union all select * from null_checks
union all select * from duplicate_checks
union all select * from range_checks
union all select * from stats
union all select * from enrichment_checks
order by case category
    when 'Row Counts' then 1
    when 'NULL Checks' then 2
    when 'Duplicates' then 3
    when 'Range Checks' then 4
    when 'Statistics' then 5
    when 'Enrichment' then 6
end
