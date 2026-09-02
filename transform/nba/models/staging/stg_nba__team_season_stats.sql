-- Combines the two LeagueDashTeamStats bronze flavors (Base, Advanced) into one
-- row per team per season. Only the columns actually needed are kept
-- (see the travel-logistics plan, section 3.1).

-- Gets the jsons of bronze/nba_team_stats/team_stats_base_*
with base_rows as (
    select
        filename,
        unnest(cast(resultSets->0->'rowSet' as json[])) as row
    from read_json_auto(
        's3://' || '{{ env_var("S3_BUCKET_NAME") }}' || '/bronze/nba_team_stats/team_stats_base_*.json',
        format = 'auto',
        filename = true
    )
),

-- Clean base rows: metrics about each team
-- Base column order: 0 TEAM_ID, 1 TEAM_NAME, 2 GP, 3 W, 4 L, 5 W_PCT, ... 26 PTS
base as (
    select
        regexp_extract(filename, 'team_stats_base_([0-9]{4}-[0-9]{2})\.json', 1) as season,
        try_cast(row->>'$[0]' as bigint)                                        as team_id,
        row->>'$[1]'                                                            as team_name,
        try_cast(row->>'$[2]' as integer)                                       as gp, -- (games played)
        try_cast(row->>'$[3]' as integer)                                       as w,
        try_cast(row->>'$[4]' as integer)                                       as l,
        try_cast(row->>'$[5]' as double)                                        as w_pct,
        try_cast(row->>'$[26]' as double)                                       as pts
    from base_rows
),

-- Gets the jsons of bronze/nba_team_stats/team_stats_advanced_*
adv_rows as (
    select
        filename,
        unnest(cast(resultSets->0->'rowSet' as json[])) as row
    from read_json_auto(
        's3://' || '{{ env_var("S3_BUCKET_NAME") }}' || '/bronze/nba_team_stats/team_stats_advanced_*.json',
        format = 'auto',
        filename = true
    )
),

-- Clean advanced rows: data about how good is each team?
-- Advanced column order: 0 TEAM_ID, ... 8 OFF_RATING, 10 DEF_RATING, 12 NET_RATING, ... 22 PACE
advanced as (
    select
        regexp_extract(filename, 'team_stats_advanced_([0-9]{4}-[0-9]{2})\.json', 1) as season,
        try_cast(row->>'$[0]' as bigint)                                            as team_id,
        -- Offensive Rating: points scored per 100 possessions adjusted for pace
        try_cast(row->>'$[8]' as double)                                            as off_rating,
        -- Defensive Rating: points allowed per 100 possessions adjusted for pace
        try_cast(row->>'$[10]' as double)                                           as def_rating,
        -- off_rating - def_rating: how good is the team, adjusted for pace
        try_cast(row->>'$[12]' as double)                                           as net_rating,
        -- Pace: possessions per 48 mins (full game), used to normalize the other variables, 
        -- so teams are not better or worse because they have more or less possessions
        try_cast(row->>'$[22]' as double)                                           as pace
    from adv_rows
)

-- Final output
select
    b.season,
    b.team_id,
    b.team_name,
    b.gp,
    b.w,
    b.l,
    b.w_pct,
    b.pts,
    a.off_rating,
    a.def_rating,
    a.net_rating,
    a.pace
from base b
join advanced a on b.team_id = a.team_id and b.season = a.season
