-- One row per team per game -- the "leg" of travel required to arrive at that
-- game. Window functions are partitioned by (team_id, season), not just
-- team_id, because SEASONS (ingestion/nba/config.py) has real multi-year gaps
-- between included seasons (COVID seasons excluded) -- partitioning by team
-- alone would treat the offseason gap as a single, nonsensical "rest" value.


-- Games CTE
with games as (
    select
        season,
        team_id,
        team_abbreviation,
        game_id,
        game_date,
        is_home,
        opponent_abbreviation,
        case when is_home then team_abbreviation else opponent_abbreviation end as game_location_abbreviation
    from {{ ref('stg_nba__game_log') }}
),

-- Previous game CTE (games CTE + last game info):
-- Look back one row (same team, same season, ordered by date) to find where
-- and when this team played last -- null for a team's first game of a season.
with_prev as (
    select
        *,
        lag(game_date) over (partition by team_id, season order by game_date)                     as prev_game_date,
        lag(game_location_abbreviation) over (partition by team_id, season order by game_date)    as prev_location_abbreviation
    from games
),

-- Arena CTE: previous game CTE + arena locations of last and current game
with_arenas as (
    select
        wp.*,
        curr_arena.lat              as curr_lat,
        curr_arena.lon              as curr_lon,
        curr_arena.utc_offset_hours as curr_utc_offset_hours,
        prev_arena.lat              as prev_lat,
        prev_arena.lon              as prev_lon,
        prev_arena.utc_offset_hours as prev_utc_offset_hours
    from with_prev wp
    left join {{ ref('nba_arenas') }} curr_arena on wp.game_location_abbreviation = curr_arena.team_abbreviation
    left join {{ ref('nba_arenas') }} prev_arena on wp.prev_location_abbreviation = prev_arena.team_abbreviation
),

-- Home/away streaches CTE:
-- Consecutive same is_home games: road trip or homestand. To know how long has they been.
with_streaks as (
    select
        *,
        row_number() over (partition by team_id, season order by game_date)
            - row_number() over (partition by team_id, season, is_home order by game_date) as streak_group
    from with_arenas
)

-- Final output
select
    season,
    team_id,
    team_abbreviation,
    game_id,
    game_date,
    is_home,
    opponent_abbreviation,
    game_location_abbreviation,
    prev_game_date,
    prev_location_abbreviation,
    -- Full rest days between the previous game and this one; 0 = back-to-back. Null for first games of a season.
    date_diff('day', prev_game_date, game_date) - 1                        as rest_days,
    date_diff('day', prev_game_date, game_date) - 1 = 0                    as is_back_to_back,
    round({{ haversine_miles('prev_lat', 'prev_lon', 'curr_lat', 'curr_lon') }}, 1) as distance_miles,
    abs(curr_utc_offset_hours - prev_utc_offset_hours)                     as timezones_crossed,
    -- Eastward = the destination's UTC offset is greater (less negative) than
    -- the previous location's, e.g. Pacific (-8) -> Eastern (-5).
    case
        when curr_utc_offset_hours is null or prev_utc_offset_hours is null then null
        else curr_utc_offset_hours > prev_utc_offset_hours
    end                                                                    as is_eastward,
    -- Size of the current consecutive-away streak, repeated on every row in
    -- it (e.g. a 4-game road trip shows 4 on all 4 rows). Null on home games.
    case when not is_home then count(*) over (partition by team_id, season, is_home, streak_group) end as road_trip_length
from with_streaks
