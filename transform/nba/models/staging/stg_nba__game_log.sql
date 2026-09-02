-- Gets the jsons of /bronze/nba_game_log/*
with game_rows as (
    select
        filename,
        unnest(cast(resultSets->0->'rowSet' as json[])) as row
    from read_json_auto(
        's3://' || '{{ env_var("S3_BUCKET_NAME") }}' || '/bronze/nba_game_log/*.json',
        format = 'auto',
        filename = true
    )
),

-- Select fields and clean
-- Column order, verified against the raw LeagueGameLog response
cleaned as (
    select
        -- season comes from the bronze filename (game_log_{season}.json), not the
        -- raw SEASON_ID field
        regexp_extract(filename, 'game_log_([0-9]{4}-[0-9]{2})\.json', 1) as season,
        try_cast(row->>'$[1]' as bigint)                                 as team_id,
        row->>'$[2]'                                                     as team_abbreviation,
        row->>'$[3]'                                                     as team_name,
        row->>'$[4]'                                                     as game_id,
        try_cast(row->>'$[5]' as date)                                   as game_date,
        row->>'$[6]'                                                     as matchup,
        row->>'$[7]'                                                     as wl, -- (win/loss)
        try_cast(row->>'$[27]' as double)                                as plus_minus
    from game_rows
)

-- Final table
select
    season,
    team_id,
    team_abbreviation,
    team_name,
    game_id,
    game_date,
    matchup,
    -- MATCHUP is "XXX vs. YYY" for a home game, "XXX @ YYY" for an away game.
    matchup ilike '%vs.%'                                                as is_home,
    case
        when matchup ilike '%vs.%' then trim(split_part(matchup, 'vs.', 2))
        else trim(split_part(matchup, '@', 2))
    end                                                                  as opponent_abbreviation,
    wl,
    plus_minus -- Point margin for this team (positive = won by that margin)
from cleaned
