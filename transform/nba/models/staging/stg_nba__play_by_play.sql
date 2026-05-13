with raw as (
    select
        game->>'gameId'        as game_id,
        unnest(cast(game->'actions' as json[])) as action
    from read_json_auto(
        's3://' || '{{ env_var("S3_BUCKET_NAME") }}' || '/bronze/nba_pbp/*.json',
        format = 'auto'
    )
),

cleaned as (
    select
        game_id,
        (action->>'actionNumber')::integer                          as action_number,
        action->>'clock'                                            as clock_raw,
        {{ parse_clock("action->>'clock'") }}                       as clock_seconds_elapsed,
        (action->>'period')::integer                                as period,
        action->>'teamId'                                           as team_id,
        action->>'teamTricode'                                      as team_tricode,
        action->>'personId'                                         as player_id,
        action->>'playerName'                                       as player_name,
        action->>'actionType'                                       as action_type,
        action->>'subType'                                          as sub_type,
        action->>'description'                                      as description,
        try_cast(action->>'isFieldGoal' as boolean)                 as is_field_goal,
        action->>'shotResult'                                       as shot_result,
        try_cast(action->>'shotDistance' as float)                  as shot_distance,
        try_cast(action->>'pointsTotal' as integer)                 as points_total,
        try_cast(action->>'scoreHome' as integer)                   as score_home,
        try_cast(action->>'scoreAway' as integer)                   as score_away,
        action->>'location'                                         as location,
        try_cast(action->>'videoAvailable' as boolean)              as video_available,
        try_cast(action->>'actionId' as integer)                    as action_id
    from raw
    where action is not null
)

select * from cleaned
