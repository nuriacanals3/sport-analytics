with pbp as (
    select * from {{ ref('stg_nba__play_by_play') }}
),

player_actions as (
    select
        game_id,
        player_id,
        player_name,
        team_id,
        team_tricode,

        count(*) filter (
            where action_type = 'freethrow' and shot_result = 'Made'
        )                                                               as free_throws_made,
        count(*) filter (
            where action_type = 'freethrow'
        )                                                               as free_throws_attempted,

        count(*) filter (
            where is_field_goal = true and shot_result = 'Made'
        )                                                               as field_goals_made,
        count(*) filter (
            where is_field_goal = true
        )                                                               as field_goals_attempted,

        count(*) filter (
            where is_field_goal = true
            and shot_result = 'Made'
            and sub_type = '3pt'
        )                                                               as three_pointers_made,
        count(*) filter (
            where is_field_goal = true and sub_type = '3pt'
        )                                                               as three_pointers_attempted,

        count(*) filter (where action_type = 'rebound')                as rebounds,
        count(*) filter (where action_type = 'assist')                 as assists,
        count(*) filter (where action_type = 'turnover')               as turnovers,
        count(*) filter (where action_type = 'block')                  as blocks,
        count(*) filter (where action_type = 'steal')                  as steals,
        count(*) filter (where action_type = 'foul')                   as fouls

    from pbp
    where player_id is not null and player_id != ''
    group by game_id, player_id, player_name, team_id, team_tricode
)

select
    game_id,
    player_id,
    player_name,
    team_id,
    team_tricode,
    field_goals_made,
    field_goals_attempted,
    three_pointers_made,
    three_pointers_attempted,
    free_throws_made,
    free_throws_attempted,
    rebounds,
    assists,
    turnovers,
    blocks,
    steals,
    fouls,
    (field_goals_made - three_pointers_made) * 2
        + three_pointers_made * 3
        + free_throws_made                                              as points_scored,
    case
        when field_goals_attempted = 0 then null
        else round(field_goals_made::float / field_goals_attempted, 3)
    end                                                                 as fg_pct,
    case
        when free_throws_attempted = 0 then null
        else round(free_throws_made::float / free_throws_attempted, 3)
    end                                                                 as ft_pct
from player_actions
order by game_id, points_scored desc
