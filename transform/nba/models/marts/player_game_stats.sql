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

        -- Made/missed split comes from action_type, not shot_result
        count(*) filter (
            where action_type = 'Made Shot'
        )                                                               as field_goals_made,
        count(*) filter (
            where action_type in ('Made Shot', 'Missed Shot')
        )                                                               as field_goals_attempted,

        -- 3-pointers identified via '3PT' in description
        count(*) filter (
            where action_type = 'Made Shot'
            and description like '%3PT%'
        )                                                               as three_pointers_made,
        count(*) filter (
            where action_type in ('Made Shot', 'Missed Shot')
            and description like '%3PT%'
        )                                                               as three_pointers_attempted,

        -- Free throws: missed ones have description starting with 'MISS'
        count(*) filter (
            where action_type = 'Free Throw'
            and description not like 'MISS%'
        )                                                               as free_throws_made,
        count(*) filter (
            where action_type = 'Free Throw'
        )                                                               as free_throws_attempted,

        count(*) filter (where action_type = 'Rebound')                as rebounds,
        count(*) filter (where action_type = 'Turnover')               as turnovers,
        count(*) filter (where action_type = 'Foul')                   as fouls

        -- Assists, blocks, steals are attributes of other actions in NBA PBP v3,
        -- not separate action types. They require extracting assist/block/steal
        -- person IDs from the raw JSON — a future enhancement.

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
    turnovers,
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
