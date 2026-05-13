with player_stats as (
    select * from {{ ref('player_game_stats') }}
),

game_summary as (
    select * from {{ ref('game_summary') }}
)

select
    ps.game_id,
    ps.team_id,
    ps.team_tricode,
    sum(ps.points_scored)                                       as team_points,
    sum(ps.field_goals_made)                                    as team_fg_made,
    sum(ps.field_goals_attempted)                               as team_fg_attempted,
    round(
        sum(ps.field_goals_made)::float
        / nullif(sum(ps.field_goals_attempted), 0),
        3
    )                                                           as team_fg_pct,
    sum(ps.three_pointers_made)                                 as team_3pt_made,
    sum(ps.three_pointers_attempted)                            as team_3pt_attempted,
    sum(ps.free_throws_made)                                    as team_ft_made,
    sum(ps.free_throws_attempted)                               as team_ft_attempted,
    sum(ps.rebounds)                                            as team_rebounds,
    sum(ps.turnovers)                                           as team_turnovers,
    sum(ps.fouls)                                               as team_fouls,
    count(distinct ps.player_id)                                as players_in_game,
    gs.periods_played,
    gs.went_to_overtime
from player_stats ps
join game_summary gs on ps.game_id = gs.game_id
group by
    ps.game_id,
    ps.team_id,
    ps.team_tricode,
    gs.periods_played,
    gs.went_to_overtime
