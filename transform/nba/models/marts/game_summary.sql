with pbp as (
    select * from {{ ref('stg_nba__play_by_play') }}
),

last_scores as (
    select
        game_id,
        score_home,
        score_away,
        period,
        row_number() over (
            partition by game_id
            order by period desc, action_number desc
        ) as rn
    from pbp
    where score_home is not null and score_away is not null
),

game_stats as (
    select
        game_id,
        count(*)                                                as total_actions,
        count(*) filter (where is_field_goal = true)           as total_fg_attempts,
        count(*) filter (where action_type = 'freethrow')      as total_ft_attempts,
        max(period)                                             as periods_played,
        count(distinct team_id) filter (where team_id != '')   as teams_in_game
    from pbp
    group by game_id
)

select
    gs.game_id,
    ls.score_home                                               as final_score_home,
    ls.score_away                                               as final_score_away,
    case
        when ls.score_home > ls.score_away then 'home'
        when ls.score_away > ls.score_home then 'away'
        else 'tie'
    end                                                         as winner_location,
    abs(ls.score_home - ls.score_away)                          as point_margin,
    gs.periods_played,
    gs.periods_played > 4                                       as went_to_overtime,
    gs.total_actions,
    gs.total_fg_attempts,
    gs.total_ft_attempts
from game_stats gs
join last_scores ls on gs.game_id = ls.game_id and ls.rn = 1
