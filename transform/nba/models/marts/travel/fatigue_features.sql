-- Team-game grain: one row per team per game, with the opponent's row attached
-- via a self-join on game_id (each game has exactly 2 team rows). This is the
-- model Phase 3's cost model trains on -- plus_minus is the target.

with games as (
    select * from {{ ref('stg_nba__game_log') }}
),

-- Paired CTE: adds opponent info to each game (same rows but both teams info in each one)
-- Each game_id has exactly one other team row; joining self to itself on
-- game_id (excluding the same team_id) attaches that opponent row.
paired as (
    select
        self.season,
        self.game_id,
        self.game_date,
        self.team_id,
        self.team_abbreviation,
        self.is_home,
        self.wl,
        self.plus_minus,
        opp.team_id                as opponent_team_id,
        opp.team_abbreviation       as opponent_team_abbreviation
    from games self
    join games opp
        on self.game_id = opp.game_id
        and self.team_id != opp.team_id
),

self_travel as (
    select * from {{ ref('team_travel_legs') }}
),

opp_travel as (
    select * from {{ ref('team_travel_legs') }}
),

self_stats as (
    select * from {{ ref('stg_nba__team_season_stats') }}
),

opp_stats as (
    select * from {{ ref('stg_nba__team_season_stats') }}
)

select
    p.season,
    p.game_id,
    p.game_date,
    p.team_id,
    p.team_abbreviation,
    p.opponent_team_id,
    p.opponent_team_abbreviation,
    p.is_home,
    p.wl,
    p.plus_minus                                                        as target_plus_minus,
    
    -- Own/opponent net rating and home/away, kept as separate columns (not
    -- differenced) per the travel-logistics plan's spec for this model.
    ss.net_rating                                                       as self_net_rating,
    os.net_rating                                                       as opponent_net_rating,

    -- Differential fatigue/travel features (self minus opponent) -- these are
    -- what the plan's cost model is meant to regress plus_minus on.
    st.rest_days                                                        as self_rest_days,
    ot.rest_days                                                        as opponent_rest_days,
    st.rest_days - ot.rest_days                                         as rest_days_diff,

    st.distance_miles                                                   as self_travel_miles,
    ot.distance_miles                                                   as opponent_travel_miles,
    st.distance_miles - ot.distance_miles                               as travel_miles_diff,

    st.is_back_to_back                                                  as self_is_back_to_back,
    ot.is_back_to_back                                                  as opponent_is_back_to_back,

    st.timezones_crossed                                                as self_timezones_crossed,
    ot.timezones_crossed                                                as opponent_timezones_crossed,
    st.timezones_crossed - ot.timezones_crossed                         as timezones_crossed_diff,

    -- Signed version (positive = flew east, negative = flew west) -- this is
    -- what Phase 3's regression actually uses, not the magnitude above.
    st.timezones_crossed_signed                                         as self_timezones_shift,
    ot.timezones_crossed_signed                                         as opponent_timezones_shift,
    st.timezones_crossed_signed - ot.timezones_crossed_signed           as timezones_shift_diff,

    st.road_trip_length                                                 as self_road_trip_length,
    ot.road_trip_length                                                 as opponent_road_trip_length
from paired p
left join self_travel st on p.team_id = st.team_id and p.game_id = st.game_id
left join opp_travel ot on p.opponent_team_id = ot.team_id and p.game_id = ot.game_id
left join self_stats ss on p.team_id = ss.team_id and p.season = ss.season
left join opp_stats os on p.opponent_team_id = os.team_id and p.season = os.season
