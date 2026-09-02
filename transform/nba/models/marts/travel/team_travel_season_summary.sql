-- Per-team, per-season travel totals -- the "one team" diagnostic view named in
-- the travel-logistics plan (section 2): never the optimisation unit itself,
-- just a way to sanity-check and report on individual teams' travel burden.

select
    season,
    team_id,
    team_abbreviation,
    count(*)                            as games_played,
    sum(distance_miles)                 as total_miles,
    sum(is_back_to_back::int)           as total_back_to_backs,
    avg(rest_days)                      as avg_rest_days,
    sum(timezones_crossed)              as total_timezones_crossed,
    max(road_trip_length)               as longest_road_trip
from {{ ref('team_travel_legs') }}
group by season, team_id, team_abbreviation
