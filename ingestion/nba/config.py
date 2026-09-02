# Shared config for the travel-logistics ingestion scripts.
#
# Both league_game_log.py and team_season_stats.py need the exact same list
# of seasons -- fatigue_features (Phase 2) self-joins LeagueGameLog against
# itself and against team stats by season, so any drift between the two
# scripts would silently produce a partial join. Keeping the list in one
# place removes that risk.
#
# Regular seasons only (playoffs have a different travel pattern -- series,
# not a single game). 2019-20 (bubble) and 2020-21 (compressed schedule) are
# excluded as anomalous per the travel-logistics plan.
SEASONS = ["2018-19", "2021-22", "2022-23", "2023-24", "2024-25"]

SEASON_TYPE = "Regular Season"
