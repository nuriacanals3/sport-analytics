"""League schedule structure for the optimiser, built from the marts.
No dbt here -- reads team_travel_legs (a materialized table, no credentials
needed) and the nba_arenas seed directly via DuckDB.
"""
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date as Date

import duckdb

from optimization.geo import haversine_miles

DUCKDB_PATH = 'transform/nba/nba.duckdb'
SEASON = '2024-25'  # the optimisation case study season
_printed_neutral_site_note = False

# Real NBA neutral-site games in 2024-25. Neither team actually plays at their own arena for
# these, so both teams' travel legs need the real venue, not either team's
# home city. Coordinates: Arena CDMX (Mexico City), T-Mobile Arena (Las
# Vegas, NBA Cup semifinals), Accor Arena (Paris).
NEUTRAL_SITE_VENUES = {
    '0022400147': 'MEXICO_CITY',   # WAS @ MIA, 2024-11-02, NBA Mexico City Game
    '0022401229': 'VEGAS_CUP',     # ATL @ MIL, 2024-12-14, NBA Cup semifinal
    '0022401230': 'VEGAS_CUP',     # OKC @ HOU, 2024-12-14, NBA Cup semifinal
    '0022400621': 'PARIS',         # SAS @ IND, 2025-01-23, NBA Paris Games
    '0022400633': 'PARIS',         # IND @ SAS, 2025-01-25, NBA Paris Games
}
NEUTRAL_VENUE_COORDS = {
    'MEXICO_CITY': (19.4534, -99.1425),
    'VEGAS_CUP': (36.1028, -115.1786),
    'PARIS': (48.8389, 2.3789),
}
# Standard (non-DST) UTC offsets, same static-offset convention as nba_arenas.csv
# (America/Mexico_City has no DST since 2022; Las Vegas is Pacific; Paris is CET).
NEUTRAL_VENUE_UTC_OFFSETS = {
    'MEXICO_CITY': -6,
    'VEGAS_CUP': -8,
    'PARIS': 1,
}

# Real designated "home" team for bookkeeping purposes (the 82-games/team
# count, WL record). This does NOT affect travel/fatigue calculations.
#   - Paris: confirmed explicitly.
#   - Mexico City: confirmed.
#   - The two Vegas Cup semifinals have no real "home" team at all. True
#     neutral tournament games, so they're left on the arbitrary tie-break.
CONFIRMED_NEUTRAL_SITE_HOME_TEAM = {
    '0022400147': 'WAS',  # Mexico City -- confirmed
    '0022400621': 'SAS',  # Paris, Jan 23 -- confirmed
    '0022400633': 'IND',  # Paris, Jan 25 -- confirmed
}


@dataclass(frozen=True)
class Game:
    game_id: str
    date: Date
    home_team: str
    away_team: str


class Schedule:
    def __init__(self, games, arena_coords, arena_utc_offsets):
        self.games = {g.game_id: g for g in games}
        self.arena_coords = arena_coords  # {team_abbreviation: (lat, lon)}
        self.arena_utc_offsets = arena_utc_offsets  # {team_abbreviation: utc_offset_hours}
        self._team_game_ids = self._build_team_index()

    def _build_team_index(self):
        idx = defaultdict(list)
        for g in self.games.values():
            idx[g.home_team].append(g.game_id)
            idx[g.away_team].append(g.game_id)
        for team, gids in idx.items():
            gids.sort(key=lambda gid: self.games[gid].date)
        return idx

    @property
    def teams(self):
        return list(self._team_game_ids.keys())

    def team_schedule(self, team):
        """This team's game_ids, sorted by date."""
        return self._team_game_ids[team]

    def location_for(self, game_id, team):
        """Where `team` plays in this game -- a team abbreviation (the home
        team's arena) for a normal game, or a neutral-site venue key for one
        of the handful of real neutral-site games (see NEUTRAL_SITE_VENUES).
        Either way, the result is a valid key into self.arena_coords.
        """
        if game_id in NEUTRAL_SITE_VENUES:
            return NEUTRAL_SITE_VENUES[game_id]
        return self.games[game_id].home_team

    def is_effectively_away(self, game_id, team):
        """True if `team` should count as travelling/away for THIS game, for
        fatigue-relevant purposes (road-trip streaks, the K cap) -- as
        opposed to `home_team`/`away_team`, which only ever reflect
        bookkeeping (the 82-games/team count, WL record). At a neutral-site
        game NEITHER team gets real home benefits (no home crowd, no
        sleeping in their own bed) even though one of them is still
        nominally "home" for the record book -- so both sides count as away
        here, regardless of that nominal designation.
        """
        if game_id in NEUTRAL_SITE_VENUES:
            return True
        return self.games[game_id].away_team == team

    def team_leg_details(self, team):
        """[(game_id, distance_miles, rest_days, is_back_to_back, timezones_crossed)]
        for this team's full sorted schedule. First game of the season has no
        previous leg -- distance is 0.0 (matches how SUM() ignores NULLs in
        the dbt equivalent), and rest_days/is_back_to_back/timezones_crossed
        are None (genuinely unknown, not "no back-to-back" -- there's no
        season-boundary game to compare against).
        """
        gids = self.team_schedule(team)
        legs = []
        prev_loc = None
        prev_date = None
        for gid in gids:
            loc = self.location_for(gid, team)
            date = self.games[gid].date
            if prev_loc is None:
                dist = 0.0
                rest_days = None
                is_back_to_back = None
                timezones_crossed = None
            else:
                dist = haversine_miles(*self.arena_coords[prev_loc], *self.arena_coords[loc])
                rest_days = (date - prev_date).days - 1
                is_back_to_back = (rest_days == 0)
                timezones_crossed = abs(self.arena_utc_offsets[loc] - self.arena_utc_offsets[prev_loc])
            legs.append((gid, dist, rest_days, is_back_to_back, timezones_crossed))
            prev_loc = loc
            prev_date = date
        return legs

    def team_leg_distances(self, team):
        """[(game_id, distance_from_previous_game)] -- thin wrapper over
        team_leg_details for callers that only need distance (Phase 4's
        miles-only objective, search.py's incremental delta).
        """
        return [(gid, dist) for gid, dist, *_ in self.team_leg_details(team)]

    def total_miles(self):
        """Full recompute: sum of every team's own leg distances. This is the
        slow-but-obviously-correct version search.py's incremental
        evaluation is checked against.
        """
        return sum(
            dist
            for team in self.teams
            for _, dist in self.team_leg_distances(team)
        )

    def apply_date_changes(self, changes):
        """changes: {game_id: new_date}. Mutates in place and returns the
        same changes dict with OLD dates instead -- pass that back to
        apply_date_changes() again to undo (used by search.py to revert a
        rejected move without a full schedule copy).
        """
        undo = {}
        touched_teams = set()
        for game_id, new_date in changes.items():
            game = self.games[game_id]
            undo[game_id] = game.date
            self.games[game_id] = replace(game, date=new_date)
            touched_teams.add(game.home_team)
            touched_teams.add(game.away_team)

        for team in touched_teams:
            self._team_game_ids[team].sort(key=lambda gid: self.games[gid].date)

        return undo


def load_schedule(duckdb_path=DUCKDB_PATH, season=SEASON):
    con = duckdb.connect(duckdb_path, read_only=True)

    # ORDER BY here isn't cosmetic: without it, DuckDB doesn't guarantee row
    # order, and that order silently ends up determining self.teams' order
    # (via dict insertion order below) -- which rng.choice(schedule.teams)
    # in moves.py depends on. Without a stable sort, the same random seed
    # could produce different search results across separate runs.
    rows = con.execute("""
        select game_id, game_date, team_id, team_abbreviation, opponent_abbreviation, is_home
        from team_travel_legs
        where season = ?
        order by game_id, team_id
    """, [season]).fetchall()

    arena_rows = con.execute(
        "select team_abbreviation, lat, lon, utc_offset_hours from nba_arenas"
    ).fetchall()
    con.close()

    arena_coords = {abbr: (lat, lon) for abbr, lat, lon, _ in arena_rows}
    arena_coords.update(NEUTRAL_VENUE_COORDS)

    arena_utc_offsets = {abbr: utc_offset for abbr, _, _, utc_offset in arena_rows}
    arena_utc_offsets.update(NEUTRAL_VENUE_UTC_OFFSETS)

    by_game = defaultdict(list)
    for game_id, game_date, team_id, team_abbr, opp_abbr, is_home in rows:
        by_game[game_id].append((game_date, team_id, team_abbr, opp_abbr, is_home))

    games = []
    no_home_marked_game_ids = []
    for game_id, sides in by_game.items():
        game_date = sides[0][0]
        home_side = next((s for s in sides if s[4]), None)
        if home_side is not None:
            home_team = home_side[2]
            away_team = home_side[3]
        else:
            # These are real neutral-site games (NBA Mexico City Game, NBA
            # Cup semifinals in Las Vegas, NBA Paris Games). "home_team" here is only
            # bookkeeping (the 82-games/team count, WL record) -- it does
            # NOT decide where the game is treated as being played
            # (NEUTRAL_SITE_VENUES + location_for() handle that, separately)
            # or whether it counts as travel for fatigue purposes
            # (is_effectively_away treats both sides as away regardless).
            #
            # An arbitrary-but-deterministic tie-break only
            # for the 2 Vegas Cup games, which genuinely have no real home
            # team to look up (neutral tournament games).
            no_home_marked_game_ids.append(game_id)
            abbrs = sorted(s[2] for s in sides)
            home_team = CONFIRMED_NEUTRAL_SITE_HOME_TEAM.get(game_id, abbrs[0])
            away_team = abbrs[1] if home_team == abbrs[0] else abbrs[0]
        games.append(Game(game_id=game_id, date=game_date, home_team=home_team, away_team=away_team))

    # Print this note only once per process -- callers like run_phase_b.py
    # reload the schedule dozens of times in a single run, and the note
    # would otherwise repeat identically every time, drowning out real
    # progress output for no new information.
    global _printed_neutral_site_note
    if no_home_marked_game_ids and not _printed_neutral_site_note:
        print(f"Note: {len(no_home_marked_game_ids)} game(s) had no marked home team in the "
              f"source data -- confirmed real neutral-site games (Mexico City / Vegas Cup / "
              f"Paris), handled via NEUTRAL_SITE_VENUES: {no_home_marked_game_ids}")
        _printed_neutral_site_note = True

    return Schedule(games, arena_coords, arena_utc_offsets)
