"""Feasibility-preserving moves over a Schedule. Each move function proposes
a candidate {game_id: new_date} change and returns it only if feasible;
returns None if it couldn't find a valid candidate (the caller just tries
again with a different random pick).

Hard constraints enforced here, per the travel-logistics plan:
  - one game per team per day
  - one game per arena per day
  - cap on consecutive away games (K)
The fixture multiset itself (who plays whom, home/away, 82 games/team) is
never touched by any move -- only dates change.

3 move types:
  - home-and-home leg swap: change the two legs dates, only involve two teams.
  - date swap: of one team, swap dates of two of it's games --> it affect other teams
  - Road-trip reorder: one team road trips and permute the order of stops keeping the same dates
"""
import random

from optimization.schedule import NEUTRAL_SITE_VENUES

MAX_CONSECUTIVE_AWAY = 9  # K - real 2024-25 maximum (Charlotte's actual 9-game 
# road trip). Change if optimizing another season


def _team_day_conflict(schedule, team, new_date, excluding_game_ids):
    """True if `team` already has another game on new_date, ignoring the
    games currently being moved (which are allowed to land on each other's
    old dates as part of the same swap).
    """
    for gid in schedule.team_schedule(team):
        if gid in excluding_game_ids:
            continue
        if schedule.games[gid].date == new_date:
            return True
    return False


def _arena_day_conflict(schedule, arena_team, new_date, excluding_game_ids):
    """True if `arena_team`'s arena already hosts another game on new_date.
    In this dataset every arena belongs to exactly one team, so this is
    implied by that team's own day-conflict check -- kept as an explicit,
    separate check anyway, matching the plan's wording, and in case a future
    dataset ever has a shared arena.
    """
    for gid in schedule.team_schedule(arena_team):
        if gid in excluding_game_ids:
            continue
        game = schedule.games[gid]
        if game.home_team == arena_team and game.date == new_date:
            return True
    return False


def _longest_away_streak(schedule, team):
    """Longest run of consecutive away games in `team`'s current schedule.
    Uses is_effectively_away, not the raw away_team flag: a neutral-site
    game counts as away for BOTH teams here, regardless of which one is
    nominally "home" for the record book (see is_effectively_away's
    docstring) -- otherwise a team's real, continuous travel could be
    undercounted just because one leg of it was nominally a "home" game.
    """
    longest = current = 0
    for gid in schedule.team_schedule(team):
        if schedule.is_effectively_away(gid, team):
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _respects_away_cap(schedule, teams, k=MAX_CONSECUTIVE_AWAY):
    return all(_longest_away_streak(schedule, team) <= k for team in teams)


def _try_apply_and_check(schedule, changes, teams_to_check, k):
    """Applies `changes`, checks the away-streak cap, and always reverts --
    callers get a clean feasibility answer without a lingering mutation.
    """
    undo = schedule.apply_date_changes(changes)
    feasible = _respects_away_cap(schedule, teams_to_check, k)
    schedule.apply_date_changes(undo)
    return feasible


def home_and_home_swap(schedule, rng, k=MAX_CONSECUTIVE_AWAY):
    """Pick a pair that plays twice; swap the two legs' dates. Always safe
    against day/arena conflicts (see the Phase 4 design discussion -- both
    games already involve only these two teams and these two dates), so the
    only thing left to check is the away-streak cap.
    """
    team = rng.choice(schedule.teams)
    gids = schedule.team_schedule(team)
    by_opponent = {}
    for gid in gids:
        game = schedule.games[gid]
        opponent = game.away_team if game.home_team == team else game.home_team
        by_opponent.setdefault(opponent, []).append(gid)

    # Neutral-site games' dates are fixed by real-world broadcast/venue
    # contracts, not something a schedule-maker could realistically move --
    # same reasoning as road_trip_reorder excluding them from its shuffle.
    pairs = [
        gids2 for gids2 in by_opponent.values()
        if len(gids2) == 2 and not any(gid in NEUTRAL_SITE_VENUES for gid in gids2)
    ]
    if not pairs:
        return None

    gid_a, gid_b = rng.choice(pairs)
    date_a, date_b = schedule.games[gid_a].date, schedule.games[gid_b].date
    changes = {gid_a: date_b, gid_b: date_a}

    opponent = schedule.games[gid_a].away_team if schedule.games[gid_a].home_team == team \
        else schedule.games[gid_a].home_team
    if not _try_apply_and_check(schedule, changes, {team, opponent}, k):
        return None
    return changes


def date_swap(schedule, rng, k=MAX_CONSECUTIVE_AWAY):
    """Pick one team, two of its games against different opponents, swap
    their dates. Needs a real feasibility check against both opponents.
    """
    team = rng.choice(schedule.teams)
    # Neutral-site games' dates are fixed by real-world broadcast/venue
    # contracts -- not eligible to be picked up and moved by this move.
    gids = [gid for gid in schedule.team_schedule(team) if gid not in NEUTRAL_SITE_VENUES]
    if len(gids) < 2:
        return None

    gid_a, gid_b = rng.sample(gids, 2)
    game_a, game_b = schedule.games[gid_a], schedule.games[gid_b]
    opp_a = game_a.away_team if game_a.home_team == team else game_a.home_team
    opp_b = game_b.away_team if game_b.home_team == team else game_b.home_team
    if opp_a == opp_b:
        return None  # same pair twice -- that's a home-and-home swap, not this move

    date_a, date_b = game_a.date, game_b.date
    excluding = {gid_a, gid_b}
    if _team_day_conflict(schedule, opp_a, date_b, excluding):
        return None
    if _team_day_conflict(schedule, opp_b, date_a, excluding):
        return None
    if game_a.home_team == team and _arena_day_conflict(schedule, team, date_b, excluding):
        return None
    if game_b.home_team == team and _arena_day_conflict(schedule, team, date_a, excluding):
        return None

    changes = {gid_a: date_b, gid_b: date_a}
    if not _try_apply_and_check(schedule, changes, {team, opp_a, opp_b}, k):
        return None
    return changes


def road_trip_reorder(schedule, rng, k=MAX_CONSECUTIVE_AWAY):
    """Pick one of a team's existing road trips, permute the order of stops
    within it, keeping the same set of dates fixed. Each reassigned opponent
    must not already have a conflicting game on its new day.

    Trip *boundaries* use is_effectively_away, so a neutral-site game
    doesn't wrongly split a real trip in two just because it's nominally
    "home" for one side. But neutral-site games themselves are excluded from
    the actual shuffle: their real-world dates are fixed by broadcast/venue
    logistics, not something a schedule-maker could realistically move --
    only the normal away legs within the trip are eligible to be reordered.
    """
    team = rng.choice(schedule.teams)
    gids = schedule.team_schedule(team)

    trips = []
    current = []
    for gid in gids:
        if schedule.is_effectively_away(gid, team):
            current.append(gid)
        else:
            if len(current) >= 2:
                trips.append(current)
            current = []
    if len(current) >= 2:
        trips.append(current)
    if not trips:
        return None

    trip = rng.choice(trips)
    shufflable = [gid for gid in trip if gid not in NEUTRAL_SITE_VENUES]
    if len(shufflable) < 2:
        return None

    dates = [schedule.games[gid].date for gid in shufflable]
    shuffled = shufflable[:]
    rng.shuffle(shuffled)
    if shuffled == shufflable:
        return None

    changes = {gid: new_date for gid, new_date in zip(shuffled, dates)}
    excluding = set(shufflable)
    opponents = set()
    for gid, new_date in changes.items():
        game = schedule.games[gid]
        opponent = game.away_team if game.home_team == team else game.home_team
        opponents.add(opponent)
        if _team_day_conflict(schedule, opponent, new_date, excluding):
            return None
        if _arena_day_conflict(schedule, opponent, new_date, excluding):
            return None

    if not _try_apply_and_check(schedule, changes, {team} | opponents, k):
        return None
    return changes


MOVE_TYPES = [home_and_home_swap, date_swap, road_trip_reorder]


def propose_move(schedule, rng, k=MAX_CONSECUTIVE_AWAY, max_attempts=20):
    """Tries random moves (of a random type) until one is feasible, or gives
    up after max_attempts and returns None (search.py just skips that
    iteration).
    """
    for _ in range(max_attempts):
        move_fn = rng.choice(MOVE_TYPES)
        changes = move_fn(schedule, rng, k)
        if changes is not None:
            return changes
    return None
