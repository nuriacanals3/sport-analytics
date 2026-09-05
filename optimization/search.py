"""Simulated annealing over Schedule, with incremental (delta) objective
evaluation: a move touches at most a handful of teams, so only those teams'
leg sequences get recomputed -- not the whole league's 30 teams x 82 games
every iteration. "Incremental" here means "per few affected teams," not
literal per-leg patching -- a deliberate simplicity/correctness trade-off,
checked against the slow, obviously-correct full recompute in the tests.
"""
import math
import random

from optimization.moves import MAX_CONSECUTIVE_AWAY, propose_move


def _affected_teams(schedule, changes):
    teams = set()
    for gid in changes:
        game = schedule.games[gid]
        teams.add(game.home_team)
        teams.add(game.away_team)
    # Sorting makes the sum order (and therefore the whole search trajectory)
    # deterministic given a seed regardless of the process's hash seed.
    return sorted(teams)


def _teams_total_miles(schedule, teams):
    return sum(
        dist
        for team in teams
        for _, dist in schedule.team_leg_distances(team)
    )


def apply_and_get_delta(schedule, changes):
    """Applies `changes` to `schedule` (already mutated on return) and
    returns (delta_in_total_miles, undo_changes). Caller decides whether to
    keep the move (do nothing more) or revert it (apply_date_changes(undo)).
    """
    teams = _affected_teams(schedule, changes)
    before = _teams_total_miles(schedule, teams)
    undo = schedule.apply_date_changes(changes)
    after = _teams_total_miles(schedule, teams)
    return after - before, undo


def simulated_annealing(
    schedule,
    iterations=25_000,
    initial_temp=50.0,
    cooling_rate=0.9995,
    k=MAX_CONSECUTIVE_AWAY,
    seed=None,
):
    rng = random.Random(seed)
    current_miles = schedule.total_miles()
    best_miles = current_miles
    best_snapshot = {gid: g.date for gid, g in schedule.games.items()}
    temp = initial_temp
    history = []

    for i in range(iterations):
        changes = propose_move(schedule, rng, k=k)
        if changes is None:
            temp *= cooling_rate
            continue

        delta, undo = apply_and_get_delta(schedule, changes)
        accept = delta < 0 or rng.random() < math.exp(-delta / max(temp, 1e-9))

        if accept:
            current_miles += delta
            if current_miles < best_miles:
                best_miles = current_miles
                best_snapshot = {gid: g.date for gid, g in schedule.games.items()}
        else:
            schedule.apply_date_changes(undo)

        temp *= cooling_rate
        if i % 1000 == 0:
            history.append((i, current_miles, best_miles, temp))

    return best_snapshot, best_miles, history
