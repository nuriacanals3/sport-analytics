"""Simulated annealing over Schedule, with incremental (delta) objective
evaluation: a move touches at most a handful of teams, so only those teams'
leg sequences get recomputed -- not the whole league's 30 teams x 82 games
every iteration. "Incremental" here means "per few affected teams," not
literal per-leg patching -- a deliberate simplicity/correctness trade-off,
checked against the slow, obviously-correct full recompute in the tests.

Generalised for Phase B (optimization/objectives.py): the objective being
minimised is described as a tuple of "components" (e.g. just total miles for
Phase A; (fatigue_burden, carbon) for Phase B) plus a `combine` function that
turns those components into the single scalar accept/reject decisions are
based on. Callers that don't pass these get Phase A's exact original
behaviour (miles-only) -- nothing about run_phase_a.py needed to change.
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


def _default_teams_components(schedule, teams):
    return (_teams_total_miles(schedule, teams),)


def _default_full_components(schedule):
    return (schedule.total_miles(),)


def _default_combine(components):
    return components[0]


def calibrate_initial_temp(
    schedule, teams_components, full_components, combine,
    k=MAX_CONSECUTIVE_AWAY, rng=None, sample_size=200, acceptance_target=0.3,
):
    """Samples real proposed moves against the given objective and picks an
    initial temperature that gives a typical uphill move roughly
    `acceptance_target` probability of being accepted. Not wired in as
    simulated_annealing's default (that stays a fixed 50.0, so Phase A's
    behaviour never changes) -- callers with a different objective scale
    (Phase B's carbon and burden are on totally different scales from miles,
    and the normalised combined score is on yet another) should call this
    explicitly instead of assuming 50.0 still means anything for their
    objective.
    """
    rng = rng or random.Random()
    current_components = full_components(schedule)
    current_value = combine(current_components)
    deltas = []
    for _ in range(sample_size):
        changes = propose_move(schedule, rng, k=k)
        if changes is None:
            continue
        delta_components, undo = apply_and_get_delta(schedule, changes, teams_components)
        new_components = tuple(c + d for c, d in zip(current_components, delta_components))
        deltas.append(abs(combine(new_components) - current_value))
        schedule.apply_date_changes(undo)  # sampling only -- always revert

    if not deltas:
        return 50.0  # fallback: couldn't sample any move at all
    typical = sorted(deltas)[len(deltas) // 2]  # median
    if typical == 0:
        return 50.0
    # exp(-typical / T) = acceptance_target  =>  T = -typical / ln(acceptance_target)
    return -typical / math.log(acceptance_target)


def apply_and_get_delta(schedule, changes, teams_components=_default_teams_components):
    """Applies `changes` to `schedule` (already mutated on return) and
    returns (delta_components, undo_changes) -- delta_components is a tuple,
    one entry per objective component, each the change in that component's
    value restricted to the teams this move actually touched. Caller decides
    whether to keep the move (do nothing more) or revert it
    (apply_date_changes(undo)).
    """
    teams = _affected_teams(schedule, changes)
    before = teams_components(schedule, teams)
    undo = schedule.apply_date_changes(changes)
    after = teams_components(schedule, teams)
    delta_components = tuple(a - b for a, b in zip(after, before))
    return delta_components, undo


def simulated_annealing(
    schedule,
    iterations=25_000,
    initial_temp=50.0,
    cooling_rate=0.9995,
    k=MAX_CONSECUTIVE_AWAY,
    seed=None,
    teams_components=_default_teams_components,
    full_components=_default_full_components,
    combine=_default_combine,
):
    rng = random.Random(seed)
    current_components = full_components(schedule)
    current_value = combine(current_components)
    best_value = current_value
    best_snapshot = {gid: g.date for gid, g in schedule.games.items()}
    temp = initial_temp
    history = []

    for i in range(iterations):
        changes = propose_move(schedule, rng, k=k)
        if changes is None:
            temp *= cooling_rate
            continue

        delta_components, undo = apply_and_get_delta(schedule, changes, teams_components)
        new_components = tuple(c + d for c, d in zip(current_components, delta_components))
        new_value = combine(new_components)
        delta = new_value - current_value

        accept = delta < 0 or rng.random() < math.exp(-delta / max(temp, 1e-9))

        if accept:
            current_components = new_components
            current_value = new_value
            if current_value < best_value:
                best_value = current_value
                best_snapshot = {gid: g.date for gid, g in schedule.games.items()}
        else:
            schedule.apply_date_changes(undo)

        temp *= cooling_rate
        if i % 1000 == 0:
            history.append((i, current_value, best_value, temp))

    return best_snapshot, best_value, history
