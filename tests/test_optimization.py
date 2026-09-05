"""Focused unit tests for the deterministic, easy-to-get-wrong pieces of the
optimiser (per the project's testing agreement) -- not full coverage.
Everything else in optimization/ is verified via Phase 4's "Verify" criteria
(a real run against run_phase_a.py), not unit tests.
"""
import math
import random

import pytest

from optimization.geo import haversine_miles
from optimization.moves import (
    MAX_CONSECUTIVE_AWAY,
    _longest_away_streak,
    propose_move,
)
from optimization.schedule import load_schedule
from optimization.search import apply_and_get_delta


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def test_haversine_same_point_is_zero():
    assert haversine_miles(34.0430, -118.2673, 34.0430, -118.2673) == pytest.approx(0.0, abs=1e-6)


def test_haversine_known_distance_lal_to_bos():
    # Real-world great-circle LAX-area to Boston is ~2,600 miles -- also
    # cross-checked against the SQL macro's own output for this exact pair
    # back in Phase 2 (2592.2 miles).
    dist = haversine_miles(34.0430, -118.2673, 42.3662, -71.0621)
    assert dist == pytest.approx(2592.2, abs=1.0)


def test_haversine_is_symmetric():
    a_to_b = haversine_miles(41.8807, -87.6742, 40.7505, -73.9934)
    b_to_a = haversine_miles(40.7505, -73.9934, 41.8807, -87.6742)
    assert a_to_b == pytest.approx(b_to_a, abs=1e-9)


# ---------------------------------------------------------------------------
# Move feasibility invariants
# ---------------------------------------------------------------------------

@pytest.fixture
def schedule():
    """Function-scoped (fresh load per test) so tests that mutate the
    schedule (applying moves) can't leak state into other tests.
    """
    return load_schedule()


def _check_invariants(schedule):
    for team in schedule.teams:
        gids = schedule.team_schedule(team)
        assert len(gids) == 82, f'{team} has {len(gids)} games, expected 82'
        dates = [schedule.games[gid].date for gid in gids]
        assert len(dates) == len(set(dates)), f'{team} is double-booked'
        streak = _longest_away_streak(schedule, team)
        assert streak <= MAX_CONSECUTIVE_AWAY, f'{team} has a {streak}-game road trip (K={MAX_CONSECUTIVE_AWAY})'
    assert len(schedule.games) == 1230


def test_initial_schedule_is_feasible(schedule):
    _check_invariants(schedule)


def test_moves_preserve_feasibility(schedule):
    """After many applied random moves, every hard constraint (82
    games/team, no double-booking, the away-streak cap) must still hold --
    this is the actual safety net for moves.py, not code review.
    """
    rng = random.Random(123)
    applied = 0
    for _ in range(1500):
        changes = propose_move(schedule, rng)
        if changes is None:
            continue
        schedule.apply_date_changes(changes)
        applied += 1

    assert applied > 0, "no moves were ever applied -- test isn't exercising anything"
    _check_invariants(schedule)


# ---------------------------------------------------------------------------
# Incremental delta evaluation matches a full recompute
# ---------------------------------------------------------------------------

def test_incremental_delta_matches_full_recompute(schedule):
    """The whole point of incremental evaluation is speed without giving up
    correctness -- assert it agrees with the slow, obviously-correct full
    recompute across many random moves, not just one.
    """
    rng = random.Random(99)
    checked = 0
    for _ in range(200):
        changes = propose_move(schedule, rng)
        if changes is None:
            continue

        miles_before = schedule.total_miles()
        delta, undo = apply_and_get_delta(schedule, changes)
        miles_after_full_recompute = schedule.total_miles()

        assert miles_after_full_recompute - miles_before == pytest.approx(delta, abs=1e-6)
        checked += 1

        # leave the schedule as the (accepted) new state for the next iteration
        del undo

    assert checked > 0, "no moves were ever applied -- test isn't exercising anything"
