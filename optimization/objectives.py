"""Phase B objectives: fatigue burden (calibrated by Phase 3's cost model)
and carbon (pure geometry, no model). Both operate on a Schedule
(optimization/schedule.py) and both can report a per-team breakdown, per the
plan's mandatory fairness reporting.

See the Phase 5 planning discussion for why fatigue burden uses each team's
own raw (non-differential) exposure per game, weighted by |beta_k| from the
Phase 3 model, rather than reusing the training-time differential features
directly (which cancel to exactly zero summed over the whole league).
rest_days_diff has no burden term: it has no natural, non-arbitrary "zero
point" once it's not being compared to an opponent, and it wasn't
statistically significant in Phase 3 anyway (p=0.182) -- back_to_back
(p<0.001) already carries the "insufficient rest" signal.
"""
from modelling.cost_model import load_cost_model

# Boeing 757 -- the aircraft ~27 of the 30 NBA teams charter (Delta-operated).
# ~1,020 gal/hr fuel burn at long-range cruise (~529 mph, ICAO/manufacturer
# figures), 9.75 kg CO2e per gallon of jet fuel burnt. Whole-aircraft factor,
# not per-passenger -- charter emissions are per-aircraft (the plan's own
# framing), unlike a later commercial-flight comparison (Phase 6), which
# would need NBA_TRAVELING_PARTY_SIZE below as a per-passenger multiplier.
CHARTER_CO2_PER_MILE_KG = 1020 / 529 * 9.75  # ~= 18.8 kg CO2/mile

# Players, coaches, and staff -- not used in the charter formula above
# (per-aircraft, doesn't scale with headcount), reserved for Phase 6.
NBA_TRAVELING_PARTY_SIZE = 50


def total_miles(schedule, per_team=False):
    """Pure geometry -- same objective Phase A used, exposed here too so
    run_phase_b.py has one place to get every metric it needs to report."""
    if not per_team:
        return schedule.total_miles()
    return {
        team: sum(dist for _, dist in schedule.team_leg_distances(team))
        for team in schedule.teams
    }


def team_fatigue_burden(schedule, team, fatigue_weights):
    burden = 0.0
    for _gid, dist, _rest_days, is_back_to_back, timezones_crossed in schedule.team_leg_details(team):
        burden += abs(fatigue_weights['back_to_back_diff']) * (1.0 if is_back_to_back else 0.0)
        burden += abs(fatigue_weights['travel_miles_diff']) * dist
        burden += abs(fatigue_weights['timezones_shift_diff']) * (timezones_crossed or 0)
    return burden


def fatigue_burden(schedule, cost_model=None, per_team=False):
    """F(S): total league fatigue burden. Always >= 0 for every team, so the
    league total is a real, minimisable quantity (unlike the raw
    differential features it's derived from, which cancel to zero)."""
    cost_model = cost_model or load_cost_model()
    per_team_burden = {
        team: team_fatigue_burden(schedule, team, cost_model.fatigue_weights)
        for team in schedule.teams
    }
    if per_team:
        return per_team_burden
    return sum(per_team_burden.values())


def carbon(schedule, per_team=False):
    """C(S): total league carbon, charter/status-quo assumption (posterior
    transport-scenario comparisons are Phase 6, not here)."""
    per_team_carbon = {
        team: miles * CHARTER_CO2_PER_MILE_KG
        for team, miles in total_miles(schedule, per_team=True).items()
    }
    if per_team:
        return per_team_carbon
    return sum(per_team_carbon.values())


def normalize(value, low, high):
    """Min-max normalisation against fixed anchors (not recomputed per
    call) -- see run_phase_b.py for how the anchors are established (the
    lambda=0 and lambda=1 runs' own achievable extremes)."""
    if high <= low:
        return 0.0
    return (value - low) / (high - low)


def combined_score(F, C, F_low, F_high, C_low, C_high, lam):
    """(1 - lam)*F_norm + lam*C_norm -- the plan's Phase B objective for a
    single lambda. This scalarises one schedule's score for a given lambda;
    the actual Pareto frontier is the whole SET of (F, C) pairs across the
    lambda grid, not this single number (see the plan's "no fake exchange
    rate" principle)."""
    f_norm = normalize(F, F_low, F_high)
    c_norm = normalize(C, C_low, C_high)
    return (1 - lam) * f_norm + lam * c_norm
