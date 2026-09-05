"""Phase B: dual objective (fatigue burden + carbon) and a Pareto sweep over
lambda. Writes artifacts to optimization/artifacts/pareto_results/ for the
Streamlit app.

Normalisation anchors: rather than an arbitrary made-up scale, F (fatigue) 
and C (carbon) are each min-max normalised against two real reference points
-- the best that run alone (lambda=0 minimises raw F, lambda=1 minimises raw C)
can achieve, and the real schedule's own value. This means the real schedule 
always sits at exactly (1, 1) in normalised space, and any point below that 
on either axis is a genuine improvement over the status quo.

Known, disclosed gap (accepted as-is, not a bug -- see the Phase 5 build
discussion): lambda values around 0.7-0.9 have consistently landed on
essentially no improvement over the real schedule, across multiple seeds and
retuned calibration. The other 8 lambda points (0.0-0.6 and 1.0) all find
real improvement and already demonstrate the full range asked for --
maximise-fatigue-reduction, maximise-carbon-reduction, and several genuine
mixed tradeoffs in between (0.4 in particular beats the real schedule on
BOTH axes at once). The 0.7-0.9 gap only means that one narrow slice of the
"leans heavily toward carbon" region is less finely sampled -- revisit with
a better-tuned search later if finer resolution there ever matters, rather
than assume it's silently been fixed.
"""
import os
import random

import pandas as pd

from modelling.cost_model import load_cost_model
from optimization import objectives
from optimization.schedule import load_schedule
from optimization.search import calibrate_initial_temp, simulated_annealing

LAMBDA_GRID = [round(i / 10, 1) for i in range(11)]  # 0.0, 0.1, ..., 1.0
ITERATIONS = 25_000
SEEDS = [1, 2, 3]  # try a few, keep the best -- see the Phase 5 build discussion:
# every lambda run uses the same seed for both calibration and search, so
# different lambdas can land on genuinely unlucky trajectories by chance,
# the same seed-sensitivity already observed and discussed back in Phase 4.
ARTIFACTS_DIR = 'optimization/artifacts/pareto_results'


def _teams_components(schedule, teams, cost_model):
    f = sum(
        objectives.team_fatigue_burden(schedule, team, cost_model.fatigue_weights)
        for team in teams
    )
    c = sum(
        sum(dist for _, dist in schedule.team_leg_distances(team)) * objectives.CHARTER_CO2_PER_MILE_KG
        for team in teams
    )
    return (f, c)


def _full_components(schedule, cost_model):
    return (
        objectives.fatigue_burden(schedule, cost_model=cost_model),
        objectives.carbon(schedule),
    )


def _run_search_once(seed, cost_model, combine):
    schedule = load_schedule()
    teams_components = lambda sch, teams: _teams_components(sch, teams, cost_model)
    full_components = lambda sch: _full_components(sch, cost_model)

    # Each objective (raw F, raw C, or a normalised combined score) lives on
    # its own scale -- initial_temp=50.0 (search.py's default, tuned for
    # Phase A's miles scale) means something completely different for each
    # of these. Calibrate it fresh every time rather than reuse a number
    # tuned for a different objective.
    initial_temp = calibrate_initial_temp(
        schedule, teams_components, full_components, combine,
        rng=random.Random(seed),
    )

    return simulated_annealing(
        schedule,
        iterations=ITERATIONS,
        initial_temp=initial_temp,
        seed=seed,
        teams_components=teams_components,
        full_components=full_components,
        combine=combine,
    )


def _run_search(cost_model, combine):
    """Tries every seed in SEEDS, keeps the one with the lowest combine()
    score -- a single seed's trajectory can land somewhere genuinely unlucky
    (see the Phase 5 build discussion), so this is the standard, principled
    fix for stochastic local search, not just re-rolling until it looks good.
    """
    best_snapshot, best_value = None, None
    for seed in SEEDS:
        snapshot, value, _history = _run_search_once(seed, cost_model, combine)
        if best_value is None or value < best_value:
            best_snapshot, best_value = snapshot, value
    return best_snapshot, best_value


def _snapshot_to_metrics(snapshot, cost_model):
    """Reloads a fresh schedule, applies a saved date-snapshot, and computes
    every real metric via a full recompute -- avoids trusting any
    incrementally-tracked running total (same practice as run_phase_a.py).
    """
    schedule = load_schedule()
    schedule.apply_date_changes(snapshot)
    return schedule, {
        'total_miles': objectives.total_miles(schedule),
        'fatigue_burden': objectives.fatigue_burden(schedule, cost_model=cost_model),
        'carbon': objectives.carbon(schedule),
    }, {
        'total_miles': objectives.total_miles(schedule, per_team=True),
        'fatigue_burden': objectives.fatigue_burden(schedule, cost_model=cost_model, per_team=True),
        'carbon': objectives.carbon(schedule, per_team=True),
    }


def main():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    cost_model = load_cost_model()

    real_schedule = load_schedule()
    real_metrics = {
        'total_miles': objectives.total_miles(real_schedule),
        'fatigue_burden': objectives.fatigue_burden(real_schedule, cost_model=cost_model),
        'carbon': objectives.carbon(real_schedule),
    }
    real_per_team = {
        'total_miles': objectives.total_miles(real_schedule, per_team=True),
        'fatigue_burden': objectives.fatigue_burden(real_schedule, cost_model=cost_model, per_team=True),
        'carbon': objectives.carbon(real_schedule, per_team=True),
    }
    print(f"Real schedule: F={real_metrics['fatigue_burden']:.1f}  C={real_metrics['carbon']:,.0f}  "
          f"miles={real_metrics['total_miles']:,.1f}")

    print("\nEstablishing normalisation anchors...")
    f_snapshot, _ = _run_search(cost_model, combine=lambda c: c[0])
    _, f_anchor_metrics, _ = _snapshot_to_metrics(f_snapshot, cost_model)
    f_low = f_anchor_metrics['fatigue_burden']
    print(f"  lambda=0 (pure fatigue) best F: {f_low:.1f}")

    c_snapshot, _ = _run_search(cost_model, combine=lambda c: c[1])
    _, c_anchor_metrics, _ = _snapshot_to_metrics(c_snapshot, cost_model)
    c_low = c_anchor_metrics['carbon']
    print(f"  lambda=1 (pure carbon) best C: {c_low:,.0f}")

    f_high = real_metrics['fatigue_burden']
    c_high = real_metrics['carbon']
    print(f"  anchors: F in [{f_low:.1f}, {f_high:.1f}], C in [{c_low:,.0f}, {c_high:,.0f}]")

    summary_rows = [{
        'source': 'real', 'lambda': None,
        **real_metrics,
        'fatigue_burden_norm': objectives.normalize(real_metrics['fatigue_burden'], f_low, f_high),
        'carbon_norm': objectives.normalize(real_metrics['carbon'], c_low, c_high),
    }]
    per_team_rows = [
        {'source': 'real', 'lambda': None, 'team': team, **{
            metric: real_per_team[metric][team] for metric in real_per_team
        }}
        for team in real_schedule.teams
    ]
    schedule_rows = [
        {'source': 'real', 'lambda': None, 'game_id': g.game_id, 'date': g.date,
         'home_team': g.home_team, 'away_team': g.away_team}
        for g in real_schedule.games.values()
    ]

    print(f"\nSweeping lambda grid: {LAMBDA_GRID}")
    for lam in LAMBDA_GRID:
        combine = lambda c, lam=lam: objectives.combined_score(c[0], c[1], f_low, f_high, c_low, c_high, lam)
        snapshot, _ = _run_search(cost_model, combine=combine)
        schedule, metrics, per_team = _snapshot_to_metrics(snapshot, cost_model)

        source = f'lambda_{lam}'
        print(f"  {source}: F={metrics['fatigue_burden']:.1f}  C={metrics['carbon']:,.0f}  "
              f"miles={metrics['total_miles']:,.1f}")

        summary_rows.append({
            'source': source, 'lambda': lam,
            **metrics,
            'fatigue_burden_norm': objectives.normalize(metrics['fatigue_burden'], f_low, f_high),
            'carbon_norm': objectives.normalize(metrics['carbon'], c_low, c_high),
        })
        per_team_rows.extend(
            {'source': source, 'lambda': lam, 'team': team, **{
                metric: per_team[metric][team] for metric in per_team
            }}
            for team in schedule.teams
        )
        schedule_rows.extend(
            {'source': source, 'lambda': lam, 'game_id': g.game_id, 'date': g.date,
             'home_team': g.home_team, 'away_team': g.away_team}
            for g in schedule.games.values()
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_parquet(f'{ARTIFACTS_DIR}/summary.parquet', index=False)
    pd.DataFrame(per_team_rows).to_parquet(f'{ARTIFACTS_DIR}/per_team.parquet', index=False)
    pd.DataFrame(schedule_rows).to_parquet(f'{ARTIFACTS_DIR}/schedules.parquet', index=False)

    pd.set_option('display.float_format', lambda x: f'{x:,.1f}')
    print("\n=== Full summary ===")
    print(summary_df[['source', 'lambda', 'fatigue_burden', 'carbon', 'total_miles']].to_string(index=False))
    print(f"\nArtifacts written to {ARTIFACTS_DIR}/")


if __name__ == '__main__':
    main()
