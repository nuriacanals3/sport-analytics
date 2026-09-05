"""Phase A: pure geometric objective (total league miles, no fatigue model).
Validates the whole search engine cheaply before Phase B adds the model-based
fatigue objective on top.
"""
from optimization.moves import MAX_CONSECUTIVE_AWAY, _longest_away_streak
from optimization.schedule import load_schedule
from optimization.search import simulated_annealing


def check_feasible(schedule):
    for team in schedule.teams:
        gids = schedule.team_schedule(team)
        assert len(gids) == 82, f'{team} has {len(gids)} games, expected 82'
        dates = [schedule.games[gid].date for gid in gids]
        assert len(dates) == len(set(dates)), f'{team} is double-booked'
        streak = _longest_away_streak(schedule, team)
        assert streak <= MAX_CONSECUTIVE_AWAY, f'{team} has a {streak}-game road trip'
    assert len(schedule.games) == 1230


def main():
    schedule = load_schedule()
    baseline_miles = schedule.total_miles()
    print(f"Real (baseline) schedule: {baseline_miles:,.1f} total miles")
    check_feasible(schedule)
    print("Baseline schedule: feasible.")

    best_snapshot, best_miles, history = simulated_annealing(schedule, iterations=25_000, seed=1)

    print(f"\nBest schedule found: {best_miles:,.1f} total miles")
    print(f"Reduction vs. real schedule: {baseline_miles - best_miles:,.1f} miles "
          f"({(baseline_miles - best_miles) / baseline_miles:.1%})")

    print("\n--- progress (iteration, current, best, temperature) ---")
    for i, current, best, temp in history[::5]:
        print(f"  {i:>6}  current={current:>12,.1f}  best={best:>12,.1f}  T={temp:.3f}")

    # Rebuild the best schedule found and verify it independently -- not
    # just trusting the running "best_miles" tracked during the search.
    best_schedule = load_schedule()
    best_schedule.apply_date_changes(best_snapshot)
    check_feasible(best_schedule)
    print("\nBest schedule: feasible (independently verified).")

    recomputed_best_miles = best_schedule.total_miles()
    print(f"Best schedule total miles (independently recomputed): {recomputed_best_miles:,.1f}")
    assert abs(recomputed_best_miles - best_miles) < 1.0, \
        "tracked best_miles disagrees with an independent full recompute"

    assert best_miles <= baseline_miles, "search produced a WORSE schedule than reality"
    print(f"\nVerify: best ({best_miles:,.1f}) <= real baseline ({baseline_miles:,.1f}): "
          f"{best_miles <= baseline_miles}")


if __name__ == '__main__':
    main()
