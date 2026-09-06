"""Phase 6: exports the small, static arena geography the Streamlit app needs
for its route maps and Schedule Board cards -- key (a team abbreviation, or a
neutral-site venue key like 'PARIS') -> lat/lon/utc_offset_hours/arena/city. A
one-off export, decoupled from the Phase 5 sweep: this data never changes
between lambda points or seeds, so there's no reason to make the app touch
DuckDB, or re-run run_phase_b.py's 25,000-iteration search, just to get
arena coordinates and names into its hands.

Coordinates/offsets reuse schedule.py's own arena_coords/arena_utc_offsets
dicts (already merged with the 3 neutral venues -- see NEUTRAL_VENUE_COORDS
in schedule.py), so there's one source of truth for that geography. Arena
NAME and city are display-only strings the optimiser itself never needs, so
they're not in schedule.py -- read directly from the nba_arenas seed here,
plus a small local dict for the 3 neutral venues' names.

Run once, or whenever transform/nba/seeds/nba_arenas.csv changes:
    python -m optimization.export_arenas
"""
import os

import duckdb
import pandas as pd

from optimization.run_phase_b import ARTIFACTS_DIR
from optimization.schedule import DUCKDB_PATH, load_schedule

NEUTRAL_VENUE_NAMES = {
    'MEXICO_CITY': ('Arena CDMX', 'Mexico City'),
    'VEGAS_CUP': ('T-Mobile Arena', 'Las Vegas'),
    'PARIS': ('Accor Arena', 'Paris'),
}


def main():
    schedule = load_schedule()

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    team_names = con.execute(
        'select team_abbreviation, arena, city from nba_arenas'
    ).df().set_index('team_abbreviation')
    con.close()

    rows = []
    for key, (lat, lon) in schedule.arena_coords.items():
        if key in NEUTRAL_VENUE_NAMES:
            arena_name, city = NEUTRAL_VENUE_NAMES[key]
        else:
            arena_name, city = team_names.loc[key, 'arena'], team_names.loc[key, 'city']
        rows.append({
            'key': key,
            'lat': lat,
            'lon': lon,
            'utc_offset_hours': schedule.arena_utc_offsets[key],
            'arena': arena_name,
            'city': city,
        })

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    pd.DataFrame(rows).to_parquet(f'{ARTIFACTS_DIR}/arenas.parquet', index=False)
    print(f"Wrote {len(rows)} arena/venue rows to {ARTIFACTS_DIR}/arenas.parquet")


if __name__ == '__main__':
    main()
