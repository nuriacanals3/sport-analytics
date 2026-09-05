# Sport Analytics

A local data pipeline that fetches NBA data from the NBA Stats API, transforms it with dbt, and produces aggregated game, player, and travel-logistics metrics.

Built to learn the modern data stack end-to-end: Airflow for orchestration, S3-compatible object storage for raw data, and dbt + DuckDB for transformation.

Two things live here:
1. **The core pipeline** — play-by-play ingestion → game/player/team box-score marts. Working, orchestrated daily.
2. **Travel logistics** (in progress) — models NBA team travel across a season (distances, rest, back-to-backs, timezone crossings), trains a fatigue cost model, and runs schedule optimisation (including a dual fatigue+carbon Pareto sweep) to explore lower-travel, lower-fatigue, lower-carbon alternative schedules. See [docs/travel-logistics-plan.md](docs/travel-logistics-plan.md) for the full six-phase roadmap; **Phases 1–5 are done** (ingestion, dbt travel models, fatigue cost model, local-search engine, Pareto sweep) — Phase 6 (transport scenarios + Streamlit) is next.

---

## Architecture

Medallion architecture (Bronze → Silver → Gold):

```
NBA Stats API
      │
      ├── play_by_play.py         ──▶ bronze/nba_pbp/*.json
      ├── league_game_log.py      ──▶ bronze/nba_game_log/*.json
      └── team_season_stats.py    ──▶ bronze/nba_team_stats/*.json
      │
      ▼
[Bronze]  Raw JSON uploaded to an S3-compatible bucket (AWS S3, Backblaze B2, etc.)
      │
      ▼
[Silver]  dbt staging models — DuckDB reads the bucket directly (httpfs), flattens JSON
          ├── stg_nba__play_by_play
          ├── stg_nba__game_log
          └── stg_nba__team_season_stats
      │
      ▼
[Gold]    dbt mart models — aggregated tables stored in DuckDB
          ├── game_summary, player_game_stats, team_game_stats
          └── travel/
              ├── team_travel_legs            (per-team-game: distance, rest, back-to-back, timezone crossing)
              ├── team_travel_season_summary  (per-team-season totals — a diagnostic view)
              └── fatigue_features            (team-game grain + opponent, differential features + target)
      │
      ▼  (Python, NOT dbt -- dbt's job stops at producing the feature table)
[Model]   modelling/{features,train,cost_model}.py
          Linear regression (statsmodels) on fatigue_features -> fatigue-feature weights
      │
      ▼
[Optimise] optimization/{schedule,moves,search,geo,objectives,run_phase_a,run_phase_b}.py
          Local search (simulated annealing) over the real season's schedule, feasibility-
          preserving moves, incremental delta evaluation -- Phase A objective is pure miles;
          Phase B sweeps lambda over a fatigue+carbon Pareto frontier, writes parquet artifacts
```

Orchestrated by an **Airflow DAG** running daily at 08:00 UTC (currently wires up the play-by-play path; the travel-logistics ingestion/dbt models/Python layers all run manually for now — see [docs/travel-logistics-plan.md](docs/travel-logistics-plan.md) section 4 on why those stay out of the daily DAG):
```
extract_nba_api_to_s3 → dbt_run_silver → dbt_run_gold → dbt_test
```

**What's still ahead** (Phase 6 of the travel-logistics plan, not built yet): a carbon/transport-scenario layer (commercial/SAF comparisons against the charter baseline) and a Streamlit app reading the precomputed Pareto artifacts.

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow 2.8.1 |
| Data source | [nba_api](https://github.com/swar/nba_api) |
| Storage | Any S3-compatible bucket (AWS S3, Backblaze B2, Cloudflare R2, ...) + boto3 |
| Transformation | dbt-duckdb 1.10.1 |
| Local warehouse | DuckDB (reads the bucket via `httpfs`) |
| Fatigue model | statsmodels (linear OLS — chosen over scikit-learn for p-values/interpretability) |
| Optimiser tests | pytest |
| Language | Python 3.10 |

---

## Project Structure

```
sport-analytics/
├── ingestion/nba/
│   ├── play_by_play.py           # Bronze: play-by-play per game
│   ├── league_game_log.py        # Bronze: team-game results per season
│   ├── team_season_stats.py      # Bronze: team strength (Base + Advanced) per season
│   └── config.py                 # Shared SEASONS list -- keeps the two above in sync
├── airflow/dags/nba_pipeline_dag.py  # Airflow DAG chaining the play-by-play path
├── docs/
│   └── travel-logistics-plan.md  # Roadmap for the travel-logistics feature
├── transform/nba/                # dbt project
│   ├── seeds/nba_arenas.csv      # Static arena coordinates, timezone, UTC offset
│   ├── macros/                   # Reusable SQL: parse_clock, haversine_miles
│   ├── models/staging/           # Silver: clean and flatten raw JSON
│   └── models/marts/
│       ├── game_summary.sql, player_game_stats.sql, team_game_stats.sql
│       └── travel/                # Gold: travel + fatigue models
├── modelling/                     # Python, NOT dbt -- fatigue cost model
│   ├── features.py, train.py, cost_model.py
│   └── artifacts/fatigue_cost_model.pkl  # gitignored, regenerable via train.py
├── optimization/                  # Python, NOT dbt -- schedule optimiser
│   ├── schedule.py                # league schedule structure, neutral-site game handling
│   ├── moves.py                   # feasibility-preserving moves
│   ├── search.py                  # simulated annealing, incremental delta evaluation
│   ├── geo.py                     # haversine (Python reimplementation, for the search's tight loop)
│   ├── objectives.py              # fatigue burden (uses the cost model) + carbon
│   ├── run_phase_a.py             # Phase A: pure-miles objective, validates the engine
│   ├── run_phase_b.py             # Phase B: lambda sweep -> Pareto artifacts (parquet)
│   └── artifacts/pareto_results/  # gitignored, regenerable via run_phase_b.py
└── tests/test_optimization.py    # haversine, move feasibility, incremental delta vs. full recompute
```

Adding a new sport means creating `ingestion/{sport}/` and `transform/{sport}/` — no changes to existing code.

---

## Setup

### Prerequisites
- Python 3.10 specifically (not newer — `apache-airflow==2.8.1` doesn't support 3.12+; check with `python3.10 --version`, install via `brew install python@3.10` if missing)
- An S3-compatible storage account with a bucket: AWS S3, or a free-forever alternative like [Backblaze B2](https://www.backblaze.com/sign-up) (10GB free, no expiring trial)

### Install

```bash
git clone https://github.com/nuriacanals3/sport-analytics.git
cd sport-analytics
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configure credentials

Create a `.env` file at the project root (already gitignored):

```
# Credentials -- names are generic S3-style even if using AWS directly.
# For a non-AWS provider (Backblaze B2, Cloudflare R2, ...) these are still
# just "access key id" / "secret access key" under a provider-specific name.
B2_KEY_ID=your_key_id
B2_APP_KEY=your_app_key
S3_BUCKET_NAME=your-bucket-name
S3_REGION=your-bucket-region          # e.g. eu-west-1 (AWS) or eu-central-003 (B2)

# Only needed for a non-AWS S3-compatible provider. Leave unset for real AWS S3.
S3_ENDPOINT_URL=https://s3.<region>.<provider-domain>   # full URL, used by boto3
S3_ENDPOINT=s3.<region>.<provider-domain>                # host only, used by DuckDB httpfs
S3_URL_STYLE=path                                         # most non-AWS providers need "path"
```

Create `transform/nba/profiles.yml` (already gitignored):

```yaml
nba_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: nba.duckdb
      extensions:
        - httpfs
      settings:
        s3_region: "{{ env_var('S3_REGION', 'eu-west-1') }}"
        s3_access_key_id: "{{ env_var('B2_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('B2_APP_KEY') }}"
        s3_endpoint: "{{ env_var('S3_ENDPOINT', '') }}"
        s3_url_style: "{{ env_var('S3_URL_STYLE', 'vhost') }}"
```

### Initialize Airflow

```bash
airflow standalone
# UI available at http://localhost:8080
```

---

## Running the Pipeline

Load credentials in every new terminal session:
```bash
export $(cat .env | xargs)
source venv/bin/activate
```

**Full play-by-play pipeline via Airflow:** trigger the `nba_pipeline_daily` DAG from the UI.

**Or run each step manually:**
```bash
# Bronze -- play-by-play
python -m ingestion.nba.play_by_play

# Bronze -- travel-logistics sources (multi-season, run once per analysis, not daily)
python -m ingestion.nba.league_game_log
python -m ingestion.nba.team_season_stats

# Seed -- arena coordinates (needed before travel models)
dbt seed --project-dir transform/nba --profiles-dir transform/nba

# Silver
dbt run --select staging --project-dir transform/nba --profiles-dir transform/nba

# Gold -- this already includes travel/ (it's nested under marts/)
dbt run --select marts --project-dir transform/nba --profiles-dir transform/nba

# Tests
dbt test --project-dir transform/nba --profiles-dir transform/nba

# Python -- fatigue cost model (Phase 3), reads fatigue_features, no credentials needed
python -m modelling.train

# Python -- schedule optimiser unit tests, then Phase A (miles-only), then Phase B (Pareto sweep)
python -m pytest tests/test_optimization.py -v
python -m optimization.run_phase_a
python -m optimization.run_phase_b
```

---

## Querying the Data

Gold tables are stored in `transform/nba/nba.duckdb` — mart tables (`game_summary`, `team_travel_legs`, `fatigue_features`, ...) are physically materialized there, so they need no credentials to query. Staging models are DuckDB *views* — querying them re-reads the bucket live, so they need `httpfs` credentials set first.

```python
import duckdb

con = duckdb.connect('transform/nba/nba.duckdb')

# Top scorers
con.execute("SELECT player_name, team_tricode, points_scored FROM player_game_stats ORDER BY points_scored DESC LIMIT 10").df()

# Games that went to overtime
con.execute("SELECT game_id, final_score_home, final_score_away, periods_played FROM game_summary WHERE went_to_overtime = true").df()

# Which teams travel the most?
con.execute("SELECT team_abbreviation, total_miles, total_back_to_backs FROM team_travel_season_summary WHERE season = '2024-25' ORDER BY total_miles DESC LIMIT 10").df()
```

**Exploring interactively:** `transform/nba/open_duckdb.sh` opens a DuckDB CLI session with bucket credentials pre-loaded (needed only for the staging views — mart tables work with a plain `duckdb nba.duckdb`, no script needed):
```bash
cd transform/nba
./open_duckdb.sh
```

See [docs/querying.md](docs/querying.md) for more example queries and a full column reference.

---

## Documentation

Detailed documentation is in the [`docs/`](docs/) folder:

- [**docs/travel-logistics-plan.md**](docs/travel-logistics-plan.md) — the six-phase roadmap for the travel/fatigue/optimisation feature: what's built, what's next, and the design principles behind it
- [**docs/architecture.md**](docs/architecture.md) — how each layer works, data flow, design decisions
- [**docs/dbt_models.md**](docs/dbt_models.md) — dbt concepts, model explanations, how to add new models
- [**docs/querying.md**](docs/querying.md) — how to query the data, column reference, example queries
- [**docs/extending.md**](docs/extending.md) — how to add a new sport, new analysis, or new API endpoint
- [**docs/troubleshooting.md**](docs/troubleshooting.md) — common errors and how to fix them
