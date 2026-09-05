# sport-analytics — Project Context

## What This Project Does
Automated NBA data pipeline using a Medallion architecture:
- **Bronze**: Raw play-by-play JSON fetched from nba_api, uploaded to an S3-compatible bucket
- **Silver**: dbt staging models that clean and flatten the raw JSON (DuckDB reads the bucket directly via httpfs)
- **Gold**: dbt mart models with aggregated stats — game summaries, player stats, team stats

A second feature, **travel logistics**, sits alongside this: models NBA team travel across a
season (distances, fatigue, timezones) and runs schedule optimisation to explore lower-travel
alternative schedules. Full six-phase roadmap in `docs/travel-logistics-plan.md`. **Phases 1-5
are done** (ingestion, dbt travel models, fatigue cost model, local-search engine, dual
fatigue+carbon Pareto sweep); Phase 6 (transport scenarios + Streamlit) is next.

## Architecture — core pipeline
```
nba_api → ingestion/nba/play_by_play.py → bucket (bronze/nba_pbp/*.json)
                                               ↓
                   transform/nba/models/staging/stg_nba__play_by_play  (DuckDB view)
                                               ↓
                   transform/nba/models/marts/  (DuckDB tables)
                       ├── game_summary
                       ├── player_game_stats
                       └── team_game_stats
```
Airflow DAG `nba_pipeline_daily` chains all three layers every day at 08:00 UTC.

## Architecture — travel logistics
```
nba_api → ingestion/nba/{league_game_log,team_season_stats}.py → bucket (bronze/nba_game_log/,
                                                                          bronze/nba_team_stats/)
                                               ↓
   transform/nba/models/staging/{stg_nba__game_log, stg_nba__team_season_stats}  (DuckDB views)
                                               ↓
                   transform/nba/models/marts/travel/  (DuckDB tables)
                       ├── team_travel_legs             (per-team-game: distance, rest, b2b, tz)
                       ├── team_travel_season_summary    (per-team-season totals)
                       └── fatigue_features               (+ opponent, differential features, target)
                                               ↓
   modelling/{features,train,cost_model}.py  (Python, NOT dbt -- linear regression, fatigue weights)
                                               ↓
   optimization/{schedule,moves,search,geo,objectives,run_phase_a,run_phase_b}.py
                                               (Python, NOT dbt -- local search / SA, Pareto sweep)
```
This half is **offline, run-once-per-analysis** -- deliberately kept out of the daily Airflow DAG
(see the plan doc for why). `tests/test_optimization.py` covers the deterministic, easy-to-get-wrong
pieces (haversine, move feasibility, incremental delta vs. full recompute).

## Key File Locations
| Purpose | Path |
|---------|------|
| Bronze ingestion (core pipeline) | `ingestion/nba/play_by_play.py` |
| Bronze ingestion (travel logistics) | `ingestion/nba/{league_game_log,team_season_stats,config}.py` |
| Airflow DAG | `airflow/dags/nba_pipeline_dag.py` |
| dbt project root | `transform/nba/` |
| Silver models | `transform/nba/models/staging/` |
| Gold models (core pipeline) | `transform/nba/models/marts/` |
| Gold models (travel logistics) | `transform/nba/models/marts/travel/` |
| Arena seed (coords, timezone, UTC offset) | `transform/nba/seeds/nba_arenas.csv` |
| dbt macros | `transform/nba/macros/` |
| Fatigue cost model (Python, not dbt) | `modelling/` |
| Schedule optimiser (Python, not dbt) | `optimization/` |
| Optimiser unit tests | `tests/test_optimization.py` |
| Travel-logistics roadmap | `docs/travel-logistics-plan.md` |
| Storage credentials | `.env` (gitignored) |
| dbt connection | `transform/nba/profiles.yml` (gitignored) |

## Raw Bronze JSON Structure (play-by-play)
Each file: `s3://{S3_BUCKET_NAME}/bronze/nba_pbp/pbp_{game_id}_{date}.json`

The other two bronze sources (`nba_game_log`, `nba_team_stats`) are shaped very differently —
JSON arrays, not named objects — see the comments in `stg_nba__game_log.sql` /
`stg_nba__team_season_stats.sql` for their structure and the positional-extraction pattern used.

Top-level shape:
```json
{"game": {"gameId": "...", "actions": [...], "videoAvailable": 0}}
```
Each action object fields: `actionNumber, clock, period, teamId, teamTricode, personId,
playerName, actionType, subType, isFieldGoal, shotResult, shotDistance, pointsTotal,
scoreHome, scoreAway, description, location, videoAvailable, actionId`

The `clock` field uses ISO 8601 duration format: `"PT10M23.00S"` (time remaining in period).
The `parse_clock` macro in `transform/nba/macros/parse_clock.sql` converts it to seconds elapsed.

## dbt Model Lineage
```
S3 JSON (bronze)
    └── stg_nba__play_by_play  (staging, view — silver layer)
            ├── game_summary          (mart, table — gold)
            ├── player_game_stats     (mart, table — gold)
            └── team_game_stats       (mart, table — gold, joins the two above)

    ├── stg_nba__game_log            (staging, view — silver layer)
    └── stg_nba__team_season_stats   (staging, view — silver layer)
            └── team_travel_legs               (mart, table — gold)
                    ├── team_travel_season_summary  (mart, table — gold)
                    └── fatigue_features            (mart, table — gold; feeds modelling/, Python)
```

## Local Development Setup
```bash
# Activate virtual environment (Python 3.10 specifically -- apache-airflow==2.8.1
# doesn't support 3.12+; brew install python@3.10 if needed)
source venv/bin/activate

# Start Airflow locally (UI at http://localhost:8080)
airflow standalone

# Run dbt independently (useful during development)
export $(cat .env | xargs)          # load storage credentials into shell
cd transform/nba
dbt debug                           # verify connection to DuckDB + bucket
dbt seed                            # load the nba_arenas seed (needed before travel models)
dbt run --select staging            # run silver layer only
dbt run --select marts              # run gold layer only (includes marts/travel/)
dbt test                            # run all data quality tests
dbt docs generate && dbt docs serve # browse model docs in browser

# Travel-logistics Python layers (from repo root, outside transform/nba/)
python -m modelling.train           # refit the fatigue cost model, print RMSE/R² vs baseline
python -m optimization.run_phase_a  # run the local-search engine (Phase A, miles-only objective)
python -m optimization.run_phase_b  # Pareto sweep (Phase B, fatigue+carbon) -- writes parquet artifacts
python -m pytest tests/test_optimization.py -v   # optimiser unit tests
```

## Environment Variables
Stored in `.env` at project root (gitignored). Storage is **Backblaze B2, not AWS** (the original
AWS account was closed mid-project — see `docs/travel-logistics-plan.md` history if curious).
Required:
- `B2_KEY_ID` — B2's equivalent of an access key ID (NOT `AWS_ACCESS_KEY_ID`)
- `B2_APP_KEY` — B2's equivalent of a secret access key (NOT `AWS_SECRET_ACCESS_KEY`)
- `S3_BUCKET_NAME`
- `S3_REGION` — e.g. `eu-central-003` (B2's region naming, not AWS's)
- `S3_ENDPOINT_URL` — full URL, e.g. `https://s3.eu-central-003.backblazeb2.com` (used by boto3)
- `S3_ENDPOINT` — host only, e.g. `s3.eu-central-003.backblazeb2.com` (used by DuckDB httpfs)
- `S3_URL_STYLE` — `path` (B2 needs this; AWS S3 would use the default `vhost`)

If ever pointed back at real AWS S3: the credential var *names* (`B2_KEY_ID`/`B2_APP_KEY`) are
hardcoded in the ingestion scripts and `profiles.yml` — there's no fallback to `AWS_ACCESS_KEY_ID`
naming, so put the AWS credentials under those same `B2_*` names in `.env`. Only
`S3_ENDPOINT`/`S3_ENDPOINT_URL`/`S3_URL_STYLE` are truly optional and fall back to AWS-compatible
defaults when unset.

For dbt tasks in Airflow: the BashOperator passes `os.environ`, so export these before starting Airflow with `export $(cat .env | xargs)`.

## Scalability — Adding a New Sport or Data Source
The folder structure is designed so each sport is isolated:
1. Add `ingestion/{sport}/` with ingestion logic
2. Add `transform/{sport}/` as a new dbt project
3. Add a new DAG file in `airflow/dags/`

No changes to existing code required.

## Stack
- Python 3.10
- Apache Airflow 2.8.1 (SequentialExecutor + SQLite — local learning setup)
- Backblaze B2 (bronze layer storage, S3-compatible API), boto3
- dbt-duckdb + DuckDB with httpfs extension (reads the bucket directly, no data copy needed)
- nba_api v1.11.4
- statsmodels (Phase 3 fatigue cost model — linear OLS, not scikit-learn, for p-values/interpretability)
- pytest (optimiser unit tests only — see `tests/test_optimization.py`)

## Conventions
- dbt model naming: `stg_{source}__{entity}.sql` for staging, `{entity}.sql` for marts
- Staging models → views (always fresh, zero storage cost)
- Mart models → tables (fast reads for analysis)
- Tests and documentation live in `_staging.yml` / `_marts.yml` alongside models
- Reusable SQL logic goes in `transform/nba/macros/`

## Ways of working (learning project)

The goal of this project is for me to *understand* it, not just to have
working code. For every phase:

- Before writing code, explain what you're about to do and why, including
  the main design choices and what you considered and rejected. Wait for
  my OK.
- Prefer clear, conventional code over clever code. If a line isn't
  obvious, add a short comment on the intent.
- After implementing, give a concise walkthrough: what each new file does,
  how the pieces connect, and the 2–3 files I should read closely to
  understand this phase.
- For verification, tell me what we should check and why, and let me run
  at least one check myself (a query, a script, a number to inspect)
  before you confirm it works. Don't just tell me tests passed.
- Assume I'll ask "why this and not that" — pre-empt it.
