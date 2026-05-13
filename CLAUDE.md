# sport-analytics — Project Context

## What This Project Does
Automated NBA data pipeline using a Medallion architecture:
- **Bronze**: Raw play-by-play JSON fetched from nba_api, uploaded to S3
- **Silver**: dbt staging models that clean and flatten the raw JSON (DuckDB reads S3 directly via httpfs)
- **Gold**: dbt mart models with aggregated stats — game summaries, player stats, team stats

## Architecture
```
nba_api → ingestion/nba/play_by_play.py → S3 (bronze/nba_pbp/*.json)
                                               ↓
                   transform/nba/models/staging/stg_nba__play_by_play  (DuckDB view)
                                               ↓
                   transform/nba/models/marts/  (DuckDB tables)
                       ├── game_summary
                       ├── player_game_stats
                       └── team_game_stats
```
Airflow DAG `nba_pipeline_daily` chains all three layers every day at 08:00 UTC.

## Key File Locations
| Purpose | Path |
|---------|------|
| Bronze ingestion | `ingestion/nba/play_by_play.py` |
| Airflow DAG | `airflow/dags/nba_pipeline_dag.py` |
| dbt project root | `transform/nba/` |
| Silver models | `transform/nba/models/staging/` |
| Gold models | `transform/nba/models/marts/` |
| dbt macros | `transform/nba/macros/` |
| AWS credentials | `.env` (gitignored) |
| dbt connection | `transform/nba/profiles.yml` (gitignored) |

## Raw S3 JSON Structure
Each file: `s3://{S3_BUCKET_NAME}/bronze/nba_pbp/pbp_{game_id}_{date}.json`

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
```

## Local Development Setup
```bash
# Activate virtual environment
source venv/bin/activate

# Start Airflow locally (UI at http://localhost:8080)
airflow standalone

# Run dbt independently (useful during development)
export $(cat .env | xargs)          # load AWS credentials into shell
cd transform/nba
dbt debug                           # verify connection to DuckDB + S3
dbt run --select staging            # run silver layer only
dbt run --select marts              # run gold layer only
dbt test                            # run all data quality tests
dbt docs generate && dbt docs serve # browse model docs in browser
```

## Environment Variables
Stored in `.env` at project root (gitignored). Required:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `S3_BUCKET_NAME` — currently `nba-bronze-layer`
- `AWS_REGION` — defaults to `eu-west-1` if not set

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
- AWS S3 (bronze layer storage), boto3
- dbt-duckdb + DuckDB with httpfs extension (reads S3 directly, no data copy needed)
- nba_api v1.11.4

## Conventions
- dbt model naming: `stg_{source}__{entity}.sql` for staging, `{entity}.sql` for marts
- Staging models → views (always fresh, zero storage cost)
- Mart models → tables (fast reads for analysis)
- Tests and documentation live in `_staging.yml` / `_marts.yml` alongside models
- Reusable SQL logic goes in `transform/nba/macros/`
