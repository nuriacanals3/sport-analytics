# Sport Analytics

A local data pipeline that fetches NBA play-by-play data from the NBA Stats API, transforms it with dbt, and produces aggregated game and season metrics.

Built to learn the modern data stack end-to-end: Airflow for orchestration, S3 for raw storage, and dbt + DuckDB for transformation.

---

## Architecture

Medallion architecture (Bronze → Silver → Gold):

```
NBA Stats API
      │
      ▼
[Bronze]  Raw JSON uploaded to S3
          s3://bucket/bronze/nba_pbp/pbp_{game_id}_{date}.json
      │
      ▼
[Silver]  dbt staging model — DuckDB reads S3 directly, flattens JSON
      │
      ▼
[Gold]    dbt mart models — aggregated tables stored in DuckDB
          ├── game_summary
          ├── player_game_stats
          └── team_game_stats
```

Orchestrated by an **Airflow DAG** running daily at 08:00 UTC:
```
extract_nba_api_to_s3 → dbt_run_silver → dbt_run_gold → dbt_test
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow 2.8.1 |
| Data source | [nba_api](https://github.com/swar/nba_api) |
| Storage | AWS S3 + boto3 |
| Transformation | dbt-duckdb 1.10.1 |
| Local warehouse | DuckDB (reads S3 via httpfs) |
| Language | Python 3.10 |

---

## Project Structure

```
sport-analytics/
├── ingestion/nba/play_by_play.py     # Bronze: fetch from API and upload to S3
├── airflow/dags/nba_pipeline_dag.py  # Airflow DAG chaining all layers
└── transform/nba/                    # dbt project
    ├── models/staging/               # Silver: clean and flatten raw JSON
    └── models/marts/                 # Gold: aggregated game and player stats
```

Adding a new sport means creating `ingestion/{sport}/` and `transform/{sport}/` — no changes to existing code.

---

## Setup

### Prerequisites
- Python 3.10+
- AWS account with an S3 bucket and an IAM user with `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` permissions

### Install

```bash
git clone https://github.com/nuriacanals3/sport-analytics.git
cd sport-analytics
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configure credentials

Create a `.env` file at the project root (already gitignored):

```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET_NAME=your-bucket-name
AWS_REGION=eu-west-1
```

Create `transform/nba/profiles.yml` (already gitignored):

```yaml
nba_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/sport-analytics/transform/nba/nba.duckdb
      extensions:
        - httpfs
      settings:
        s3_region: "{{ env_var('AWS_REGION', 'eu-west-1') }}"
        s3_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
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

**Full pipeline via Airflow:** trigger the `nba_pipeline_daily` DAG from the UI.

**Or run each step manually:**
```bash
# Bronze
python ingestion/nba/play_by_play.py

# Silver
dbt run --select staging --project-dir transform/nba --profiles-dir transform/nba

# Gold
dbt run --select marts --project-dir transform/nba --profiles-dir transform/nba

# Tests
dbt test --project-dir transform/nba --profiles-dir transform/nba
```

---

## Querying the Data

Gold tables are stored in `transform/nba/nba.duckdb`:

```python
import duckdb

con = duckdb.connect('transform/nba/nba.duckdb')

# Top scorers
con.execute("SELECT player_name, team_tricode, points_scored FROM player_game_stats ORDER BY points_scored DESC LIMIT 10").df()

# Games that went to overtime
con.execute("SELECT game_id, final_score_home, final_score_away, periods_played FROM game_summary WHERE went_to_overtime = true").df()
```

See [docs/querying.md](docs/querying.md) for more example queries and a full column reference.

---

## Documentation

Detailed documentation is in the [`docs/`](docs/) folder:

- [**docs/architecture.md**](docs/architecture.md) — how each layer works, data flow, design decisions
- [**docs/dbt_models.md**](docs/dbt_models.md) — dbt concepts, model explanations, how to add new models
- [**docs/querying.md**](docs/querying.md) — how to query the data, column reference, example queries
- [**docs/extending.md**](docs/extending.md) — how to add a new sport, new analysis, or new API endpoint
- [**docs/troubleshooting.md**](docs/troubleshooting.md) — common errors and how to fix them
