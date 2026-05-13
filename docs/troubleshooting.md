# Troubleshooting

## Most Common Issue — Missing Credentials

**Symptom:** Any S3 or dbt error mentioning `AccessDenied`, `No credentials`, `Authentication Failure`.

**Cause:** The `.env` variables are not exported in your current terminal session.

**Fix:**
```bash
export $(cat .env | xargs)
source venv/bin/activate
```

This must be done **every time you open a new terminal**. It is not permanent.

If you also get this error when running the Airflow DAG (but not when running dbt manually), it means Airflow was started without the credentials. Restart Airflow:
```bash
export $(cat .env | xargs) && source venv/bin/activate && airflow standalone
```

---

## `dbt debug` Errors

### `profiles.yml file [ERROR file not found]`
The file does not exist yet — you need to create it manually. It is gitignored so it was not included in the repo. See the Setup section in the README for the content.

### `Connection test: [ERROR]`
The DuckDB connection itself failed. Check:
1. The `path:` in `profiles.yml` is an absolute path (not relative)
2. The directory containing `nba.duckdb` exists
3. Your AWS credentials are exported and correct

Run `dbt debug` again after exporting credentials:
```bash
export $(cat .env | xargs) && dbt debug --project-dir transform/nba --profiles-dir transform/nba
```

---

## `dbt run` Errors

### `UNNEST() can only be applied to lists, structs and NULL, not JSON`
The staging model requires an explicit cast. Make sure this line reads:
```sql
unnest(cast(game->'actions' as json[])) as action   -- correct
```
Not:
```sql
unnest(game->'actions') as action   -- wrong
```

### `HTTP 403 Forbidden` / `AccessDenied`
S3 credentials are not set or are wrong. Run:
```bash
export $(cat .env | xargs)
dbt run --project-dir transform/nba --profiles-dir transform/nba
```

If the error persists, verify the credentials work:
```python
import boto3, os
from dotenv import load_dotenv
load_dotenv()
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
print(s3.list_buckets())
```

### `HTTP 403` with correct credentials — wrong region
DuckDB's `httpfs` must have the correct S3 region. If your bucket is in `us-east-1` but `profiles.yml` says `eu-west-1`, reads will fail. Update `profiles.yml`:
```yaml
settings:
  s3_region: "us-east-1"   # must match your bucket's actual region
```

Or add `AWS_REGION=us-east-1` to your `.env` file.

### `Binder Error: Values list does not have a column named "X"`
A downstream model references a column that doesn't exist in an upstream model. Check that all column names in `team_game_stats.sql` match what `player_game_stats.sql` actually outputs. Run:
```bash
dbt compile --project-dir transform/nba --profiles-dir transform/nba
```
This generates compiled SQL in `transform/nba/target/compiled/` without executing it — useful for debugging column references.

### `dbt run` succeeds but gold tables show stale data
The gold tables are only updated when you run `dbt run`. The silver view is always fresh. Re-run to refresh:
```bash
export $(cat .env | xargs)
dbt run --project-dir transform/nba --profiles-dir transform/nba
```

---

## Ingestion Errors

### `No games found for {date}`
The NBA regular season runs roughly October–June. Running the ingestion script outside that window returns no games — this is expected behavior, not a bug.

### `nba_api` timeout or connection error
The NBA Stats API occasionally rate-limits or goes down. The ingestion script includes a `time.sleep(1.5)` between API calls to reduce this. If you hit repeated timeouts, wait a few minutes and try again. The Airflow DAG is configured to retry twice with a 5-minute delay.

### Game file already exists in S3
The ingestion uses `put_object` which overwrites any existing file with the same key. If the same game is ingested twice (e.g., from two different DAG runs), the second upload silently replaces the first. This is intentional — idempotent behavior.

---

## Airflow Errors

### DAG not appearing in the UI
Airflow scans the `dags/` folder periodically (every 30 seconds by default). Wait a moment and refresh the page. If the DAG still doesn't appear, check for import errors:
```bash
airflow dags list-import-errors
```

### `ModuleNotFoundError: No module named 'ingestion'`
The DAG file adds `project_root` to `sys.path`. If Airflow is started from a different directory, the path may be wrong. The DAG uses:
```python
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
```
Verify this resolves correctly by printing it in a Python shell from the `airflow/dags/` directory.

### dbt task fails in Airflow but works manually
The BashOperator inherits the environment of the Airflow process. If dbt works when you run it manually but fails in the DAG, Airflow was likely started without the AWS credentials. Restart:
```bash
export $(cat .env | xargs) && source venv/bin/activate && airflow standalone
```

### Old DAG `nba_bronze_ingestion_daily` still showing in the UI
This DAG was replaced by `nba_pipeline_daily`. The old file was deleted, but Airflow keeps metadata about deleted DAGs. You can hide it in the UI by toggling the DAG to "paused" or by running:
```bash
airflow dags delete nba_bronze_ingestion_daily
```

---

## DuckDB File Errors

### `nba.duckdb` file not found when running Python queries
The DuckDB file is created by `dbt run`. If you haven't run dbt yet (or the file was deleted), run:
```bash
export $(cat .env | xargs)
dbt run --project-dir transform/nba --profiles-dir transform/nba
```

### DuckDB file locked — `database is locked`
Only one process can write to a DuckDB file at a time. If you have a Python session with an open connection and then try to run `dbt run`, you will get a lock error. Close any open DuckDB connections first:
```python
con.close()
```

---

## Data Questions

### Why are rebounds/turnovers/fouls counts higher than expected?
The `Rebound` action type includes both individual rebounds and team rebounds (e.g., when a ball goes out of bounds after a missed shot). All of these are attributed to a player_name in the API, so they all count. The numbers may look slightly higher than official box scores for this reason.

### Why are there no assist/block/steal columns?
In the NBA PBP v3 API, assists, blocks, and steals are **not separate action types**. They appear as attributes embedded inside the description of other actions:
- Assist: `"Reaves 28' 3PT Jump Shot (21 PTS) (Vanderbilt 1 AST)"` — credited to Reaves (scorer), but Vanderbilt assisted
- Block: `"Robinson 3' Layup (Block by Turner)"` — credited to Robinson, but Turner blocked

Extracting these requires regex parsing of the `description` column. This is a planned future enhancement. See [extending.md](extending.md) for how to add new fields.

### Why does the staging view take 30–40 seconds to query?
Because it reads directly from S3 every time. DuckDB streams the JSON files over HTTPS. For development, use the gold tables (`game_summary`, `player_game_stats`, `team_game_stats`) which are stored locally and query in milliseconds. Only query the staging view when you need to inspect raw data or debug transformations.
