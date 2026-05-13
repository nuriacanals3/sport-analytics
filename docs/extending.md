# Extending the Pipeline

## Adding a New Sport or Data Source

The folder structure isolates each sport completely. Adding a new sport requires **zero changes to existing code**.

### Step 1 — Create the ingestion package

```
ingestion/
└── {sport}/
    ├── __init__.py         (empty)
    └── {endpoint}.py       (your fetch + upload logic)
```

Model your new file on `ingestion/nba/play_by_play.py`. The pattern is:

```python
import os, json, boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = 'bronze/{sport}/{data_type}/'  # unique prefix per sport/endpoint


def fetch_data(...):
    # call your API here
    return data_dict


def upload_to_s3(data, filename):
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_PREFIX}{filename}",
        Body=json.dumps(data),
        ContentType='application/json'
    )


def main():
    data = fetch_data(...)
    upload_to_s3(data, 'filename.json')


if __name__ == '__main__':
    main()
```

### Step 2 — Create a dbt project

```
transform/
└── {sport}/
    ├── dbt_project.yml
    ├── profiles.yml         (gitignored, create locally)
    ├── models/
    │   ├── sources.yml
    │   ├── staging/
    │   └── marts/
    └── macros/
```

Copy `transform/nba/dbt_project.yml` and change:
- `name:` → new sport name
- `profile:` → new profile name (e.g. `soccer_duckdb`)

Add the new profile to your local `profiles.yml` (or use the same DuckDB file with a different schema):

```yaml
soccer_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/sport-analytics/transform/soccer/soccer.duckdb
      extensions:
        - httpfs
      settings:
        s3_region: "{{ env_var('AWS_REGION', 'eu-west-1') }}"
        s3_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

### Step 3 — Add an Airflow DAG

Create `airflow/dags/{sport}_pipeline_dag.py`. Copy `nba_pipeline_dag.py` and change:
- The DAG ID (e.g. `soccer_pipeline_daily`)
- The import of the ingestion module
- `DBT_PROJECT_DIR` to point at `transform/{sport}`

---

## Adding a New Analysis (Gold Model)

### Adding a new aggregation model

Create a `.sql` file in `transform/nba/models/marts/`. It just needs a `SELECT` referencing existing models via `{{ ref(...) }}`.

**Example — season averages:**

```sql
-- transform/nba/models/marts/player_season_averages.sql
select
    player_id,
    player_name,
    team_tricode,
    count(distinct game_id)               as games_played,
    round(avg(points_scored), 1)          as ppg,
    round(avg(field_goals_attempted), 1)  as fga_per_game,
    round(avg(fg_pct), 3)                 as avg_fg_pct,
    round(avg(rebounds), 1)               as rpg,
    round(avg(turnovers), 1)              as topg
from {{ ref('player_game_stats') }}
where points_scored > 0
group by player_id, player_name, team_tricode
order by ppg desc
```

Run it:
```bash
export $(cat .env | xargs) && source venv/bin/activate
dbt run --select player_season_averages --project-dir transform/nba --profiles-dir transform/nba
```

Then document it in `_marts.yml`:
```yaml
- name: player_season_averages
  description: "Season averages per player across all ingested games"
  columns:
    - name: player_id
      tests: [not_null]
    - name: ppg
      description: "Points per game"
```

### Adding a model that joins two existing models

```sql
-- transform/nba/models/marts/player_game_with_context.sql
select
    p.*,
    g.final_score_home,
    g.final_score_away,
    g.winner_location,
    g.went_to_overtime,
    -- Did this player's team win?
    case
        when p.team_id = home_team_id and g.winner_location = 'home' then true
        when p.team_id = away_team_id and g.winner_location = 'away' then true
        else false
    end as player_team_won
from {{ ref('player_game_stats') }} p
join {{ ref('game_summary') }} g on p.game_id = g.game_id
```

---

## Adding New Fields to the Staging Model

The staging model currently extracts a subset of fields from the raw JSON. The raw JSON contains more fields that are available to add.

**How to see all available fields:**
```python
import json, boto3, os
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
obj = s3.get_object(
    Bucket=os.getenv('S3_BUCKET_NAME'),
    Key='bronze/nba_pbp/pbp_0022500938_2026-03-10.json'
)
data = json.loads(obj['Body'].read())

# See all top-level game fields
print(data['game'].keys())

# See all fields available per action
print(data['game']['actions'][0].keys())
```

**Adding a field to the staging model:**

Edit `stg_nba__play_by_play.sql` and add a line in the `cleaned` CTE:

```sql
-- Example: add the assisting player ID (useful for computing assist stats)
action->>'assistPersonId'           as assist_player_id,
action->>'assistPlayerNameInitial'  as assist_player_name_initial,

-- Example: add the blocking player ID
action->>'blockPersonId'            as block_player_id,
```

After editing the staging model, re-run the full pipeline to rebuild the gold tables:
```bash
export $(cat .env | xargs) && source venv/bin/activate
dbt run --project-dir transform/nba --profiles-dir transform/nba
```

---

## Adding a New Ingestion Endpoint (Same Sport)

If the NBA API has other data you want to use (team standings, player bios, season stats), follow the same pattern:

1. Create a new function file: `ingestion/nba/{endpoint}.py`
2. Upload to a new S3 prefix: `bronze/nba/{endpoint}/`
3. Create a new staging model: `transform/nba/models/staging/stg_nba__{entity}.sql` that reads from the new S3 prefix
4. Add a new task to the Airflow DAG in `airflow/dags/nba_pipeline_dag.py`

The naming convention for staging models is `stg_{source}__{entity}.sql` — the double underscore separates the source name from the entity name.

---

## Adding a New dbt Test

**Built-in tests** (add to `_staging.yml` or `_marts.yml`):
```yaml
- name: column_name
  tests:
    - not_null
    - unique
    - accepted_values:
        arguments:
          values: ['value1', 'value2']
```

**Custom SQL test** (create in `transform/nba/tests/`):
```sql
-- tests/assert_scores_non_negative.sql
-- Test passes when this query returns 0 rows
select game_id
from {{ ref('game_summary') }}
where final_score_home < 0 or final_score_away < 0
```

**Adding the `dbt-utils` package** for more test types (range checks, expression tests, etc.):

1. Create `transform/nba/packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

2. Install it:
```bash
dbt deps --project-dir transform/nba --profiles-dir transform/nba
```

3. Use in tests:
```yaml
- name: points_scored
  tests:
    - dbt_utils.accepted_range:
        min_value: 0
```
