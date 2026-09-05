# Architecture

## Overview

The pipeline follows the **Medallion architecture**, a common pattern in data engineering where data moves through three progressive layers of refinement:

```
Raw API data → Bronze (store as-is) → Silver (clean) → Gold (aggregate)
```

Each layer has a clear responsibility. You never modify a layer's data from a downstream layer — data always flows forward.

---

## Bronze Layer

**What it does:** Fetches raw data from the NBA Stats API and stores it in S3 unchanged.

**Code:** `ingestion/nba/play_by_play.py`

**How it works step by step:**

1. `ScoreboardV2(game_date=yesterday)` — asks the NBA API for all games played yesterday, returns a list of `GAME_ID` values (e.g. `"0022500938"`)
2. For each game, `PlayByPlayV3(game_id=game_id).get_dict()` — fetches the full play-by-play for that game as a Python dictionary
3. `upload_to_s3(data, filename)` — serializes the dictionary to JSON and uploads it to S3

**S3 file path pattern:**
```
s3://{S3_BUCKET_NAME}/bronze/nba_pbp/pbp_{game_id}_{date}.json
```

**Why store raw JSON?** The bronze layer is the source of truth. If a transformation has a bug, you can re-run dbt without re-fetching from the API. The API has rate limits and occasionally goes down, so having the raw data stored protects you.

**Raw JSON structure:**
```json
{
  "game": {
    "gameId": "0022500938",
    "videoAvailable": 0,
    "actions": [
      {
        "actionNumber": 1,
        "clock": "PT12M00.00S",
        "period": 1,
        "teamId": "1610612748",
        "teamTricode": "MIA",
        "personId": "1628389",
        "playerName": "Adebayo",
        "actionType": "Made Shot",
        "subType": "Driving Layup Shot",
        "isFieldGoal": 1,
        "shotResult": "Made",
        "shotDistance": 3.0,
        "scoreHome": "2",
        "scoreAway": "0",
        "pointsTotal": 2,
        "description": "Adebayo 3' Driving Layup (2 PTS)",
        ...
      },
      ...
    ]
  }
}
```

**Key field explanations:**

| Field | Description |
|-------|-------------|
| `actionNumber` | Sequential number of this action within the game |
| `clock` | Time remaining in the period. Format: ISO 8601 duration, e.g. `"PT10M23.00S"` = 10 min 23 sec remaining |
| `period` | Quarter/period: 1–4 = regulation, 5+ = overtime |
| `actionType` | Type of play. Common values: `"Made Shot"`, `"Missed Shot"`, `"Free Throw"`, `"Rebound"`, `"Turnover"`, `"Foul"`, `"Substitution"`, `"Timeout"` |
| `isFieldGoal` | `1` if this is a field goal attempt (only `Made Shot` and `Missed Shot` actions) |
| `shotResult` | `"Made"` or `"Missed"` for field goals. **Empty string for free throws** — use the `description` field instead |
| `scoreHome` / `scoreAway` | Running score at this point. `null` for non-scoring actions |
| `pointsTotal` | Cumulative game points for this player at this moment in the game |
| `description` | Human-readable text: `"Adebayo 28' 3PT Jump Shot (14 PTS) (Green 3 AST)"`. Used to detect 3-pointers (`3PT` substring) and missed free throws (`MISS` prefix) |

---

## Silver Layer

**What it does:** Reads all raw JSON files from S3 and flattens them into a clean, typed relational table.

**Code:** `transform/nba/models/staging/stg_nba__play_by_play.sql`

**How it works:**

DuckDB connects to S3 via its built-in `httpfs` extension, which lets it read files from S3 as if they were local files. The staging model runs a single SQL query that:

1. Reads all JSON files matching the glob `s3://bucket/bronze/nba_pbp/*.json` using `read_json_auto()`
2. Each file returns one row with a `game` column containing the entire JSON object
3. `unnest(cast(game->'actions' as json[]))` explodes the `actions` array, creating one row per action per game
4. The `cleaned` CTE casts every field to its proper SQL type and renames fields to snake_case

```sql
with raw as (
    select
        game->>'gameId' as game_id,
        unnest(cast(game->'actions' as json[])) as action
    from read_json_auto('s3://bucket/bronze/nba_pbp/*.json', format = 'auto')
),
cleaned as (
    select
        game_id,
        (action->>'actionNumber')::integer as action_number,
        action->>'clock'                   as clock_raw,
        {{ parse_clock("action->>'clock'") }} as clock_seconds_elapsed,
        ...
    from raw
)
select * from cleaned
```

**Why a view?** The staging model is materialized as a `view`, not a table. This means every time you query it, DuckDB re-reads from S3. This ensures the silver layer always reflects the latest bronze data without any manual refresh step. The cost is that queries against this view are slower (S3 reads take time).

**The `cast(... as json[])` requirement:** DuckDB's `unnest()` function requires an explicit list type. The `->` JSON operator returns a raw `JSON` type, which DuckDB can't iterate. Casting to `json[]` tells DuckDB to treat it as a list of JSON objects.

---

## Gold Layer

**What it does:** Produces aggregated, analysis-ready tables from the silver view.

**Code:** `transform/nba/models/marts/`

**Why tables?** The gold models are materialized as `table` because queries against S3 via the silver view are slow (30–40 seconds). Once built, the gold tables live in the local DuckDB file and query in milliseconds.

**Model dependencies:**
```
stg_nba__play_by_play (view)
    ├── game_summary (table)
    ├── player_game_stats (table)
    └── team_game_stats (table)
            depends on player_game_stats + game_summary
```

dbt resolves these dependencies automatically using `{{ ref('model_name') }}`. You never write explicit `JOIN` paths to tables — dbt figures out the execution order.

---

## Airflow Orchestration

**Code:** `airflow/dags/nba_pipeline_dag.py`

**DAG:** `nba_pipeline_daily` — runs daily at 08:00 UTC.

**Task chain:**
```
extract_nba_api_to_s3  (PythonOperator)
        ↓
dbt_run_silver         (BashOperator: dbt run --select staging)
        ↓
dbt_run_gold           (BashOperator: dbt run --select marts)
        ↓
dbt_test               (BashOperator: dbt test)
```

**Why BashOperator for dbt?** The simplest approach for local learning. The BashOperator runs dbt as a CLI command. More advanced setups use [astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos) which generates one Airflow task per dbt model, giving finer visibility — worth exploring once you understand the basics.

**Credential passing:** The BashOperator inherits `os.environ` from the Airflow process. This means Airflow must be started from a shell that has the storage credentials exported (see CLAUDE.md's "Environment Variables" section for the current variable names):
```bash
export $(cat .env | xargs) && airflow standalone
```

---

## Data Flow Summary

```
NBA API
  │
  │  nba_api Python library
  ▼
ingestion/nba/play_by_play.py
  │
  │  boto3 PutObject
  ▼
S3: bronze/nba_pbp/pbp_{game_id}_{date}.json    ← raw JSON, one file per game
  │
  │  DuckDB httpfs + read_json_auto + unnest
  ▼
stg_nba__play_by_play                            ← view, one row per game action
  │
  │  SQL aggregations
  ├──────────────────────────────────────────────
  ▼                    ▼                    ▼
game_summary     player_game_stats    team_game_stats
(1 row/game)     (1 row/player/game)  (1 row/team/game)
  │                    │                    │
  └────────────────────┴────────────────────┘
              nba.duckdb file
              (query with Python or DuckDB CLI)
```
