# dbt Models

## What is dbt?

dbt (data build tool) lets you write SQL `SELECT` statements and handles everything else: creating tables/views, running models in the right dependency order, and testing the results.

**Key concepts:**

| Concept | What it is |
|---------|-----------|
| **Model** | A `.sql` file containing a single `SELECT` statement. dbt wraps it in `CREATE TABLE AS` or `CREATE VIEW AS` |
| **`{{ ref('name') }}`** | How you reference another model. dbt resolves the order automatically |
| **Materialization** | How the result is stored: `view` (runs on every query) or `table` (stored once, fast to read) |
| **Test** | A data quality check defined in `.yml` files. Runs with `dbt test` |
| **Macro** | Reusable SQL logic, like a function you can call from any model |
| **Source** | A declaration of raw data that dbt doesn't manage (e.g., your S3 files) |

---

## Project Configuration — `dbt_project.yml`

```yaml
name: nba
version: '1.0.0'
profile: nba_duckdb          # matches the name in profiles.yml

model-paths: ["models"]
macro-paths: ["macros"]

models:
  nba:
    staging:
      +materialized: view    # all models in models/staging/ → views
    marts:
      +materialized: table   # all models in models/marts/ → tables
```

The `+materialized` setting is inherited by all models in that folder. You can override it per-model by adding `{{ config(materialized='table') }}` at the top of a `.sql` file.

---

## Connection — `profiles.yml`

`profiles.yml` is the file that tells dbt how to connect to your database. It is **gitignored** because it contains your AWS credentials.

```yaml
nba_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/transform/nba/nba.duckdb   # where the DuckDB file lives
      extensions:
        - httpfs                                  # enables reading from S3
      settings:
        s3_region: "{{ env_var('AWS_REGION', 'eu-west-1') }}"
        s3_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

`env_var('VAR_NAME')` reads from your shell environment. This is why you must run `export $(cat .env | xargs)` before running dbt.

---

## The `parse_clock` Macro

**File:** `transform/nba/macros/parse_clock.sql`

The NBA API stores game clock values in ISO 8601 duration format: `"PT10M23.00S"` means "10 minutes and 23 seconds remaining in the period".

This macro converts that to **seconds elapsed** in the period (useful for analysis like "how many points were scored in the last 2 minutes?").

```sql
{% macro parse_clock(clock_col, period_duration_seconds=720) %}
    case
        when {{ clock_col }} is null then null
        else (
            {{ period_duration_seconds }}
            - (
                cast(regexp_extract({{ clock_col }}, 'PT(\d+)M', 1) as integer) * 60
                + cast(regexp_extract({{ clock_col }}, 'M(\d+(?:\.\d+)?)S', 1) as float)
            )
        )
    end
{% endmacro %}
```

**How it works:**
- `regexp_extract('PT10M23.00S', 'PT(\d+)M', 1)` → `"10"` (minutes remaining)
- `regexp_extract('PT10M23.00S', 'M(\d+(?:\.\d+)?)S', 1)` → `"23.00"` (seconds remaining)
- For a 12-minute quarter (720 seconds): `720 - (10 × 60 + 23) = 720 - 623 = 97 seconds elapsed`

**Usage in a model:**
```sql
{{ parse_clock("action->>'clock'") }}     -- uses default 720s (12-minute quarter)
{{ parse_clock("action->>'clock'", 300) }} -- for a 5-minute overtime period
```

---

## Silver Model — `stg_nba__play_by_play`

**File:** `transform/nba/models/staging/stg_nba__play_by_play.sql`

**Materialization:** view

**What it does:** Reads all bronze JSON files from S3 and returns one row per game action.

**Full SQL walkthrough:**

```sql
with raw as (
    -- read_json_auto reads ALL files matching the glob in one query
    -- Each file = one row, with a 'game' column containing the JSON object
    select
        game->>'gameId' as game_id,
        -- unnest explodes the actions array into individual rows
        -- cast to json[] required because DuckDB's unnest needs a typed list
        unnest(cast(game->'actions' as json[])) as action
    from read_json_auto(
        's3://bucket/bronze/nba_pbp/*.json',
        format = 'auto'
    )
),

cleaned as (
    select
        game_id,

        -- Cast each JSON field to its proper SQL type
        -- ->> extracts as text, :: casts to a type
        (action->>'actionNumber')::integer     as action_number,
        action->>'clock'                       as clock_raw,
        {{ parse_clock("action->>'clock'") }}  as clock_seconds_elapsed,
        (action->>'period')::integer           as period,

        action->>'teamId'                      as team_id,
        action->>'teamTricode'                 as team_tricode,
        action->>'personId'                    as player_id,
        action->>'playerName'                  as player_name,

        action->>'actionType'                  as action_type,
        action->>'subType'                     as sub_type,
        action->>'description'                 as description,

        -- try_cast returns NULL instead of an error if the value can't be cast
        try_cast(action->>'isFieldGoal' as boolean)   as is_field_goal,
        action->>'shotResult'                         as shot_result,
        try_cast(action->>'shotDistance' as float)    as shot_distance,
        try_cast(action->>'pointsTotal' as integer)   as points_total,

        -- Scores are null for non-scoring actions (timeouts, fouls, etc.)
        try_cast(action->>'scoreHome' as integer)     as score_home,
        try_cast(action->>'scoreAway' as integer)     as score_away,

        action->>'location'                           as location,
        try_cast(action->>'videoAvailable' as boolean) as video_available,
        try_cast(action->>'actionId' as integer)       as action_id
    from raw
    where action is not null
)

select * from cleaned
```

**Output columns:**

| Column | Type | Notes |
|--------|------|-------|
| `game_id` | varchar | e.g. `"0022500938"` |
| `action_number` | integer | Sequential within the game |
| `clock_raw` | varchar | e.g. `"PT10M23.00S"` |
| `clock_seconds_elapsed` | float | Seconds elapsed in the current period |
| `period` | integer | 1–4 regulation, 5+ overtime |
| `team_id` | varchar | NBA team ID |
| `team_tricode` | varchar | e.g. `"MIA"`, `"LAL"` |
| `player_id` | varchar | NBA player ID |
| `player_name` | varchar | Player last name |
| `action_type` | varchar | `"Made Shot"`, `"Missed Shot"`, `"Free Throw"`, `"Rebound"`, `"Turnover"`, `"Foul"`, etc. |
| `sub_type` | varchar | Shot type e.g. `"Jump Shot"`, `"Driving Layup Shot"` |
| `description` | varchar | Full human-readable description |
| `is_field_goal` | boolean | `true` for `Made Shot` and `Missed Shot` only |
| `shot_result` | varchar | `"Made"` or `"Missed"` for field goals. **Empty for free throws** |
| `shot_distance` | float | Distance in feet |
| `points_total` | integer | Cumulative player game points at this moment |
| `score_home` | integer | Running home score. `null` for non-scoring actions |
| `score_away` | integer | Running away score. `null` for non-scoring actions |

---

## Important quirks of the NBA API data

These are non-obvious things discovered by querying the actual data:

**1. `shot_result` is empty for free throws.**
Field goals use `shotResult = "Made"` or `"Missed"`. Free throws have an empty string. Instead, look at the `description` field:
- Made free throw: `"Adebayo Free Throw 1 of 2 (11 PTS)"` — has `(N PTS)` at the end
- Missed free throw: `"MISS Adebayo Free Throw 2 of 2"` — starts with `MISS`

**2. 3-pointers are identified via the description, not a dedicated field.**
A 3-point shot has `"3PT"` in the description: `"Reaves 28' 3PT Jump Shot (21 PTS)"`. There is no boolean `is3pt` field.

**3. Assists, blocks, and steals are not separate action types.**
They are embedded in other actions' descriptions:
- Assist: appears at the end of a made shot description — `"(Vanderbilt 1 AST)"`
- Block: appears in the description of a missed shot — `"Adebayo 3' Layup (Block by Turner)"`
- Steal: appears in a turnover description

Counting these per player requires regex extraction from `description`, which is not yet implemented.

**4. `pointsTotal` is cumulative, not per-action.**
It represents the player's total game points at the moment of that action, not the points scored by that specific action.

---

## Gold Model — `game_summary`

**File:** `transform/nba/models/marts/game_summary.sql`

**Grain:** one row per game.

**Key logic:** The final score comes from the last action in the game that has a non-null score. This is found using `ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY period DESC, action_number DESC)` and taking `rn = 1`.

**Output columns:** `game_id`, `final_score_home`, `final_score_away`, `winner_location` (home/away/tie), `point_margin`, `periods_played`, `went_to_overtime`, `total_actions`, `total_fg_attempts`, `total_ft_attempts`

---

## Gold Model — `player_game_stats`

**File:** `transform/nba/models/marts/player_game_stats.sql`

**Grain:** one row per player per game.

**Key stat logic:**

```sql
-- Field goals: action_type distinguishes made vs missed (not shot_result)
field_goals_made:     count(*) filter (where action_type = 'Made Shot')
field_goals_attempted: count(*) filter (where action_type in ('Made Shot', 'Missed Shot'))

-- 3-pointers: detected via '3PT' substring in the description
three_pointers_made:  count(*) filter (where action_type = 'Made Shot' and description like '%3PT%')

-- Free throws: missed ones start with 'MISS' in the description
free_throws_made:     count(*) filter (where action_type = 'Free Throw' and description not like 'MISS%')

-- Points formula
points_scored = (field_goals_made - three_pointers_made) * 2
              + three_pointers_made * 3
              + free_throws_made
```

**Note on rebounds:** The `Rebound` action type in the raw data includes both individual player rebounds and team rebounds (unattributed rebounds). All rebounds count here since they all have a `player_name` associated.

---

## Gold Model — `team_game_stats`

**File:** `transform/nba/models/marts/team_game_stats.sql`

**Grain:** one row per team per game.

Aggregates `player_game_stats` by `(game_id, team_id, team_tricode)` and joins with `game_summary` to add game context (OT indicator, periods played).

---

## Tests — `_staging.yml` and `_marts.yml`

Tests are declared in YAML files alongside the models. Run with `dbt test`.

**Built-in test types:**
- `not_null` — column must have no null values
- `unique` — column must have no duplicate values
- `accepted_values` — column must only contain the listed values

**Current tests (14 total, all passing):**

| Model | Column | Test |
|-------|--------|------|
| `stg_nba__play_by_play` | `game_id` | not_null |
| `stg_nba__play_by_play` | `action_number` | not_null |
| `stg_nba__play_by_play` | `period` | not_null, accepted_values [1–7] |
| `stg_nba__play_by_play` | `action_type` | not_null |
| `game_summary` | `game_id` | not_null, unique |
| `game_summary` | `final_score_home` | not_null |
| `game_summary` | `final_score_away` | not_null |
| `game_summary` | `winner_location` | accepted_values [home, away, tie] |
| `player_game_stats` | `game_id` | not_null |
| `player_game_stats` | `player_id` | not_null |
| `team_game_stats` | `game_id` | not_null |
| `team_game_stats` | `team_id` | not_null |

**Adding a new test:**
```yaml
# in _marts.yml, under the column you want to test
- name: points_scored
  tests:
    - not_null
```

**Adding a custom SQL test** — create a `.sql` file in `transform/nba/tests/`. The test passes when the query returns 0 rows:
```sql
-- tests/assert_scores_non_negative.sql
select game_id from {{ ref('game_summary') }}
where final_score_home < 0 or final_score_away < 0
```
