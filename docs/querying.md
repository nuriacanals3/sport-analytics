# Querying the Data

The gold layer tables live in a local DuckDB database file at `transform/nba/nba.duckdb`. This file is gitignored — it is generated when you run `dbt run`.

---

## Connecting

### Python (recommended)

```python
import duckdb

con = duckdb.connect('transform/nba/nba.duckdb')

# Returns a pandas DataFrame
df = con.execute("SELECT * FROM game_summary").df()
print(df)
```

### Jupyter Notebook

```bash
pip install jupyter
jupyter notebook
```

```python
import duckdb

con = duckdb.connect('transform/nba/nba.duckdb')
df = con.execute("SELECT * FROM player_game_stats").df()
df.describe()
```

---

## Available Tables

### `game_summary`

One row per game. Contains final scores and game-level aggregates.

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | varchar | Unique game identifier, e.g. `"0022500938"` |
| `final_score_home` | integer | Home team final score |
| `final_score_away` | integer | Away team final score |
| `winner_location` | varchar | `"home"`, `"away"`, or `"tie"` |
| `point_margin` | integer | Absolute score difference |
| `periods_played` | integer | 4 = regulation, 5+ = overtime |
| `went_to_overtime` | boolean | True if the game had at least one OT period |
| `total_actions` | integer | Total play-by-play events in the game |
| `total_fg_attempts` | integer | Total field goal attempts (both teams) |
| `total_ft_attempts` | integer | Total free throw attempts (both teams) |

### `player_game_stats`

One row per player per game. Box score stats derived from play-by-play events.

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | varchar | Game identifier |
| `player_id` | varchar | NBA player ID |
| `player_name` | varchar | Player last name |
| `team_id` | varchar | NBA team ID |
| `team_tricode` | varchar | Team abbreviation, e.g. `"MIA"` |
| `field_goals_made` | integer | Made field goals (2PT + 3PT) |
| `field_goals_attempted` | integer | Attempted field goals |
| `three_pointers_made` | integer | Made 3-point field goals |
| `three_pointers_attempted` | integer | Attempted 3-pointers |
| `free_throws_made` | integer | Made free throws |
| `free_throws_attempted` | integer | Attempted free throws |
| `points_scored` | integer | Total points: (FG-3PT)×2 + 3PT×3 + FT_made |
| `fg_pct` | float | Field goal percentage (null if 0 attempts) |
| `ft_pct` | float | Free throw percentage (null if 0 attempts) |
| `rebounds` | integer | Total rebounds |
| `turnovers` | integer | Turnovers |
| `fouls` | integer | Personal fouls committed |

### `team_game_stats`

One row per team per game. Aggregated team performance.

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | varchar | Game identifier |
| `team_id` | varchar | NBA team ID |
| `team_tricode` | varchar | Team abbreviation |
| `team_points` | integer | Total team score |
| `team_fg_made` / `team_fg_attempted` | integer | Team FG counts |
| `team_fg_pct` | float | Team field goal percentage |
| `team_3pt_made` / `team_3pt_attempted` | integer | Team 3PT counts |
| `team_ft_made` / `team_ft_attempted` | integer | Team FT counts |
| `team_rebounds` | integer | Total team rebounds |
| `team_turnovers` | integer | Total team turnovers |
| `team_fouls` | integer | Total team fouls |
| `players_in_game` | integer | Distinct players with recorded actions |
| `periods_played` | integer | From `game_summary` |
| `went_to_overtime` | boolean | From `game_summary` |

---

## Example Queries

### Top scorers

```python
con.execute("""
    SELECT player_name, team_tricode, points_scored, fg_pct, rebounds
    FROM player_game_stats
    ORDER BY points_scored DESC
    LIMIT 10
""").df()
```

### Season averages per player

```python
con.execute("""
    SELECT
        player_name,
        team_tricode,
        count(distinct game_id)          as games_played,
        round(avg(points_scored), 1)     as ppg,
        round(avg(fg_pct), 3)            as avg_fg_pct,
        round(avg(rebounds), 1)          as rpg,
        round(avg(turnovers), 1)         as topg
    FROM player_game_stats
    WHERE points_scored > 0
    GROUP BY player_name, team_tricode
    HAVING count(distinct game_id) >= 3
    ORDER BY ppg DESC
""").df()
```

### Highest-scoring games

```python
con.execute("""
    SELECT game_id, final_score_home, final_score_away,
           final_score_home + final_score_away as total_points,
           went_to_overtime
    FROM game_summary
    ORDER BY total_points DESC
""").df()
```

### Games that went to overtime

```python
con.execute("""
    SELECT game_id, final_score_home, final_score_away, point_margin, periods_played
    FROM game_summary
    WHERE went_to_overtime = true
    ORDER BY periods_played DESC
""").df()
```

### Best FG% games (minimum 10 attempts)

```python
con.execute("""
    SELECT player_name, team_tricode, game_id,
           field_goals_made, field_goals_attempted, fg_pct, points_scored
    FROM player_game_stats
    WHERE field_goals_attempted >= 10
    ORDER BY fg_pct DESC
    LIMIT 10
""").df()
```

### Team performance comparison

```python
con.execute("""
    SELECT
        team_tricode,
        count(*) as games,
        round(avg(team_points), 1)  as avg_points,
        round(avg(team_fg_pct), 3)  as avg_fg_pct,
        round(avg(team_rebounds), 1) as avg_rebounds
    FROM team_game_stats
    GROUP BY team_tricode
    ORDER BY avg_points DESC
""").df()
```

### Players with most free throw attempts

```python
con.execute("""
    SELECT player_name, team_tricode, game_id,
           free_throws_made, free_throws_attempted, ft_pct
    FROM player_game_stats
    ORDER BY free_throws_attempted DESC
    LIMIT 10
""").df()
```

### Close games (margin ≤ 5 points)

```python
con.execute("""
    SELECT game_id, final_score_home, final_score_away,
           point_margin, went_to_overtime
    FROM game_summary
    WHERE point_margin <= 5
    ORDER BY point_margin
""").df()
```

### Per-game stats for a specific team

```python
team = 'LAL'

con.execute(f"""
    SELECT p.player_name, p.game_id, p.points_scored, p.rebounds, p.turnovers,
           g.final_score_home, g.final_score_away, g.winner_location
    FROM player_game_stats p
    JOIN game_summary g ON p.game_id = g.game_id
    WHERE p.team_tricode = '{team}'
    ORDER BY p.game_id, p.points_scored DESC
""").df()
```

---

## Querying the Silver Layer (Advanced)

The silver layer is a **view** that reads from S3 live. Queries against it are slower (30–40 seconds) because DuckDB reads the JSON files each time, but you get the most up-to-date data.

You need AWS credentials loaded in your Python session:

```python
import duckdb, os
from dotenv import load_dotenv

load_dotenv()  # reads from .env file

con = duckdb.connect('transform/nba/nba.duckdb')
con.execute(f"SET s3_region='{os.getenv('AWS_REGION', 'eu-west-1')}'")
con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}'")
con.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}'")

# Now you can query the view
df = con.execute("""
    SELECT DISTINCT action_type, count(*) as cnt
    FROM stg_nba__play_by_play
    GROUP BY action_type
    ORDER BY cnt DESC
""").df()
```
