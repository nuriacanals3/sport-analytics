# Travel Logistics — Implementation Plan

> Feature roadmap for Claude Code. This document is the map of **what to build and in what order**.
> It does **not** replace `CLAUDE.md` (which stays as the repo's persistent conventions guide).
> Work through it **phase by phase** — each phase ends in something executable and verifiable.
> Do not build later phases before an earlier phase's verification passes.

> **Status (as of 2026-09-06): all six phases done and verified.** See CLAUDE.md's
> "Architecture — travel logistics" section for the current file map. Phase 5 has one
> disclosed, accepted gap: 3 of 11 lambda points (0.7-0.9) found no real improvement over
> the real schedule -- see optimization/run_phase_b.py's module docstring. Phase 6 (Streamlit
> app) verified running locally; Streamlit Community Cloud deployment is the next action, not
> yet done.

---

## 1. Context & goal

The repo already has a working ingestion + transformation pipeline for NBA data
(Bronze → Silver → Gold, `nba_api` → S3 → dbt-duckdb, orchestrated by Airflow;
existing marts: `game_summary`, `player_game_stats`, `team_game_stats`).

We are adding a new analytical + optimisation feature on top of it:

> Model NBA team travel logistics across a season — quantify flight distances, turnaround
> times and fatigue load — then use scheduling optimisation to propose alternative schedules
> that reduce travel, improve player recovery and lower carbon footprint.

This decomposes into **three conceptually distinct layers**, built in six phases:

1. **Data + travel modelling** — schedule + arena geography → travel legs (easy, pure dbt).
2. **Fatigue metrics + a predictive cost model** — engineered fatigue features + a model whose
   coefficients become the *weights* of the fatigue index (medium).
3. **Schedule optimisation** — local search over the whole league, dual objective fatigue + carbon (hard).

---

## 2. Guiding principles (read before writing any code)

These are the decisions that are easy to get wrong. They are non-negotiable design choices, not suggestions.

- **The API has no "fatigue" endpoint.** Fatigue features are *engineered* from the schedule +
  arena coordinates. The API only supplies (a) games with results — the target — and (b) team
  strength — controls. Everything else we compute.

- **Optimise the whole league, never a single team.** Reordering one team's trips with the rest
  of the league frozen is a fiction (every moved game is a rival's home game that also moves).
  Instead: start from the **real, feasible schedule** and improve it with **local search** using
  moves that preserve feasibility. Every move is evaluated at league level. This makes the
  problem tractable and means the output is **never worse than reality** and directly comparable to it.
  "One team" survives only as a **diagnostic view** (report miles/fatigue per team before/after),
  not as the optimisation unit.

- **The model *calibrates the weights*; the objective *sums the burden*.** The predictive model
  predicts point margin. Its coefficients on fatigue features give the marginal margin-impact per
  unit of each feature — those magnitudes are the weights that let us combine heterogeneous
  components (a mile vs a back-to-back) on one scale. But **do not sum predicted margins in the
  objective**: margin is zero-sum between the two teams of a game (differential feature), so it
  cancels league-wide and means nothing. The optimisation objective is **total fatigue burden** —
  a per-team, always-positive, always-reducible quantity — summed over all team-games.

- **Carbon does not need the model.** It is pure geometry: `miles × charter_emission_factor`.
  The two halves of the objective therefore have very different confidence: carbon is a hard
  physical estimate, fatigue is a soft statistical one. Present them with that asymmetry in mind.

- **Two objectives, no fake exchange rate.** There is no natural conversion between kg CO₂ and
  margin points. **Do not collapse to a single number.** Sweep a weight `λ` and produce a
  **Pareto frontier** (carbon ↔ fatigue), marking where the real NBA schedule falls on it.

- **Optimise for total fatigue (min-sum), but always report the per-team distribution.** A schedule
  can lower league-total fatigue while dumping it on three unlucky teams. Min-sum is the primary
  objective; the cross-team distribution is a mandatory secondary metric (schedule fairness).

- **Transport alternatives are a *posterior* layer, not inside the objective.** The optimiser
  minimises miles + fatigue assuming charter (the status quo). A separate scenario module then
  takes an already-optimised schedule and shows the additional CO₂ delta of commercial / SAF.

- **The Streamlit app never runs the optimiser live.** Local search is thousands of iterations
  (minutes). Precompute the whole Pareto grid offline, save as artifacts (parquet / DuckDB); the
  app only **reads and paints**.

- **Distances are great-circle (haversine)**, not real flight routes. Standard and sufficient — just declared.

- **dbt vs Python boundary.** dbt does SQL transformation: travel legs, metrics, and the
  feature/target table. Model training, scoring, and optimisation are Python and live outside the
  dbt project. The trained model is saved as a **reusable artifact** (pickle / parametric cost
  function) callable live by the optimiser on hypothetical schedules.

---

## 3. Data

### 3.1 Endpoints (`nba_api`)

| Source | Endpoint | Role |
| --- | --- | --- |
| Backbone (games + target) | `LeagueGameLog` (`player_or_team_abbreviation='T'`) | One call per season → every team-game: `GAME_DATE`, `MATCHUP` (→ home/away + opponent), `WL`, `PLUS_MINUS` (**the target**), `TEAM_ID`, `PTS`, box basics. |
| Team strength (controls) | `LeagueDashTeamStats` (measure types Base + Advanced) | Net rating, off/def rating, pace per team-season. Prevents confounding fatigue with team quality. |
| Standings (optional control) | `LeagueStandingsV3` | Alternative/complement to strength. |
| Player depth (**Phase 3 improvement only**) | `PlayerGameLogs`, `CommonTeamRoster` / `CommonPlayerInfo` (`BIRTHDATE`) | Minutes load and roster age. Not in the base model. |
| Prospective schedule (optional) | `ScheduleLeagueV2` | Only if forward/unplayed schedule is needed; for a completed season `LeagueGameLog` is cleaner. |

> **Do not use `TeamGameLog` / `TeamGameLogs`** — deprecated by the NBA with no replacement. `LeagueGameLog` is the correct source.

### 3.2 Seed

`transform/nba/seeds/nba_arenas.csv` — static, ~30 rows:
`team_abbreviation, arena, city, lat, lon, timezone`. Neutral-site games (Mexico City, Paris,
NBA Cup in Las Vegas) handled as explicit exceptions.

### 3.3 Data window & exclusions

- **Train the cost model on multiple seasons (3–5)** for signal; **run the optimisation on one
  season** as the case study.
- **Exclude COVID seasons** (2019–20 bubble, 2020–21 compressed) — anomalous.
- **Regular season only** — playoffs are series with a different (2-2-1-1-1) travel pattern.

---

## 4. Architecture

New top-level components alongside the existing `ingestion/`, `transform/`, `airflow/`, `docs/`:

```
ingestion/nba/
  league_game_log.py        # NEW  Bronze: LeagueGameLog, multi-season → S3
  team_season_stats.py      # NEW  Bronze: LeagueDashTeamStats → S3

transform/nba/
  seeds/nba_arenas.csv      # NEW  arena coordinates + timezone
  models/staging/
    stg_game_log.sql        # NEW  Silver
    stg_team_season_stats.sql
  models/marts/travel/
    team_travel_legs.sql    # NEW  Gold: one row per team per travel leg
    team_travel_season_summary.sql
    fatigue_features.sql     # NEW  Gold: team-game grain, differential features + target

modelling/                  # NEW  Python (ML) — not dbt
  features.py               # read fatigue_features mart
  train.py                  # temporal split, baseline, fit, evaluate
  cost_model.py             # reusable cost function / artifact loader
  artifacts/fatigue_cost_model.pkl

optimization/               # NEW  Python — not dbt
  schedule.py               # league schedule data structure (from marts)
  moves.py                  # feasibility-preserving moves
  objectives.py             # geometric miles; fatigue burden (uses cost_model); carbon
  search.py                 # local-search engine (SA/tabu), incremental delta eval
  run_phase_a.py            # geometric objective
  run_phase_b.py            # sweep λ → Pareto grid → WRITE artifacts
  artifacts/pareto_results/ # parquet per λ (schedule + metrics + per-team distribution)

carbon/                     # NEW  posterior scenario layer
  scenarios.py              # charter / commercial / SAF factors on optimised schedules

app/                        # NEW  Streamlit
  streamlit_app.py          # reads precomputed artifacts + DuckDB only

notebooks/                  # NEW  construction & validation surface (phases 3–5)
  03_cost_model_validation.ipynb
  04_phase_a_sanity.ipynb
  05_pareto_exploration.ipynb
```

**Notes**
- This analysis is inherently NBA-specific and cross-cuts sports; kept flat/NBA-scoped rather than
  nested by sport. If generalised later, nest under `{component}/nba/`.
- **Airflow:** phases 1–2 (ingestion + dbt models) slot into the existing daily DAG. Phases 3–6
  (ML training, optimisation, Pareto precompute, app) are **offline, run-once-per-analysis** —
  keep them out of the daily DAG (a separate one-shot DAG or a `Makefile` is fine).
- **New dependencies** (add to `requirements.txt`): a modelling lib (scikit-learn, or
  xgboost/lightgbm), `pandas`, `numpy`, `streamlit`, `pydeck`. Local search is custom, so
  OR-Tools is **not** required; `networkx` optional for graph utilities.

---

## 5. Phases

### Phase 1 — Ingestion + seed
**Build:** `league_game_log.py` (multi-season) and `team_season_stats.py` writing raw JSON to Bronze/S3, following the existing `play_by_play.py` pattern; add `nba_arenas.csv` seed.
**Verify:** raw objects land in S3; `dbt seed` loads the arenas table; row counts are sane (~1,230 games/season × 2 team-rows).

### Phase 2 — Travel + metrics models (dbt)
**Build:**
- `stg_game_log`, `stg_team_season_stats` (Silver).
- `team_travel_legs` — one row per team per leg with: haversine distance from previous game city, rest days, back-to-back flag, timezones crossed **and direction** (eastward flagged), road-trip length, home/away.
- `team_travel_season_summary` — per-team season totals.
- `fatigue_features` — **team-game grain**, one row per team per game. Self-join `LeagueGameLog` on `GAME_ID` to attach the opponent's row, then build **differential** features (`rest_self − rest_opp`, `travel_self − travel_opp`, …), plus own/opponent net rating, home/away, and the target `PLUS_MINUS`.
**Verify:** query the marts; totals match expectation (e.g. known heavy-travel teams rank high); back-to-back counts are plausible; `dbt test` passes.

### Phase 3 — Fatigue cost model (Python)
**Build:** read `fatigue_features`; **temporal split** (train on older seasons, validate on the most recent — never mix future into past); a **baseline to beat** (e.g. home-court advantage only); start simple and interpretable (logistic/linear or gradient boosting) — the goal is *estimating
effects*, not winning a prediction contest. Extract fatigue-feature coefficients as the **weights**.
Save the model as a reusable artifact.
**Improvement (optional, deferred):** add the player-depth block (minutes load, roster age).
**Verify:** the fatigue model **beats the baseline** on the held-out season. If it does not, the cost feeding the optimiser is worthless — stop and reconsider before proceeding.

### Phase 4 — Local-search engine + Phase A (geometric)
**Build:**
- `schedule.py` — league schedule structure from the marts.
- `moves.py` — feasibility-preserving moves:
  - **date swap** (swap the dates of two of a team's games),
  - **home-and-home leg swap** (for a pair playing twice, swap which date is at which arena),
  - **road-trip reorder** (permute consecutive away games within fixed dates).
- Hard constraints enforced by the move set: **one game per team per day**, **one game per arena per day**, **cap on consecutive away games** (parameter `K`, e.g. 6). The fixture multiset (who plays whom, home/away balance, 82 games/team, fixed date window) is preserved — moves only change ordering/dates.
- `search.py` — start from the real (feasible) schedule; propose random feasible moves; accept by simulated-annealing rule; track best. **Incremental (delta) evaluation**: a move touches only a few teams' legs — recompute only those.
- `run_phase_a.py` — objective = **total league miles** (pure geometry, no model). This validates the whole engine cheaply.
**Verify:** output schedule is feasible; total miles ≤ real schedule; the best-tracked solution never exceeds the real baseline.

### Phase 5 — Phase B (dual objective + Pareto)
**Build:** `objectives.py` fatigue burden `F(S) = Σ_{team-game} Σ_k β_k · feature_k` (β from the
Phase 3 model, taken as positive burden) and carbon `C(S) = Σ_legs miles · charter_factor`.
Combined objective for weight `λ ∈ [0,1]`: `minimise (1−λ)·F_norm + λ·C_norm` (normalise each to
comparable scale). `run_phase_b.py` sweeps a `λ` grid → a set of optimised schedules.
**Must write artifacts:** for each `λ`, persist the optimised schedule and its (total miles, fatigue burden, carbon, **per-team distribution**), plus the real schedule's values
as the baseline point. Save as parquet / DuckDB for the app.
**Verify:** Pareto frontier is monotone/sensible; the real NBA point sits above/behind the frontier; artifacts are on disk and self-describing.

### Phase 6 — Transport scenarios + Streamlit output
**Build:**
- `carbon/scenarios.py` — apply charter / commercial / SAF emission factors to an optimised schedule (posterior layer, not in the optimiser). Note team travelling party ≈ 40–50 people; charter emissions are **per-aircraft**, commercial **per-passenger** (much lower CO₂ but worse recovery — the interesting tension).
- `app/streamlit_app.py` — reads precomputed artifacts + DuckDB **only**. Views:
  1. miles + CO₂ saved, real vs optimised;
  2. per-team route map before/after (pydeck arc layers);
  3. Pareto frontier with a `λ` slider, real-NBA point marked;
  4. fatigue distribution across teams (the equity angle);
  5. transport-scenario toggle on top.
**Verify:** app loads instantly (no optimisation at runtime); every widget navigates precomputed results; deploys on Streamlit Community Cloud from the repo (ship the DuckDB / parquets, they are small).

---

## 6. Roles of the two surfaces

- **Notebooks (phases 3–5):** the construction & validation surface — checking the model beats the
  baseline, eyeballing routes, exploring the frontier. Reproducible record of *how it was built and
  validated*; does not need to be pretty.
- **Streamlit app:** the polished entry point for a viewer — "here is the result, play with it."

Keep them separate: the notebook honest, the app polished.

---

## 7. Parameters to fix during implementation

- Number and choice of training seasons; which single season is the optimisation case study.
- Consecutive-away cap `K`.
- Charter (and commercial / SAF) emission factors, and travelling-party size.
- `λ` grid resolution; simulated-annealing schedule (temperature, cooling, iterations).
- Normalisation scheme for combining `F` and `C`.
