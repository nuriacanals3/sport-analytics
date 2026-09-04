"""Reads the fatigue_features mart and does the last, small bit of feature
engineering the regression needs. No dbt here -- fatigue_features is already
a materialized table, so this is a plain read, no S3/B2 credentials needed.
"""
import duckdb

DUCKDB_PATH = 'transform/nba/nba.duckdb'

# The controls (isolate team quality and home-court from the fatigue effect)
# and the fatigue/travel features whose coefficients become Phase 5's weights.
CONTROL_COLUMNS = ['is_home', 'self_net_rating', 'opponent_net_rating']
FATIGUE_COLUMNS = [
    'rest_days_diff',
    'travel_miles_diff',
    'timezones_shift_diff',
    'back_to_back_diff',
]
TARGET_COLUMN = 'target_plus_minus'


def load_features(duckdb_path=DUCKDB_PATH):
    """Reads fatigue_features and adds back_to_back_diff (self minus opponent,
    as -1/0/1) -- the one feature not already in the mart as a plain numeric
    column, since it's a difference of two booleans.
    """
    con = duckdb.connect(duckdb_path, read_only=True)
    df = con.execute("""
        select
            *,
            self_is_back_to_back::int - opponent_is_back_to_back::int as back_to_back_diff
        from fatigue_features
    """).fetchdf()
    con.close()

    # A team's first game of a season has no previous game, so its fatigue
    # features are genuinely null -- and DuckDB returns those nullable
    # columns as pandas' Int64/boolean extension dtypes (pd.NA), which
    # patsy's NaN check chokes on. Forcing plain float64 turns pd.NA into a
    # normal np.nan, which statsmodels' formula API already knows to drop.
    numeric_columns = [TARGET_COLUMN] + CONTROL_COLUMNS + FATIGUE_COLUMNS
    numeric_columns.remove('is_home')  # cast below, after the bool->int step
    df['is_home'] = df['is_home'].astype(int)
    for col in numeric_columns:
        df[col] = df[col].astype('float64')

    return df
