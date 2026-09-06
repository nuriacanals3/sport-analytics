"""Phase 6: the polished, precomputed entry point for viewing Phase 4/5's
schedule-optimisation results and Phase 6's transport-scenario comparison.

Reads ONLY the parquet artifacts under optimization/artifacts/pareto_results/
-- no DuckDB connection, no B2 credentials, no live optimisation. Every
number here was computed ahead of time by run_phase_a.py / run_phase_b.py /
export_arenas.py; this script's only job is to display them, plus apply
carbon/scenarios.py's cheap arithmetic (miles -> CO2 under a chosen transport
mode, or a unit conversion for display) live -- the only things genuinely
fast enough to run on every widget interaction without breaking the "no
optimisation at runtime" rule.

Two tabs, two independent lambda selections (deliberately NOT synced --
you might want lambda=0.4 on the league-wide frontier while poking at
lambda=1.0 for one team, and Streamlit's st.tabs keeps both tabs mounted so
switching between them never resets either one):
  - League-wide: headline totals, the Pareto frontier, transport scenarios.
  - Per-team: one team's own stats, its route map, its full-season Schedule
    Board (every game as a card, moved games flagged), and the per-team
    fatigue distribution.

Run from anywhere:
    streamlit run app/streamlit_app.py
This imports optimization.* and carbon.* as top-level packages, same as
every other script in this project -- but unlike `python -m ...`, Streamlit
does NOT reliably put the repo root on sys.path just because that's where
you launched it from (confirmed the hard way: this raised
ModuleNotFoundError for a real user even run from the repo root). So the
repo root is added explicitly below, computed from this file's own location
rather than assumed from cwd -- works regardless of where `streamlit run`
is invoked from.
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from carbon.scenarios import KM_PER_MILE, SCENARIOS, carbon_for_scenario
from optimization.schedule import NEUTRAL_SITE_VENUES

ARTIFACTS_DIR = os.path.join(_REPO_ROOT, 'optimization/artifacts/pareto_results')

st.set_page_config(page_title='NBA Travel Logistics', layout='wide')


@st.cache_data
def load_artifacts():
    summary = pd.read_parquet(f'{ARTIFACTS_DIR}/summary.parquet')
    per_team = pd.read_parquet(f'{ARTIFACTS_DIR}/per_team.parquet')
    schedules = pd.read_parquet(f'{ARTIFACTS_DIR}/schedules.parquet')
    arenas = pd.read_parquet(f'{ARTIFACTS_DIR}/arenas.parquet').set_index('key')
    return summary, per_team, schedules, arenas


def eu_number(value, decimals=0):
    """European-style formatting: '.' groups thousands, ',' is the decimal
    separator -- the reverse of Python's default (e.g. 1234567.8 with
    decimals=1 -> '1.234.567,8'). translate() swaps both in one pass, so
    there's no collision from replacing one separator into the other."""
    return f'{value:,.{decimals}f}'.translate(str.maketrans({',': '.', '.': ','}))


def fmt_distance(miles, unit):
    """One stored number (miles, matching carbon/scenarios.py's own unit),
    reformatted for display only -- never recomputed, same idea as the
    transport-scenario toggle turning one mile count into different CO2
    numbers. Handles negative deltas fine (the sign just carries through).
    """
    value = miles * KM_PER_MILE if unit == 'km' else miles
    return f'{eu_number(value)} {unit}'


# config(locale=...): Vega-Lite's own documented mechanism for swapping
# number formatting on any Altair chart -- covers axis tick labels AND
# tooltip values in one place. Shared across both tabs' charts.
EU_LOCALE = alt.Locale(number=alt.NumberLocale(
    decimal=',', thousands='.', grouping=[3], currency=['€', ''],
))


summary, per_team, schedules, arenas = load_artifacts()
LAMBDA_GRID = sorted(summary.loc[summary['lambda'].notna(), 'lambda'].unique())

st.title('NBA Travel Logistics')
st.caption(
    'Every number below comes from a precomputed optimisation run'
)

tab_league, tab_team = st.tabs(['League-wide', 'Per-team'])

# =====================================================================
# Tab 1 -- league-wide totals, the frontier, transport scenarios.
# =====================================================================
with tab_league:
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        lam1 = st.select_slider(
            'Lambda (0 = pure fatigue-reduction, 1 = pure carbon-reduction)',
            options=LAMBDA_GRID, value=0.4, key='lam_tab1',
        )
    with col2:
        scenario = st.selectbox(
            'Transport scenario',
            options=SCENARIOS, format_func=str.title, key='scenario_tab1',
        )
    with col3:
        unit1 = st.radio('Distance unit', ['mi', 'km'], horizontal=True, key='unit_tab1')

    source1 = f'lambda_{lam1}'
    real_row = summary[summary['source'] == 'real'].iloc[0]
    opt_row = summary[summary['source'] == source1].iloc[0]

    real_carbon = carbon_for_scenario(real_row['total_miles'], scenario)
    opt_carbon = carbon_for_scenario(opt_row['total_miles'], scenario)

    # --- view 1: headline metrics, real vs optimised --------------------
    st.subheader('Real vs. optimised season schedule')
    st.caption(
        'Deltas are optimised minus real, negative is an improvement and CO2 uses '
        'the transport scenario selected above.'
    )
    m1, m2, m3 = st.columns(3)
    # delta_color='inverse' on all three: up=red, down=green -- all three are
    # costs to minimise, so lower is always the improvement, unlike
    # Streamlit's own 'normal' default (up=green) which assumes the opposite.
    m1.metric('Total distance', fmt_distance(opt_row['total_miles'], unit1),
              fmt_distance(opt_row['total_miles'] - real_row['total_miles'], unit1) + ' vs. real',
              delta_color='inverse')
    m2.metric('Fatigue burden', eu_number(opt_row['fatigue_burden'], 1),
              eu_number(opt_row['fatigue_burden'] - real_row['fatigue_burden'], 1) + ' vs. real',
              delta_color='inverse')
    m3.metric(f'CO2 ({scenario})', f"{eu_number(opt_carbon)} kg",
              f"{eu_number(opt_carbon - real_carbon)} kg vs. real",
              delta_color='inverse')

    # --- view 3: Pareto frontier -----------------------------------------
    st.subheader('Fatigue burden vs. Carbon')
    st.caption(
        "Each point is one full schedule variant, not a single team or game. "
        " lambda 0.0-0.9, several of them landing on top of each other, plus one "
        "outlier (lambda=1.0, pure-carbon)."
    )
    frontier = summary.copy()
    frontier['kind'] = frontier['source'].apply(lambda s: 'Real schedule' if s == 'real' else 'Optimised schedule')
    frontier['lambda_label'] = frontier['lambda'].apply(lambda l: 'real' if pd.isna(l) else f'{l:g}')
    frontier['color_hex'] = frontier['kind'].map({'Real schedule': '#d62728', 'Optimised schedule': '#1f77b4'})
    frontier['point_size'] = frontier['kind'].map({'Real schedule': 400, 'Optimised schedule': 140})
    frontier['draw_order'] = frontier['kind'].map({'Real schedule': 0, 'Optimised schedule': 1})
    st.caption('🔴 Real schedule    🔵 Optimised schedule (one point per lambda)')

    chart = alt.Chart(frontier).mark_circle().encode(
        x=alt.X('fatigue_burden', title='Fatigue burden (total league)', scale=alt.Scale(zero=False)),
        y=alt.Y('carbon', title='Carbon, kg CO2 (charter)', scale=alt.Scale(zero=False)),
        color=alt.Color('color_hex', scale=None, legend=None),
        size=alt.Size('point_size', scale=alt.Scale(type='identity'), legend=None),
        order=alt.Order('draw_order'),
        tooltip=[
            alt.Tooltip('kind', title='schedule'),
            alt.Tooltip('lambda_label', title='lambda'),
            # format=',.2f': ',' turns on thousands grouping (using our
            # locale's '.' above, not a literal comma), '.2f' rounds to 2
            # decimals.
            alt.Tooltip('fatigue_burden', title='fatigue burden', format=',.2f'),
            alt.Tooltip('carbon', title='carbon (kg)', format=',.2f'),
            alt.Tooltip('total_miles', title='total miles', format=',.2f'),
        ],
    ).properties(height=420).interactive().configure(locale=EU_LOCALE)
    st.altair_chart(chart, width='stretch')
    # Pre-formatted as strings (same eu_number() used everywhere else in this
    # app) rather than left as raw floats -- st.dataframe's own numeric
    # formatting doesn't offer locale separator swapping, only precision.
    frontier_table = summary[['source', 'lambda', 'fatigue_burden', 'carbon', 'total_miles']] \
        .sort_values('lambda', na_position='first').copy()
    frontier_table['fatigue_burden'] = frontier_table['fatigue_burden'].apply(lambda v: eu_number(v, 1))
    frontier_table['carbon'] = frontier_table['carbon'].apply(lambda v: eu_number(v, 0))
    frontier_table['total_miles'] = frontier_table['total_miles'].apply(lambda v: eu_number(v, 0))
    st.dataframe(frontier_table, hide_index=True)

    # --- view 5: transport-scenario detail --------------------------------
    st.subheader('Transport-scenario comparison')
    st.caption(
        "Charter is the status quo (private aircraft, per-plane). Commercial and "
        "SAF-blend are posterior what-ifs -- see carbon/scenarios.py's module "
        "docstring for exactly what's real (fuel-burn geometry) vs. approximate "
        "(any passenger-based commercial figure) here."
    )
    scenario_table = pd.DataFrame([
        {
            'scenario': s,
            'real schedule (kg CO2)': eu_number(carbon_for_scenario(real_row['total_miles'], s)),
            f'optimised, lambda={lam1} (kg CO2)': eu_number(carbon_for_scenario(opt_row['total_miles'], s)),
        }
        for s in SCENARIOS
    ])
    st.dataframe(scenario_table, hide_index=True)
    st.caption(
        'The biggest real, no-fatigue-tradeoff win is switching the existing '
        'charter fleet to a SAF blend: same aircraft, same schedule, same '
        'recovery. Full commercial travel cuts carbon further but at a recovery '
        'cost. The real answer is probably some mix of the two.'
    )

# =====================================================================
# Tab 2 -- one team's own numbers, route map, Schedule Board, equity view.
# =====================================================================
with tab_team:
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        lam2 = st.select_slider(
            'Lambda (0 = pure fatigue-reduction, 1 = pure carbon-reduction)',
            options=LAMBDA_GRID, value=0.4, key='lam_tab2',
        )
    with col2:
        team = st.selectbox('Team', options=sorted(per_team['team'].unique()), key='team_tab2')
    with col3:
        unit2 = st.radio('Distance unit', ['mi', 'km'], horizontal=True, key='unit_tab2')

    source2 = f'lambda_{lam2}'

    pt_real = per_team[(per_team['team'] == team) & (per_team['source'] == 'real')].iloc[0]
    pt_opt = per_team[(per_team['team'] == team) & (per_team['source'] == source2)].iloc[0]

    real_games = schedules[
        (schedules['source'] == 'real')
        & ((schedules['home_team'] == team) | (schedules['away_team'] == team))
    ].sort_values('date').set_index('game_id')
    opt_games = schedules[
        (schedules['source'] == source2)
        & ((schedules['home_team'] == team) | (schedules['away_team'] == team))
    ].set_index('game_id')
    moved_mask = real_games['date'] != opt_games.loc[real_games.index, 'date']

    # --- team headline stats, incl. the games-moved count from the Schedule
    # Board mockup -- per-TEAM numbers, not the league totals in tab 1 above
    # (deliberately: this tab is about one team's own schedule, and a
    # league-wide number would feel disconnected from the cards below it).
    st.subheader(f'{team}: real vs. optimised')
    s1, s2, s3, s4 = st.columns(4)
    # Same delta_color='inverse' on all three as tab 1 -- see the comment
    # there: lower is always the improvement, for all three stats.
    s1.metric('Total distance', fmt_distance(pt_opt['total_miles'], unit2),
              fmt_distance(pt_opt['total_miles'] - pt_real['total_miles'], unit2) + ' vs. real',
              delta_color='inverse')
    s2.metric('Fatigue burden', eu_number(pt_opt['fatigue_burden'], 1),
              eu_number(pt_opt['fatigue_burden'] - pt_real['fatigue_burden'], 1) + ' vs. real',
              delta_color='inverse')
    s3.metric('CO2 (charter)', f"{eu_number(carbon_for_scenario(pt_opt['total_miles'], 'charter'))} kg",
              f"{eu_number(carbon_for_scenario(pt_opt['total_miles'], 'charter') - carbon_for_scenario(pt_real['total_miles'], 'charter'))} kg vs. real",
              delta_color='inverse')
    s4.metric('Games moved', f'{moved_mask.sum()} / {len(real_games)}')

    # --- view 2: per-team route map ------------------------------------------
    st.subheader('Per-team route map')
    st.caption(
        "Gradient colors for each trip: blue where the trip starts, red where it finishes."
    )
    which = st.radio('Show', options=['Real schedule', f'Optimised (lambda={lam2})'], horizontal=True)
    show_source = 'real' if which == 'Real schedule' else source2

    team_games = schedules[
        (schedules['source'] == show_source)
        & ((schedules['home_team'] == team) | (schedules['away_team'] == team))
    ].sort_values('date')

    # One record per game: location (home team's arena, EXCEPT the handful of
    # real neutral-site games -- Mexico City / Vegas Cup / Paris -- same lookup
    # schedule.py's location_for() uses, so a road trip through Paris draws the
    # real venue, not either team's home city) plus date/opponent, for the
    # tooltip below. Opponent is whichever side of the game ISN'T our team.
    game_records = [
        {
            'location': NEUTRAL_SITE_VENUES.get(row['game_id'], row['home_team']),
            'date': row['date'],
            'opponent': row['away_team'] if row['home_team'] == team else row['home_team'],
        }
        for _, row in team_games.iterrows()
    ]

    # Each arc is the travel INTO one game -- date/opponent describe the game
    # being travelled to, not the one just played.
    map_arcs = []
    for prev, cur in zip(game_records, game_records[1:]):
        if prev['location'] == cur['location']:
            continue  # consecutive home games at the same arena -- no leg to draw
        plat, plon = arenas.loc[prev['location'], ['lat', 'lon']]
        lat, lon = arenas.loc[cur['location'], ['lat', 'lon']]
        map_arcs.append({'from_lat': plat, 'from_lon': plon, 'to_lat': lat, 'to_lon': lon,
                          'date': str(cur['date']), 'opponent': cur['opponent']})

    # Chronological order, as arc HEIGHT -- early season = low arcs, late
    # season = low arcs, height is a separate visual channel from color so
    # it doesn't touch the blue=departure/red=arrival gradient below.
    for i, arc in enumerate(map_arcs):
        arc['height'] = 0.3 + (1 - i / max(len(map_arcs) - 1, 1)) * 0.9

    if map_arcs:
        layer = pdk.Layer(
            'ArcLayer', data=pd.DataFrame(map_arcs),
            get_source_position=['from_lon', 'from_lat'],
            get_target_position=['to_lon', 'to_lat'],
            get_width=2, get_source_color=[0, 128, 255], get_target_color=[255, 80, 80],
            get_height='height',
            pickable=True,  # required for hover/tooltip -- off by default
        )
        view_state = pdk.ViewState(latitude=39, longitude=-98, zoom=3)
        tooltip = {'html': '<b>vs {opponent}</b><br/>{date}',
                   'style': {'backgroundColor': 'steelblue', 'color': 'white'}}
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state,
                                 map_style=None, tooltip=tooltip))
    else:
        st.info('No travel legs to draw for this team/source.')

    # --- Schedule Board: every game as a card, moved games flagged -----------
    # Native Streamlit components throughout (st.container/st.markdown), NOT
    # the dark custom-CSS board from the design mockup -- this tab should
    # look like the rest of the app, in the viewer's own Streamlit theme.
    st.subheader('Schedule board')
    st.caption(
        f"All {len(real_games)} games for {team}, real dates. An orange "
        f":orange[MOVED] tag and border mark a game the optimiser gave a "
        f"different date at lambda={lam2}; the line under it shows the change."
    )
    CARDS_PER_ROW = 4
    game_ids = list(real_games.index)
    moved_ids = []
    for row_start in range(0, len(game_ids), CARDS_PER_ROW):
        row_ids = game_ids[row_start:row_start + CARDS_PER_ROW]
        card_cols = st.columns(CARDS_PER_ROW)
        for col, gid in zip(card_cols, row_ids):
            r = real_games.loc[gid]
            is_home = r['home_team'] == team
            opponent = r['away_team'] if is_home else r['home_team']
            venue_key = NEUTRAL_SITE_VENUES.get(gid, r['home_team'])
            new_date = opt_games.loc[gid, 'date']
            changed = r['date'] != new_date
            if changed:
                moved_ids.append(gid)
            status_line = f"{r['date']}  ·  {'HOME' if is_home else 'AWAY'}"
            if changed:
                status_line += '  ·  :orange[MOVED]'
            with col:
                # key=... gives this container's wrapper a stable
                # '.st-key-card_<gid>' CSS class (a documented Streamlit
                # pattern), which the single st.html() call below uses to
                # border just the moved cards -- not a full custom theme.
                with st.container(border=True, key=f'card_{gid}'):
                    st.caption(status_line)
                    st.markdown(f"**{opponent}**")
                    st.caption(f"{arenas.loc[venue_key, 'arena']} · {arenas.loc[venue_key, 'city']}")
                    if changed:
                        st.caption(f"~~{r['date']}~~ → **{new_date}**")
                    else:
                        st.caption('No change')

    if moved_ids:
        selector = ', '.join(f'.st-key-card_{gid}' for gid in moved_ids)
        st.html(f'<style>{selector} {{ border-color: orange !important; }}</style>')

    # --- view 4: per-team fatigue distribution (the equity view) -------------
    st.subheader('Per-team fatigue burden (the equity view)')
    compare = per_team[per_team['source'].isin(['real', source2])].pivot(
        index='team', columns='source', values='fatigue_burden'
    ).rename(columns={'real': 'Real', source2: f'Optimised (lambda={lam2})'})
    # A plain st.bar_chart can't have its tooltip format/locale controlled,
    # so this is a hand-built Altair grouped bar chart instead (melted back
    # to long form, xOffset groups the two bars per team) -- same EU_LOCALE
    # + format=',.2f' pattern as the Pareto frontier chart above.
    compare_long = compare.reset_index().melt(id_vars='team', var_name='schedule', value_name='fatigue_burden')
    bar_chart = alt.Chart(compare_long).mark_bar().encode(
        x=alt.X('team', sort=None, title=None),
        xOffset=alt.XOffset('schedule'),
        y=alt.Y('fatigue_burden', title='Fatigue burden'),
        color=alt.Color('schedule', title=None),
        tooltip=[alt.Tooltip('team'), alt.Tooltip('schedule', title='schedule'),
                 alt.Tooltip('fatigue_burden', title='fatigue burden', format=',.2f')],
    ).properties(height=400).configure(locale=EU_LOCALE)
    st.altair_chart(bar_chart, width='stretch')
