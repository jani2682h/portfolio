import dash
from dash import dcc, html, Input, Output

# Revenue / Volume / Weather
from dashboard.revenue_volume_weather import (
    create_dashboard_1,
    load_state_dfs_weather,
    load_weather_df,
    register_callbacks as register_wv,
)

# Revenue / Volume / Population
from dashboard.revenue_volume_population import (
    create_dashboard_2,
    load_state_dfs_population,
    load_population_df,
    register_callbacks as register_pv,
)

# Revenue vs Income
from dashboard.revenue_income import (
    create_dashboard_3,
    load_state_dfs_income,
    load_income_df,
    register_callbacks as register_si,
)

# Alcohol Tax Rates
from dashboard.alcohol_tax_rates import (
    create_dashboard_4,
    load_tax_df,
    register_callbacks as register_tx,
)

app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Load data once for all tabs
state_dfs_wv = load_state_dfs_weather()
weather_df   = load_weather_df()

state_dfs_pv = load_state_dfs_population()
population_df = load_population_df()

state_dfs_si = load_state_dfs_income()
income_df    = load_income_df()

state_dfs_tx, tax_df = load_tax_df()

# Build layouts
layout_wv = create_dashboard_1()
layout_pv = create_dashboard_2()
layout_si = create_dashboard_3()
layout_tx = create_dashboard_4(state_dfs_tx, tax_df)

# Page with tabs
app.layout = html.Div([
    html.H1("Midwestern Alcohol Dashboard"),
    dcc.Tabs(id="tabs", value="wv", children=[
        dcc.Tab(label="Revenue / Volume / Weather", value="wv"),
        dcc.Tab(label="Revenue / Volume / Population", value="pv"),
        dcc.Tab(label="Revenue vs Income", value="si"),
        dcc.Tab(label="Alcohol Tax Rates", value="tx"),
    ]),
    html.Div(id="tab-content")
])

@app.callback(Output("tab-content", "children"), Input("tabs", "value"))
def render_tab(tab):
    if tab == "wv":  return layout_wv
    if tab == "pv":  return layout_pv
    if tab == "si":  return layout_si
    if tab == "tx":  return layout_tx
    return html.Div("Unknown Tab")

# Register callbacks for each tab
register_wv(app, state_dfs_wv, weather_df)          # Tab 1
register_pv(app, state_dfs_pv, population_df)      # Tab 2
register_si(app, state_dfs_si, income_df)           # Tab 3
register_tx(app, tax_df)                            # Tab 4

if __name__ == "__main__":
    app.run(debug=False, port=8050)