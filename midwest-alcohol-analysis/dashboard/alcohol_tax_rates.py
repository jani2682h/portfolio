from pathlib import Path
import pandas as pd
from dash import dcc, html, Input, Output, MATCH
import plotly.graph_objects as go

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "output" / "dashboard" / "alcohol_tax.csv"

def load_tax_df():
    df = pd.read_csv(CSV_PATH, na_values=["null", "NULL", "NaN"])
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    cols = [
        "state_code",
        "state_name",
        "rate_per_liter_beer",
        "rate_per_liter_wine",
        "rate_per_liter_liquor",
    ]
    df = df[cols]
    for c in cols[2:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    state_dfs = {row["state_name"]: row for _, row in df.iterrows()}
    return state_dfs, df

def create_dashboard_4(state_dfs, df):
    drink_opts = [
        {"label": "Beer", "value": "rate_per_liter_beer"},
        {"label": "Wine", "value": "rate_per_liter_wine"},
        {"label": "Liquor", "value": "rate_per_liter_liquor"},
    ]
    state_opts = [{"label": s.title(), "value": s} for s in sorted(df["state_name"].dropna().unique())]
    default_state_vals = [s for s in df["state_name"]]
    layout = html.Div([
        html.H2("Alcohol Tax Rates per Liter"),
        html.Div([
            html.Label("Select drinks:"),
            dcc.Checklist(
                id={"type": "drink-checklist", "tab": "tx"},
                options=drink_opts,
                value=[opt["value"] for opt in drink_opts],
                inline=True,
            ),
        ], style={"marginBottom": "15px"}),
        html.Div([
            html.Label("Select states:"),
            dcc.Checklist(
                id={"type": "state-checklist", "tab": "tx"},
                options=state_opts,
                value=default_state_vals,
                inline=True,
            ),
        ], style={"marginBottom": "20px"}),
        dcc.Graph(id={"type": "tax-bar", "tab": "tx"}, style={"height": "700px"}),
    ], style={"margin": "20px"})
    return layout

def register_callbacks(app, df):
    @app.callback(
        Output({"type": "tax-bar", "tab": "tx"}, "figure"),
        Input({"type": "drink-checklist", "tab": "tx"}, "value"),
        Input({"type": "state-checklist", "tab": "tx"}, "value"),
    )
    def update_bar(selected_drinks, selected_states):
        if not selected_drinks or not selected_states:
            fig = go.Figure()
            fig.update_layout(title="Please select at least one drink and one state")
            return fig
        filtered = df[df["state_name"].isin(selected_states)]
        long_df = filtered.melt(
            id_vars=["state_name"],
            value_vars=selected_drinks,
            var_name="drink_type",
            value_name="rate_per_liter",
        )
        pretty = {
            "rate_per_liter_beer": "Beer",
            "rate_per_liter_wine": "Wine",
            "rate_per_liter_liquor": "Liquor",
        }
        long_df["drink_type"] = long_df["drink_type"].map(pretty)
        fig = go.Figure()
        for drink in long_df["drink_type"].unique():
            sub = long_df[long_df["drink_type"] == drink]
            fig.add_trace(
                go.Bar(
                    x=sub["state_name"],
                    y=sub["rate_per_liter"],
                    name=drink,
                    hovertemplate="%{x}<br>%{y:.3f} $/L",
                )
            )
        fig.update_layout(
            title="",
            xaxis_title="State",
            yaxis_title="Tax Rate ($/L)",
            barmode="group",
            template="plotly_white",
            legend_title_text="Drink Type",
            xaxis_tickangle=-45,
            height=800,
            margin=dict(l=90, r=70, t=120, b=80),
            legend=dict(x=0, y=1.2, orientation="h", font=dict(size=12)),
        )
        return fig