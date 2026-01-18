from pathlib import Path
import pandas as pd
from dash import dcc, html, Input, Output, MATCH
import plotly.graph_objects as go
from plotly.subplots import make_subplots

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "output" / "dashboard"

def load_state_dfs_population() -> dict:
    dfs = {}
    for name in ["iowa", "wisconsin", "minnesota"]:
        p = DATA_ROOT / f"{name}_dashboard_data.csv"
        df = pd.read_csv(p, na_values="null")
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
        df["Month"] = pd.to_numeric(df["Month"], errors="coerce").astype("Int64")
        df["Year_Month"] = pd.to_datetime(
            df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-01",
            errors="coerce",
        )
        df = df.dropna(subset=["Year_Month", "Total_Revenue", "Total_Volume_Liters"])
        df["State_Name"] = df["State_Name"].astype(str).str.lower()
        df = df.dropna(subset=["State_Name"])
        dfs[name] = df.sort_values("Year_Month").reset_index(drop=True)
    return dfs

def load_population_df() -> pd.DataFrame:
    p = DATA_ROOT / "pop_income_dashboard_data.csv"
    df = pd.read_csv(p, na_values="null")
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Month"] = pd.to_numeric(df["Month"], errors="coerce").astype("Int64")
    df["Year_Month"] = pd.to_datetime(
        df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-01",
        errors="coerce",
    )
    rename = {}
    for col in df.columns:
        low = col.lower()
        if "state" in low:
            rename[col] = "State_Name"
        elif low == "population" or ("pop" in low and "total" in low):
            rename[col] = "total_pop"
    df = df.rename(columns=rename)

    # Keep full record
    df = df.dropna(subset=["Year_Month", "State_Name"])
    df["State_Name"] = df["State_Name"].astype(str).str.lower()
    return df.sort_values("Year_Month").reset_index(drop=True)

field_colors = {
    "iowa_rev": "#d62728",
    "wisconsin_rev": "#ff7f0e",
    "minnesota_rev": "#20b2aa",
    "iowa_vol": "#ffd700",
    "wisconsin_vol": "#31a354",
    "minnesota_vol": "#dda0dd",
    "iowa_total_pop": "#1f77b4",
    "wisconsin_total_pop": "#2ca02c",
    "minnesota_total_pop": "#9467bd",
}

def create_dashboard_2():
    layout = html.Div(
        [
            html.H2("Revenue / Volume / Population"),
            html.Div(
                [
                    html.Label("Select States:"),
                    dcc.Checklist(
                        id={"type": "state-choice", "tab": "pv"},
                        options=[
                            {"label": "Iowa", "value": "iowa"},
                            {"label": "Wisconsin", "value": "wisconsin"},
                            {"label": "Minnesota", "value": "minnesota"},
                        ],
                        value=["iowa", "wisconsin", "minnesota"],
                        inline=True,
                    ),
                ],
                style={"marginBottom": "10px"},
            ),
            html.Div(
                [
                    html.Label("Select Metrics:"),
                    dcc.Checklist(
                        id={"type": "metric-choice", "tab": "pv"},
                        options=[
                            {"label": "Revenue", "value": "rev"},
                            {"label": "Volume", "value": "vol"},
                            {"label": "Total Population", "value": "total_pop"},
                        ],
                        value=["rev", "vol", "total_pop"],
                        inline=True,
                    ),
                ],
                style={"marginBottom": "10px"},
            ),
            dcc.Graph(id={"type": "graph", "tab": "pv"}, style={"height": "800px"}),
        ]
    )
    return layout

def register_callbacks(app, state_dfs, population_df):
    @app.callback(
        Output({"type": "graph", "tab": "pv"}, "figure"),
        Input({"type": "state-choice", "tab": "pv"}, "value"),
        Input({"type": "metric-choice", "tab": "pv"}, "value"),
    )
    def update_figure(selected_states, metrics):
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        if "vol" in metrics:
            opacity = 1.0 if metrics == ["vol"] else 0.6
            for st in selected_states:
                df = state_dfs.get(st)
                if df is None or df.empty:
                    continue
                key = f"{st}_vol"
                fig.add_trace(
                    go.Bar(
                        x=df["Year_Month"],
                        y=df["Total_Volume_Liters"],
                        name=f"{st.title()} Volume",
                        marker=dict(color=field_colors[key], opacity=opacity),
                    ),
                    secondary_y=True,
                )
        if "rev" in metrics:
            for st in selected_states:
                df = state_dfs.get(st)
                if df is None or df.empty:
                    continue
                key = f"{st}_rev"
                fig.add_trace(
                    go.Scatter(
                        x=df["Year_Month"],
                        y=df["Total_Revenue"] / 1000,
                        name=f"{st.title()} Revenue",
                        mode="lines+markers",
                        line=dict(color=field_colors[key], width=3),
                    ),
                    secondary_y=False,
                )
        if "total_pop" in metrics:
            for st in selected_states:
                sub = population_df[population_df["State_Name"] == st]
                if sub.empty:
                    continue
                # Keep total_pop , ignore NaN
                sub = sub.dropna(subset=["total_pop"])
                if sub.empty:
                    continue
                key = f"{st}_total_pop"
                fig.add_trace(
                    go.Scatter(
                        x=sub["Year_Month"],
                        y=sub["total_pop"],
                        name=f"{st.title()} Total Population",
                        mode="lines+markers",
                        line=dict(color=field_colors[key], dash="dot", width=2),
                    ),
                    secondary_y=True,
                )
        fig.update_layout(
            title="",
            template="plotly_white",
            height=800,
            margin=dict(l=80, r=70, t=120, b=80),
            legend=dict(x=0, y=1.2, orientation="h"),
        )
        fig.update_yaxes(title_text="Revenue (k$)", secondary_y=False)
        fig.update_yaxes(title_text="Volume / Total Population", secondary_y=True, showgrid=False)

        # get global min/max of revenue dates
        min_date = min(df["Year_Month"].min() for df in state_dfs.values())
        max_date = max(df["Year_Month"].max() for df in state_dfs.values())

        fig.update_xaxes(
            tickformat="%Y/%m",
            rangeslider_visible=True,
            range=[min_date, max_date],
            rangeselector=dict(
                buttons=[
                    dict(count=3, step="month", stepmode="backward", label="3M"),
                    dict(count=6, step="month", stepmode="backward", label="6M"),
                    dict(count=1, step="year", stepmode="backward", label="1Y"),
                    dict(step="all"),
                ]
            ),
        )
        return fig