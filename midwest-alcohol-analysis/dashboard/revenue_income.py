from pathlib import Path
import pandas as pd
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "output" / "dashboard"

# -------------------------------
#   Load Revenue (Iowa/WI/MN)
# -------------------------------
def load_state_dfs_income() -> dict:
    dfs = {}
    for name in ["iowa", "wisconsin", "minnesota"]:
        p = DATA_ROOT / f"{name}_dashboard_data.csv"
        df = pd.read_csv(p, usecols=["State_Name", "Year", "Month", "Total_Revenue"], na_values="null")

        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
        df["Month"] = pd.to_numeric(df["Month"], errors="coerce").astype("Int64")
        df["Year_Month"] = pd.to_datetime(
            df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-01",
            errors="coerce"
        )

        df = df.dropna(subset=["Year_Month", "Total_Revenue"])
        df["State_Name"] = df["State_Name"].astype(str).str.lower()

        dfs[name] = df.sort_values("Year_Month").reset_index(drop=True)

    return dfs


# -------------------------------
#   Load Income dataset
# -------------------------------
def load_income_df() -> pd.DataFrame:
    p = DATA_ROOT / "pop_income_dashboard_data.csv"
    df = pd.read_csv(p, na_values="null")

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Month"] = pd.to_numeric(df["Month"], errors="coerce").astype("Int64")
    df["Year_Month"] = pd.to_datetime(
        df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-01",
        errors="coerce"
    )

    df = df.rename(columns=lambda c: c.strip())

    needed = {"State_Name", "Year_Month", "Personal_Income", "Per_Capita_Personal_Income"}
    df = df.dropna(subset=needed)
    df["State_Name"] = df["State_Name"].astype(str).str.lower()

    return df.sort_values("Year_Month").reset_index(drop=True)


# -------------------------------
#   Colors
# -------------------------------
field_colors = {
    "iowa_rev": "#d62728",
    "wisconsin_rev": "#ff69b4",
    "minnesota_rev": "#20b2aa",

    "iowa_income": "#ffd700",
    "wisconsin_income": "#00bfff",
    "minnesota_income": "#1f77b4",

    "iowa_per": "#9acd32",
    "wisconsin_per": "#ff7f0e",
    "minnesota_per": "#006400",
}


# -------------------------------
#   Layout
# -------------------------------
def create_dashboard_3():
    return html.Div([
        html.H1("Revenue vs Income"),

        html.Div([
            html.Label("Select States:"),
            dcc.Checklist(
                id={"type": "state-choice", "tab": "si"},
                options=[
                    {"label": "Iowa", "value": "iowa"},
                    {"label": "Wisconsin", "value": "wisconsin"},
                    {"label": "Minnesota", "value": "minnesota"},
                ],
                value=["iowa"],
                inline=True,
            ),
        ], style={"marginBottom": "10px"}),

        html.Div([
            html.Label("Select Income Metric(s):"),
            dcc.Checklist(
                id={"type": "income-metric", "tab": "si"},
                options=[
                    {"label": "Total Personal Income", "value": "Personal_Income"},
                    {"label": "Per-Capita Personal Income", "value": "Per_Capita_Personal_Income"},
                ],
                value=["Personal_Income"],
                inline=True,
            ),
        ], style={"marginBottom": "10px"}),

        dcc.Graph(id={"type": "graph", "tab": "si"}, style={"height": "800px"}),
    ])


# -------------------------------
#   Callback
# -------------------------------
def register_callbacks(app, state_dfs, income_df):

    @app.callback(
        Output({"type": "graph", "tab": "si"}, "figure"),
        Input({"type": "state-choice", "tab": "si"}, "value"),
        Input({"type": "income-metric", "tab": "si"}, "value"),
    )
    def update_figure(selected_states, selected_income):

        # ---- Debug：印最大日期 ----
        print("Revenue max:", {s: df["Year_Month"].max() for s, df in state_dfs.items()})
        print("Income max:", income_df["Year_Month"].max())

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # ---- Revenue ----
        for st in selected_states:
            rev_df = state_dfs.get(st)
            if rev_df is None or rev_df.empty:
                continue

            fig.add_trace(
                go.Scatter(
                    x=rev_df["Year_Month"],
                    y=rev_df["Total_Revenue"] / 1000,
                    name=f"{st.title()} Revenue",
                    mode="lines+markers",
                    line=dict(color=field_colors[f"{st}_rev"], width=3),
                ),
                secondary_y=False,
            )

        # ---- Income ----
        for st in selected_states:
            inc_sub = income_df[income_df["State_Name"] == st]
            if inc_sub.empty:
                continue

            if "Personal_Income" in selected_income:
                fig.add_trace(
                    go.Scatter(
                        x=inc_sub["Year_Month"],
                        y=inc_sub["Personal_Income"],
                        name=f"{st.title()} Total Personal Income",
                        mode="lines+markers",
                        line=dict(color=field_colors[f"{st}_income"], dash="dot", width=2),
                    ),
                    secondary_y=True,
                )

            if "Per_Capita_Personal_Income" in selected_income:
                fig.add_trace(
                    go.Scatter(
                        x=inc_sub["Year_Month"],
                        y=inc_sub["Per_Capita_Personal_Income"],
                        name=f"{st.title()} Per-Capita Income",
                        mode="lines+markers",
                        line=dict(color=field_colors[f"{st}_per"], dash="dot", width=2),
                    ),
                    secondary_y=True,
                )

        # ---- Layout ----
        fig.update_layout(
            template="plotly_white",
            height=800,
            margin=dict(l=80, r=70, t=120, b=80),
            legend=dict(x=0, y=1.2, orientation="h", font=dict(size=12)),
        )

        fig.update_yaxes(title_text="Revenue (k$)", secondary_y=False)
        fig.update_yaxes(title_text="Income", secondary_y=True)

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
