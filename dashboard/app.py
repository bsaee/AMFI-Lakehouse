from pathlib import Path
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_LEADERBOARD_PATH = PROJECT_ROOT / "data" / "gold" / "category_leaderboard"
GOLD_DAILY_METRICS_PATH = PROJECT_ROOT / "data" / "gold" / "fund_daily_metrics"
SILVER_QUARANTINE_PATH = PROJECT_ROOT / "data" / "silver" / "quarantine"

st.set_page_config(
    page_title="AMFI Mutual Fund Lakehouse",
    page_icon="📈",
    layout="wide",
)


@st.cache_data(ttl=3600)
def load_leaderboard() -> pd.DataFrame:
    return DeltaTable(str(GOLD_LEADERBOARD_PATH)).to_pandas()


@st.cache_data(ttl=3600)
def load_quarantine_count() -> int:
    if not SILVER_QUARANTINE_PATH.exists():
        return 0
    return len(DeltaTable(str(SILVER_QUARANTINE_PATH)).to_pandas())


@st.cache_data(ttl=3600)
def load_fund_history(scheme_code: int) -> pd.DataFrame:
    dt = DeltaTable(str(GOLD_DAILY_METRICS_PATH))
    df = dt.to_pandas(filters=[("scheme_code", "=", scheme_code)])
    df["nav_date"] = pd.to_datetime(df["nav_date"])
    return df.sort_values("nav_date")


st.title("📈 AMFI Mutual Fund Lakehouse Analytics")
st.caption(
    "Medallion Lakehouse (Bronze → Silver → Gold) powered by PySpark, Delta Lake, Pandera & Spark SQL"
)

if not GOLD_LEADERBOARD_PATH.exists() or not GOLD_DAILY_METRICS_PATH.exists():
    st.error("Gold Delta tables not found. Run `python -m amfi_lakehouse.gold` first.")
    st.stop()

leaderboard_df = load_leaderboard()
quarantine_count = load_quarantine_count()

# --- Top KPI Row ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Active Schemes Tracked", f"{len(leaderboard_df):,}")
col2.metric("Latest NAV Date", str(leaderboard_df["latest_nav_date"].max()))
col3.metric("Quarantined Bad Records", f"{quarantine_count:,}")

valid_3y = leaderboard_df["cagr_3y"].dropna()
top_cagr = f"{valid_3y.max() * 100:.2f}%" if not valid_3y.empty else "N/A"
col4.metric("Top 3Y Annualized CAGR", top_cagr)

st.divider()

# --- Section 1: Category Leaderboard ---
st.subheader("🏆 Category Leaderboard (Spark SQL Window Rankings)")

filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 1])
categories = sorted(leaderboard_df["scheme_category"].dropna().unique().tolist())
default_cat_idx = next(
    (i for i, c in enumerate(categories) if "Large Cap" in c),
    0,
)
selected_category = filter_col1.selectbox("Scheme Category", categories, index=default_cat_idx)

plans = ["All"] + sorted(leaderboard_df["plan"].dropna().unique().tolist())
selected_plan = filter_col2.selectbox("Plan Type", plans, index=1 if "Direct Plan" in plans else 0)

sort_metric = filter_col3.selectbox(
    "Rank By",
    options=["cagr_3y", "return_1y", "return_1m", "volatility_30d_ann"],
    format_func=lambda x: {
        "cagr_3y": "3Y CAGR",
        "return_1y": "1Y Return",
        "return_1m": "1M Return",
        "volatility_30d_ann": "30D Volatility (Low→High)",
    }[x],
)

filtered_board = leaderboard_df[leaderboard_df["scheme_category"] == selected_category].copy()
if selected_plan != "All":
    filtered_board = filtered_board[filtered_board["plan"] == selected_plan]

ascending = sort_metric == "volatility_30d_ann"
filtered_board = filtered_board.sort_values(
    by=sort_metric, ascending=ascending, na_position="last"
).head(20)

display_cols = [
    "scheme_code",
    "scheme_name",
    "amc_name",
    "plan",
    "latest_nav",
    "return_1m",
    "return_1y",
    "cagr_3y",
    "volatility_30d_ann",
    "max_drawdown_history",
    "category_rank_3y",
]
formatted_board = filtered_board[display_cols].copy()
for pct_col in ["return_1m", "return_1y", "cagr_3y", "volatility_30d_ann", "max_drawdown_history"]:
    formatted_board[pct_col] = formatted_board[pct_col].apply(
        lambda v: f"{v * 100:.2f}%" if pd.notnull(v) else "—"
    )

st.dataframe(formatted_board, use_container_width=True, hide_index=True)

st.divider()

# --- Section 2: Fund Deep-Dive (NAV, Rolling Volatility & Drawdown) ---
st.subheader("🔍 Fund Performance & Risk Deep-Dive")

search_col1, search_col2 = st.columns([1, 2])
amcs = ["All AMCs"] + sorted(leaderboard_df["amc_name"].dropna().unique().tolist())
selected_amc = search_col1.selectbox("Filter by Fund House (AMC)", amcs)

scheme_pool = leaderboard_df if selected_amc == "All AMCs" else leaderboard_df[leaderboard_df["amc_name"] == selected_amc]
scheme_pool = scheme_pool.sort_values("scheme_name")
scheme_options = {
    f"{row['scheme_name']} ({row['plan'] or 'N/A'} | Code: {row['scheme_code']})": int(row["scheme_code"])
    for _, row in scheme_pool.iterrows()
}

selected_label = search_col2.selectbox("Select Mutual Fund Scheme", list(scheme_options.keys()))
selected_code = scheme_options[selected_label]

history_df = load_fund_history(selected_code)

if history_df.empty:
    st.warning("No historical time-series rows found for this scheme.")
else:
    latest_row = history_df.iloc[-1]
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Latest NAV", f"₹{latest_row['nav']:.4f}")
    m2.metric(
        "1Y Return",
        f"{latest_row['return_1y'] * 100:.2f}%" if pd.notnull(latest_row["return_1y"]) else "N/A",
    )
    m3.metric(
        "30D Annualized Volatility",
        f"{latest_row['volatility_30d_ann'] * 100:.2f}%"
        if pd.notnull(latest_row["volatility_30d_ann"])
        else "N/A",
    )
    m4.metric("Max Historical Drawdown", f"{history_df['drawdown'].min() * 100:.2f}%")

    chart_df = history_df.set_index("nav_date")

    st.markdown("**Net Asset Value (NAV) History**")
    st.line_chart(chart_df[["nav"]], height=280)

    c_left, c_right = st.columns(2)
    with c_left:
        st.markdown("**30-Day Rolling Annualized Volatility**")
        st.line_chart(chart_df[["volatility_30d_ann"]], height=240)
    with c_right:
        st.markdown("**Running Drawdown from Peak**")
        st.area_chart(chart_df[["drawdown"]], height=240)