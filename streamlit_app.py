import streamlit as st
import pandas as pd
import glob
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# Page Setup
st.set_page_config("📈 Real-Time Market Dashboard", layout="wide")
st.title("📈 Real-Time Market Stock Dashboard")

# Auto-refresh every 60s
st.experimental_rerun_interval = 60

# Load data
files = sorted(glob.glob("output/filtered_last_10_minutes/part-*.csv"))
if not files:
    st.warning("No data found yet.")
    st.stop()

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
df['window_start'] = pd.to_datetime(df['window_start'])

# Sidebar Filter Panel
st.sidebar.header("🔎 Filters")
tickers = sorted(df['ticker'].unique())
selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)

# Date Range
min_date = df['window_start'].min().date()
max_date = df['window_start'].max().date()

# Filter by selection
subset = df[(df['ticker'] == selected_ticker)].sort_values("window_start")

if subset.empty:
    st.warning("No data available for selected date range.")
    st.stop()

# Latest Update + Metrics
last = subset.iloc[-1]
st.markdown(f"📅 **Last Updated:** `{last['window_start']}`")
k1, k2, k3 = st.columns(3)
k1.metric("📊 Avg Price", f"{last['avg_price']:.2f}")
k2.metric("💥 Volatility", f"{last['volatility']:.2f}")
k3.metric("🔁 % Change", f"{last['pct_change']:.2f}%", delta_color="inverse")

# Alerts
if last['pct_change'] < -2:
    st.error(f"⚠️ Price drop alert: {last['pct_change']:.2f}% in the last interval.")
elif last['pct_change'] > 2:
    st.success(f"📈 Price surge: {last['pct_change']:.2f}%!")

if last['volatility'] > 5:
    st.warning(f"🔔 High volatility detected: {last['volatility']:.2f}")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Avg Price & Volatility", "📉 % Change Trend", "📍 Compare All Tickers", "📁 Raw Data"
])

# Tab 1: Avg Price and Volatility
with tab1:
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=subset['window_start'],
        y=subset['avg_price'],
        mode='lines+markers',
        name='Avg Price',
        line=dict(color='green' if last['pct_change'] >= 0 else 'red'),
    ))

    fig.add_trace(go.Scatter(
        x=subset['window_start'],
        y=subset['volatility'],
        mode='lines+markers',
        name='Volatility',
        line=dict(dash='dot', color='orange'),
        yaxis='y2'
    ))

    fig.update_layout(
        title=f"{selected_ticker} — Avg Price vs Volatility",
        xaxis_title="Time",
        yaxis=dict(title="Avg Price"),
        yaxis2=dict(title="Volatility", overlaying="y", side="right"),
        legend=dict(orientation="h"),
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

# Tab 2: % Change Over Time
with tab2:
    fig2 = px.line(subset, x="window_start", y="pct_change",
                   title=f"{selected_ticker} — Percentage Change",
                   labels={"pct_change": "% Change", "window_start": "Time"},
                   markers=True)
    fig2.add_hline(y=0, line_dash="dash", line_color="gray")
    fig2.update_traces(line_color='blue')
    st.plotly_chart(fig2, use_container_width=True)

# Tab 3: Compare All Tickers
with tab3:
    st.markdown("### 📍 Comparison Across Tickers")

    # --- Subplot: Avg Price ---
    fig_avg = go.Figure()
    for ticker in tickers:
        temp = df[df['ticker'] == ticker].sort_values("window_start")
        fig_avg.add_trace(go.Scatter(
            x=temp["window_start"], y=temp["avg_price"],
            mode='lines', name=ticker
        ))
    fig_avg.update_layout(
        title="Avg Price Over Time",
        xaxis_title="Time", yaxis_title="Avg Price", height=400,
        legend=dict(orientation="h")
    )
    st.plotly_chart(fig_avg, use_container_width=True)

    # --- Subplot: Volatility ---
    fig_vol = go.Figure()
    for ticker in tickers:
        temp = df[df['ticker'] == ticker].sort_values("window_start")
        fig_vol.add_trace(go.Scatter(
            x=temp["window_start"], y=temp["volatility"],
            mode='lines', name=ticker
        ))
    fig_vol.update_layout(
        title="Volatility Over Time",
        xaxis_title="Time", yaxis_title="Volatility", height=400,
        legend=dict(orientation="h")
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    # --- Subplot: % Change ---
    fig_pct = go.Figure()
    for ticker in tickers:
        temp = df[df['ticker'] == ticker].sort_values("window_start")
        fig_pct.add_trace(go.Scatter(
            x=temp["window_start"], y=temp["pct_change"],
            mode='lines', name=ticker
        ))
    fig_pct.update_layout(
        title="% Change Over Time",
        xaxis_title="Time", yaxis_title="% Change", height=400,
        legend=dict(orientation="h")
    )
    st.plotly_chart(fig_pct, use_container_width=True)

    # --- Table: Latest snapshot comparison ---
    st.markdown("### 📋 Latest Metrics Snapshot")
    latest_df = df.sort_values("window_start").groupby("ticker").tail(1)
    latest_df = latest_df.sort_values("avg_price", ascending=False)

    latest_df_display = latest_df[[
        "ticker", "window_start", "avg_price", "volatility", "pct_change"
    ]].rename(columns={
        "ticker": "Ticker",
        "window_start": "Last Update",
        "avg_price": "Avg Price",
        "volatility": "Volatility",
        "pct_change": "% Change"
    })

    st.dataframe(latest_df_display, use_container_width=True)

    # --- Highlight top gainers and losers ---
    st.markdown("### 🏅 Top Movers")
    top_gainers = latest_df_display.sort_values("% Change", ascending=False).head(3)
    top_losers = latest_df_display.sort_values("% Change").head(3)

    col1, col2 = st.columns(2)
    with col1:
        st.success("📈 Top Gainers")
        st.table(top_gainers.set_index("Ticker")[["% Change", "Avg Price"]])
    with col2:
        st.error("📉 Top Losers")
        st.table(top_losers.set_index("Ticker")[["% Change", "Avg Price"]])


# Tab 4: Raw Data Table + Export
# 1. Rename columns
subset_display = subset.rename(columns={
    "window_start":    "Time",
    "avg_price":       "Average Price",
    "volatility":      "Volatility",
    "market_status":   "Market Status",
    "pct_change":      "Percent Change"     # if you have this column
})

# 2. Map market_status values
subset_display["Market Status"] = subset_display["Market Status"].map({
    0: "Closed",
    1: "Open"
})

# 3. Display and download in tab4
with tab4:
    st.dataframe(
        subset_display.sort_values("Time", ascending=False),
        use_container_width=True
    )
    csv = subset_display.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download CSV",
        csv,
        f"{selected_ticker}_data.csv",
        "text/csv"
    )
