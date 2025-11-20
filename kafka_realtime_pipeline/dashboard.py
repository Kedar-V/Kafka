# dashboard.py
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# -------------------------------------------------------
# Streamlit page setup
# -------------------------------------------------------
st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-Sharing Analytics Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)

# -------------------------------------------------------
# Database Loaders
# -------------------------------------------------------

def load_trips(status_filter="All", limit=500):
    query = "SELECT * FROM trips_ml"
    params = {}
    if status_filter != "All":
        query += " WHERE status = :status"
        params["status"] = status_filter

    query += " ORDER BY start_time DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading trip data: {e}")
        return pd.DataFrame()


# -------------------------------------------------------
# Sidebar Controls
# -------------------------------------------------------
st.sidebar.header("🔧 Controls")

status_filter = st.sidebar.selectbox(
    "Filter by Trip Status",
    ["All", "Requested", "Ongoing", "Completed", "Cancelled"]
)

limit_records = st.sidebar.slider(
    "Number of recent trips to load:",
    min_value=100, max_value=5000, value=500, step=100
)

update_interval = st.sidebar.slider(
    "Auto-refresh interval (seconds):",
    min_value=3, max_value=30, value=5
)

st.sidebar.info("⏱ Auto-refresh is enabled")


# -------------------------------------------------------
# MAIN DASHBOARD
# -------------------------------------------------------

placeholder = st.empty()

with placeholder.container():

    df = load_trips(status_filter=status_filter, limit=limit_records)

    st.markdown(f"### Showing {len(df)} trips (filtered by **{status_filter}**)")

    if df.empty:
        st.warning("⚠ No trip data found. Waiting for events...")

    else:
        # Convert timestamps
        df["start_time"] = pd.to_datetime(df["start_time"])
        # No end_time column in trips_ml

        # ----------------------------------------
        # KPIs
        # ----------------------------------------
        total_trips = len(df)
        total_revenue = df["fare"].sum()
        avg_distance = df["distance_km"].mean()
        avg_fare = df["fare"].mean()
        completion_rate = len(df[df["status"] == "Completed"]) / total_trips * 100

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Trips", f"{total_trips:,}")
        k2.metric("Total Revenue", f"${total_revenue:,.2f}")
        k3.metric("Avg Fare", f"${avg_fare:,.2f}")
        k4.metric("Avg Distance", f"{avg_distance:.2f} km")
        k5.metric("Completion Rate", f"{completion_rate:.2f}%")


        st.markdown("---")

        # ----------------------------------------
        # Latest Trips
        # ----------------------------------------
        st.subheader("📋 Latest Trips (Top 10)")
        st.dataframe(df.head(10), use_container_width=True)

        # ----------------------------------------
        # City Revenue + Status Distribution
        # ----------------------------------------
        col1, col2 = st.columns(2)

        with col1:
            city_group = (
                df.groupby("city")["fare"]
                .sum()
                .reset_index()
                .sort_values("fare", ascending=False)
            )
            fig_city = px.bar(
                city_group,
                x="city",
                y="fare",
                title="💰 Revenue by City",
                labels={"fare": "Total Fare (R$)", "city": "City"}
            )
            st.plotly_chart(fig_city, use_container_width=True)

        with col2:
            status_group = df["status"].value_counts().reset_index()
            status_group.columns = ["status", "count"]
            fig_status = px.pie(
                status_group,
                values="count",
                names="status",
                title="📊 Trips by Status"
            )
            st.plotly_chart(fig_status, use_container_width=True)

        # ----------------------------------------
        # Time Series — Trips Per Minute
        # ----------------------------------------
        st.subheader("⏳ Trips Per Minute (Real-Time)")
        ts = df.set_index("start_time").resample("1min").size()
        ts_df = ts.reset_index()
        ts_df.columns = ["time", "count"]

        fig_ts = px.line(
            ts_df,
            x="time",
            y="count",
            title="Trips Over Time (1-min Window)",
            labels={"time": "Timestamp", "count": "Trips"}
        )
        st.plotly_chart(fig_ts, use_container_width=True)

        st.markdown("---")

        # -----------------------------------------------------------
        # MACHINE LEARNING INSIGHTS (Completion Probability, Anomaly Detection)
        # -----------------------------------------------------------
        st.header("🤖 Machine Learning Insights")

        if "completion_prob" not in df.columns:
            st.warning("ML model not yet producing predictions.")
        else:

            colA, colB = st.columns(2)

            # ---- Probability Distribution ----
            with colA:
                fig_prob = px.histogram(
                    df, x="completion_prob", nbins=20,
                    title="Distribution of Completion Probabilities",
                    color_discrete_sequence=["#3A86FF"]
                )
                st.plotly_chart(fig_prob, use_container_width=True)

            # ---- Fare vs Probability ----
            with colB:
                fig_scatter = px.scatter(
                    df,
                    x="fare",
                    y="completion_prob",
                    color="anomaly",
                    title="Fare vs Completion Probability (Anomalies Highlighted)",
                    color_discrete_map={True: "red", False: "green"},
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

            # ---- Show Anomalies ----
            st.markdown("### 🚨 Detected Anomalies")

            anomalies = df[df["anomaly"] == True]

            if anomalies.empty:
                st.success("No anomalies detected in current window.")
            else:
                st.error(f"{len(anomalies)} anomalies detected.")
                st.dataframe(anomalies, use_container_width=True)

        st.caption(
            f"Last updated: {datetime.now().strftime('%H:%M:%S')} • "
            f"Auto-refresh every {update_interval}s"
        )

# Auto-refresh
time.sleep(update_interval)
st.experimental_rerun()
