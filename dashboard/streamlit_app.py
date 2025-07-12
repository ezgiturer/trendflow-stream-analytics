import streamlit as st
import redis
import json
import os
import pandas as pd
from datetime import datetime

# --- Streamlit Page Configuration
if 'page_config_set' not in st.session_state:
    st.set_page_config(layout="wide", page_title="🛍️ TrendFlow E-Commerce Analytics")
    st.session_state['page_config_set'] = True

st.title("🛍️ TrendFlow Real-Time E-Commerce Analytics")
st.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Auto-refresh mechanism
#refresh_interval_secs = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
#st.sidebar.markdown(
#    f"<meta http-equiv='refresh' content='{refresh_interval_secs}'>",
#    unsafe_allow_html=True,
#)

# Environment variables (matching docker-compose.yml)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# --- Redis Connection ---
@st.cache_resource
def get_redis_connection():
    """Establishes and caches a Redis connection."""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        r.ping()
        st.success(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Could not connect to Redis: {e}")
        st.stop()
        return None

r = get_redis_connection()

# --- Data Fetching Functions ---
def fetch_data_from_redis(key_prefix: str, limit: int = 100):
    """Fetches data from Redis based on a key prefix."""
    if not r:
        return pd.DataFrame()

    keys = r.scan_iter(f"{key_prefix}:*")
    data_list = []
    for key in keys:
        try:
            value = r.get(key)
            if value:
                data_list.append(json.loads(value))
        except json.JSONDecodeError as e:
            st.warning(f"Failed to decode JSON from Redis key {key}: {e}")
        except Exception as e:
            st.error(f"Error fetching key {key}: {e}")

    df = pd.DataFrame(data_list)

    if not df.empty:
        if 'window_start' in df.columns:
            df['window_start'] = pd.to_datetime(df['window_start'])
        if 'window_end' in df.columns:
            df['window_end'] = pd.to_datetime(df['window_end'])
        if 'processed_at' in df.columns:
            df['processed_at'] = pd.to_datetime(df['processed_at'])

        if key_prefix == "trending_products" and 'revenue' in df.columns:
            df = df.sort_values(by=['window_end', 'revenue'], ascending=[False, False])
        elif key_prefix == "category_analytics" and 'revenue' in df.columns:
            df = df.sort_values(by=['window_end', 'revenue'], ascending=[False, False])
        elif key_prefix == "user_behavior" and 'total_events' in df.columns:
            df = df.sort_values(by=['window_end', 'total_events'], ascending=[False, False])

        if 'window_end' in df.columns:
             df = df.sort_values(by='window_end', ascending=False)
             latest_window_end = df['window_end'].max()
             df = df[df['window_end'] == latest_window_end]

    return df.head(limit)


# --- Streamlit Tabs ---
tab1, tab2, tab3 = st.tabs(["📊 Trending Products", "📈 Category Analytics", "👤 User Behavior"])

with tab1:
    st.header("Top Trending Products")
    trending_products_df = fetch_data_from_redis("trending_products", limit=10)
    if not trending_products_df.empty:
        st.dataframe(trending_products_df, use_container_width=True)
    else:
        st.info("No trending products data available yet. Ensure Spark job and Kafka producer are running.")

with tab2:
    st.header("Category Performance")
    category_analytics_df = fetch_data_from_redis("category_analytics", limit=10)
    if not category_analytics_df.empty:
        st.dataframe(category_analytics_df, use_container_width=True)
    else:
        st.info("No category analytics data available yet.")

with tab3:
    st.header("User Behavior Insights")
    user_behavior_df = fetch_data_from_redis("user_behavior", limit=10)
    if not user_behavior_df.empty:
        st.dataframe(user_behavior_df, use_container_width=True)
    else:
        st.info("No user behavior data available yet.")