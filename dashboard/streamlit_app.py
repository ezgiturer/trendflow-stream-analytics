# dashboard/streamlit_app.py

import streamlit as st
import redis
import json
import pandas as pd
import os
import time
from datetime import datetime

# Configure logging (optional for Streamlit, but good for debugging)
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Redis Connection Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost") # Will be "redis" in Docker Compose
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None) # None for no password

# Initialize Redis client (using st.cache_resource for efficiency)
@st.cache_resource
def get_redis_client():
    logging.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        r.ping() # Test connection
        logging.info("Successfully connected to Redis!")
        return r
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Could not connect to Redis: {e}")
        st.error(f"Could not connect to Redis: {e}. Make sure Redis is running.")
        st.stop()
        return None

r = get_redis_client()

# --- Functions to Fetch Data from Redis ---

def fetch_latest_trending_products(num_products=10):
    # Fetch all keys that start with "trending_products:"
    keys = r.keys("trending_products:*")
    
    data = []
    for key in keys:
        try:
            item = json.loads(r.get(key))
            data.append(item)
        except (json.JSONDecodeError, TypeError) as e:
            logging.warning(f"Could not decode JSON from Redis key {key}: {e}")
            continue

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df['window_end'] = pd.to_datetime(df['window_end'])
    # Sort by window_end and then by total_events to get the latest trending products
    df = df.sort_values(by=['window_end', 'total_events'], ascending=[False, False]).drop_duplicates(subset=['product_id'], keep='first')
    return df.head(num_products)

def fetch_latest_category_analytics():
    keys = r.keys("category_analytics:*")
    data = []
    for key in keys:
        try:
            item = json.loads(r.get(key))
            data.append(item)
        except (json.JSONDecodeError, TypeError) as e:
            logging.warning(f"Could not decode JSON from Redis key {key}: {e}")
            continue

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df['window_end'] = pd.to_datetime(df['window_end'])
    df = df.sort_values(by='window_end', ascending=False).drop_duplicates(subset=['category'], keep='first')
    return df

def fetch_latest_user_behavior_analytics():
    keys = r.keys("user_behavior:*")
    data = []
    for key in keys:
        try:
            item = json.loads(r.get(key))
            data.append(item)
        except (json.JSONDecodeError, TypeError) as e:
            logging.warning(f"Could not decode JSON from Redis key {key}: {e}")
            continue

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df['window_end'] = pd.to_datetime(df['window_end'])
    df = df.sort_values(by='window_end', ascending=False).drop_duplicates(subset=['user_agent', 'source'], keep='first')
    return df


# --- Streamlit Dashboard Layout ---

st.set_page_config(layout="wide", page_title="TrendFlow E-Commerce Analytics")

st.title("📈 TrendFlow Real-Time E-Commerce Analytics")
st.markdown("Monitor live click and purchase events, product trends, and user behavior.")

# Auto-refresh mechanism
placeholder = st.empty()

with placeholder.container():
    col1, col2 = st.columns(2)

    with col1:
        st.header("🔥 Top 10 Trending Products (Latest Window)")
        trending_products_df = fetch_latest_trending_products()
        if not trending_products_df.empty:
            st.dataframe(trending_products_df.set_index('product_name')[
                ['category', 'total_events', 'clicks', 'purchases', 'revenue', 'conversion_rate']
            ].style.format({
                'revenue': "$ {:,.2f}",
                'conversion_rate': "{:.2f}%"
            }), use_container_width=True)
        else:
            st.info("No trending product data yet. Start Kafka Producer and Spark Processor.")

    with col2:
        st.header("📊 Category Performance (Latest Window)")
        category_analytics_df = fetch_latest_category_analytics()
        if not category_analytics_df.empty:
            st.dataframe(category_analytics_df.set_index('category')[
                ['total_events', 'clicks', 'purchases', 'revenue', 'conversion_rate', 'avg_product_price']
            ].style.format({
                'revenue': "$ {:,.2f}",
                'avg_product_price': "$ {:,.2f}",
                'conversion_rate': "{:.2f}%"
            }), use_container_width=True)
        else:
            st.info("No category analytics data yet.")

    st.markdown("---") # Separator

    st.header("👥 User Behavior Analytics (Latest Window)")
    user_behavior_df = fetch_latest_user_behavior_analytics()
    if not user_behavior_df.empty:
        st.dataframe(user_behavior_df.set_index(['user_agent', 'source'])[
            ['total_events', 'clicks', 'purchases', 'revenue', 'unique_users_affected', 'conversion_rate']
        ].style.format({
            'revenue': "$ {:,.2f}",
            'conversion_rate': "{:.2f}%"
        }), use_container_width=True)
    else:
        st.info("No user behavior data yet.")

    # Display update timestamp
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Add a small delay and rerun the app to simulate live updates
# Streamlit's default is to rerun on code changes or widget interactions.
# This forces a refresh every few seconds.
time.sleep(5)
st.experimental_rerun()