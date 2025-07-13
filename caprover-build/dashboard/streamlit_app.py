# streamlit_app.py
# TrendFlow Real-Time Analytics Dashboard - Final Version with Dynamic Insights

import streamlit as st
import redis
import json
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from typing import List, Dict, Any
# Import forecasting model library
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# --- Page Configuration ---
# Set page config only once at the start of the script
if 'page_config_set' not in st.session_state:
    st.set_page_config(
        page_title="TrendFlow | Real-Time Analytics",
        page_icon="💡",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    st.session_state['page_config_set'] = True

# --- Environment Variables ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# --- Caching Redis Connection ---
@st.cache_resource
def get_redis_connection() -> redis.Redis:
    """
    Establishes and caches a connection to the Redis server.
    Handles connection errors gracefully.
    """
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Failed to connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Please ensure Redis is running. Error: {e}")
        st.stop()
    except Exception as e:
        st.error(f"An unexpected error occurred during Redis connection: {e}")
        st.stop()

# --- Data Fetching and Parsing ---
@st.cache_data(ttl=10) # Cache data for 10 seconds to reduce Redis load
def fetch_data_from_redis(_redis_conn: redis.Redis, key_prefix: str) -> pd.DataFrame:
    """
    Fetches and parses JSON data from Redis for a given key prefix.
    This function is cached to prevent re-fetching data on every UI interaction.
    """
    if not _redis_conn:
        return pd.DataFrame()
        
    keys = _redis_conn.keys(f"{key_prefix}:*")
    if not keys:
        return pd.DataFrame()

    pipeline = _redis_conn.pipeline()
    for key in keys:
        pipeline.get(key)
    
    json_data = pipeline.execute()
    
    parsed_data = []
    for item in json_data:
        if item:
            try:
                parsed_data.append(json.loads(item))
            except json.JSONDecodeError:
                # Silently ignore keys with malformed JSON
                continue
    
    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)
    # Standardize and convert timestamp columns to datetime objects for proper sorting and plotting.
    for col in ['window_start', 'window_end', 'processed_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
    return df.dropna(subset=['window_start'])

# --- NEW: Dynamic Insight Generation ---
def generate_dynamic_insight(df: pd.DataFrame, metric: str, entity: str, high_is_good: bool = True):
    """
    Analyzes a dataframe to generate a concise, dynamic insight about performance.
    
    Args:
        df (pd.DataFrame): The dataframe to analyze.
        metric (str): The column name of the metric to evaluate.
        entity (str): The column name of the entity being evaluated (e.g., 'product_name').
        high_is_good (bool): Whether a higher value for the metric is better.

    Returns:
        str: A formatted string containing the insight.
    """
    if df.empty:
        return "Not enough data for an insight."
    
    if high_is_good:
        top_performer = df.loc[df[metric].idxmax()]
        worst_performer = df.loc[df[metric].idxmin()]
        insight = f"**Top Performer:** `{top_performer[entity]}` is leading with **{top_performer[metric]:,.2f}** in {metric.replace('_', ' ')}."
    else:
        top_performer = df.loc[df[metric].idxmin()]
        worst_performer = df.loc[df[metric].idxmax()]
        insight = f"**Top Performer:** `{top_performer[entity]}` is performing best with the lowest {metric} of **{top_performer[metric]:,.2f}**."
        
    return insight

# --- Main Application ---
def main():
    """Main function to structure and render the Streamlit app."""
    
    st.sidebar.title("TrendFlow Analytics 💡")
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "Select a Dashboard",
        ("🏠 Global Overview", "📈 Trending Products", "🗂️ Category Deep Dive", "👥 User Behavior & Acquisition", "🚨 Anomaly Detection", "🔮 Forecasting Lab")
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info(
        "This dashboard displays near real-time analytics of e-commerce events, enhanced with dynamic insights."
    )
    
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    redis_conn = get_redis_connection()
    if redis_conn:
        st.sidebar.success(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    if page == "🏠 Global Overview":
        render_global_overview(redis_conn)
    elif page == "📈 Trending Products":
        render_trending_products_dashboard(redis_conn)
    elif page == "🗂️ Category Deep Dive":
        render_category_dashboard(redis_conn)
    elif page == "👥 User Behavior & Acquisition":
        render_user_behavior_dashboard(redis_conn)
    elif page == "🚨 Anomaly Detection":
        render_anomaly_detection_dashboard(redis_conn)
    elif page == "🔮 Forecasting Lab":
        render_forecasting_lab(redis_conn)

# --- Dashboard Pages ---

def render_global_overview(r: redis.Redis):
    """Renders a high-level overview dashboard summarizing KPIs from all streams."""
    st.title("🏠 Global Overview")
    st.markdown("A real-time summary of the most critical business metrics from the latest time window.")

    df_products = fetch_data_from_redis(r, "trending_products")
    df_behavior = fetch_data_from_redis(r, "user_behavior")

    if df_products.empty or df_behavior.empty:
        st.warning("Data is not yet available for all streams. Please wait for the Spark job to populate the cache.")
        st.stop()

    latest_window_start = df_behavior['window_start'].max()
    df_latest_behavior = df_behavior[df_behavior['window_start'] == latest_window_start]
    df_latest_products = df_products[df_products['window_start'] == latest_window_start]

    st.subheader(f"Live Snapshot (Window starting at {latest_window_start.strftime('%H:%M:%S')})")
    total_revenue = df_latest_products['revenue'].sum()
    total_purchases = df_latest_products['purchases'].sum()
    total_clicks = df_latest_products['clicks'].sum()
    overall_cr = (total_purchases / total_clicks * 100) if total_clicks > 0 else 0
    top_channel = df_latest_behavior.loc[df_latest_behavior['revenue'].idxmax()]['source'] if not df_latest_behavior.empty else "N/A"
    top_category = df_latest_products.loc[df_latest_products['revenue'].idxmax()]['category'] if not df_latest_products.empty else "N/A"

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"€{total_revenue:,.2f}")
    col2.metric("Total Purchases", f"{int(total_purchases):,}")
    col3.metric("Overall Conversion Rate", f"{overall_cr:.2f}%")
    
    st.markdown("---")
    st.subheader("Key Performance Drivers")
    col4, col5 = st.columns(2)
    col4.info(f"🚀 **Top Channel:** The **{top_channel.title()}** channel is currently driving the most revenue.")
    col5.info(f"🏆 **Top Category:** **{top_category.title()}** is the highest-earning product category right now.")
    
    st.markdown("---")
    st.subheader("Revenue Trend Over Time")
    df_revenue_trend = df_products.groupby('window_start')['revenue'].sum().reset_index().sort_values('window_start')
    fig = px.area(df_revenue_trend, x='window_start', y='revenue', title="Total Revenue per Time Window", markers=True)
    st.plotly_chart(fig, use_container_width=True)


def render_trending_products_dashboard(r: redis.Redis):
    """Renders the dashboard for trending products with a comparison feature."""
    st.title("📈 Trending Products Dashboard")
    st.markdown("Monitor the most popular products based on user interactions in near real-time.")

    df = fetch_data_from_redis(r, "trending_products")
    if df.empty:
        st.warning("No trending product data found in Redis. Is the Spark job running?")
        st.stop()

    latest_window_start = df['window_start'].max()
    df_latest = df[df['window_start'] == latest_window_start].copy()

    st.subheader("Top 10 Trending Products")
    trend_metric = st.selectbox("Rank products by:", options=["revenue", "purchases", "total_events"], format_func=lambda x: x.replace("_", " ").title())
    df_top10 = df_latest.nlargest(10, trend_metric).sort_values(trend_metric, ascending=False)
    
    fig = px.bar(df_top10, x="product_name", y=trend_metric, title=f"Top 10 Products by {trend_metric.replace('_', ' ').title()}", labels={"product_name": "Product", trend_metric: trend_metric.replace("_", " ").title()}, color="category", text_auto=True)
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # DYNAMIC INSIGHT
    st.info(generate_dynamic_insight(df_top10, trend_metric, 'product_name'))

    st.markdown("---")

    st.subheader("Product Trend Comparison")
    st.markdown("Select two products to compare their performance trends over time.")
    all_products = sorted(df['product_name'].unique())
    col1, col2 = st.columns(2)
    product1 = col1.selectbox("Select Product 1:", all_products, index=0)
    default_index_2 = 1 if len(all_products) > 1 else 0
    product2 = col2.selectbox("Select Product 2:", all_products, index=default_index_2)

    if product1 and product2:
        df_p1 = df[df['product_name'] == product1][['window_start', 'purchases']].rename(columns={'purchases': product1})
        df_p2 = df[df['product_name'] == product2][['window_start', 'purchases']].rename(columns={'purchases': product2})
        df_compare = pd.merge(df_p1, df_p2, on='window_start', how='outer').sort_values('window_start').fillna(0)
        fig_compare = px.line(df_compare, x='window_start', y=[product1, product2], title=f"Purchase Trends: {product1} vs. {product2}", labels={"window_start": "Time", "value": "Number of Purchases"}, markers=True)
        st.plotly_chart(fig_compare, use_container_width=True)


def render_category_dashboard(r: redis.Redis):
    """Renders the dashboard for category analytics with a new treemap feature."""
    st.title("🗂️ Category Performance Deep Dive")
    st.markdown("Analyze the performance of different product categories and their contribution to total revenue.")

    df = fetch_data_from_redis(r, "category_analytics")
    df_products = fetch_data_from_redis(r, "trending_products")
    if df.empty or df_products.empty:
        st.warning("No category or product analytics data found in Redis.")
        st.stop()

    latest_window_start = df['window_start'].max()
    df_latest_cat = df[df['window_start'] == latest_window_start].copy()
    df_latest_prod = df_products[df_products['window_start'] == latest_window_start].copy()

    # --- NEW: Performance Contribution Treemap ---
    st.subheader("Revenue Contribution by Category and Product")
    fig_treemap = px.treemap(df_latest_prod, path=[px.Constant("All Revenue"), 'category', 'product_name'], values='revenue',
                  title='Hierarchical View of Revenue Contribution',
                  color='category',
                  color_discrete_sequence=px.colors.qualitative.Pastel)
    fig_treemap.update_traces(root_color="lightgrey")
    fig_treemap.update_layout(margin = dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig_treemap, use_container_width=True)
    st.info("This treemap shows how total revenue is divided among categories (larger blocks) and individual products (smaller blocks). Hover to see details.")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Revenue Distribution by Category")
        fig_pie = px.pie(df_latest_cat, names='category', values='revenue', title='Revenue Share per Category', hole=0.3)
        st.plotly_chart(fig_pie, use_container_width=True)
    with col2:
        st.subheader("Conversion Rate Comparison")
        df_sorted_conv = df_latest_cat.sort_values('conversion_rate', ascending=False)
        fig_bar = px.bar(df_sorted_conv, x='category', y='conversion_rate', title='Conversion Rate (%) by Category', labels={'conversion_rate': 'Conversion Rate (%)'}, color='category')
        st.plotly_chart(fig_bar, use_container_width=True)


def render_user_behavior_dashboard(r: redis.Redis):
    """Renders the dashboard for user behavior with channel trend analysis."""
    st.title("👥 User Behavior & Acquisition")
    st.markdown("Understand where your users are coming from and how their performance is trending.")

    df = fetch_data_from_redis(r, "user_behavior")
    if df.empty:
        st.warning("No user behavior data found in Redis.")
        st.stop()

    latest_window_start = df['window_start'].max()
    df_latest = df[df['window_start'] == latest_window_start].copy()

    st.subheader("Acquisition Channel Conversion Funnel (Latest Window)")
    df_funnel = df_latest.groupby('source').agg(clicks=('clicks', 'sum'), purchases=('purchases', 'sum')).reset_index()
    df_funnel['conversion_rate'] = (df_funnel['purchases'] / df_funnel['clicks'].replace(0, 1)) * 100
    fig_funnel = px.funnel(df_funnel, x=['clicks', 'purchases'], y='source', title='Conversion Funnel from Clicks to Purchases by Source')
    st.plotly_chart(fig_funnel, use_container_width=True)
    
    # DYNAMIC INSIGHT
    st.info(generate_dynamic_insight(df_funnel, 'conversion_rate', 'source'))
    
    st.markdown("---")

    st.subheader("Acquisition Channel Trend Analysis")
    st.markdown("Analyze the revenue trend for each acquisition source to identify growth or fatigue.")
    df_channel_trend = df.groupby(['window_start', 'source'])['revenue'].sum().unstack().fillna(0)
    fig_trend = px.line(df_channel_trend, x=df_channel_trend.index, y=df_channel_trend.columns, title="Revenue Trend by Acquisition Channel", labels={"window_start": "Time", "value": "Revenue (€)", "source": "Channel"}, markers=True)
    st.plotly_chart(fig_trend, use_container_width=True)


def render_anomaly_detection_dashboard(r: redis.Redis):
    """Renders a dashboard for automatically detecting anomalies in key metrics."""
    st.title("🚨 Real-Time Anomaly Detection")
    st.markdown("Automatically monitor key metrics for statistically significant deviations from the norm. Red markers indicate potential issues or opportunities.")

    df_products = fetch_data_from_redis(r, "trending_products")
    if df_products.empty:
        st.warning("No product data available for anomaly detection.")
        st.stop()

    df_ts = df_products.groupby('window_start')['revenue'].sum().reset_index().sort_values('window_start')
    if len(df_ts) < 5:
        st.info("Waiting for more data to build a reliable trend for anomaly detection.")
        st.stop()

    window_size = st.slider("Anomaly Detection Window Size (periods)", 5, 50, 10)
    num_std_dev = st.slider("Standard Deviation Threshold", 1.0, 3.0, 2.0, 0.5)

    df_ts['rolling_mean'] = df_ts['revenue'].rolling(window=window_size).mean()
    df_ts['rolling_std'] = df_ts['revenue'].rolling(window=window_size).std()
    df_ts['upper_band'] = df_ts['rolling_mean'] + (df_ts['rolling_std'] * num_std_dev)
    df_ts['lower_band'] = df_ts['rolling_mean'] - (df_ts['rolling_std'] * num_std_dev)
    df_ts['anomaly'] = (df_ts['revenue'] > df_ts['upper_band']) | (df_ts['revenue'] < df_ts['lower_band'])
    df_anomalies = df_ts[df_ts['anomaly']]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_ts['window_start'], y=df_ts['upper_band'], mode='lines', line=dict(dash='dash', color='gray'), name='Upper Band'))
    fig.add_trace(go.Scatter(x=df_ts['window_start'], y=df_ts['lower_band'], mode='lines', line=dict(dash='dash', color='gray'), name='Lower Band', fill='tonexty', fillcolor='rgba(128,128,128,0.1)'))
    fig.add_trace(go.Scatter(x=df_ts['window_start'], y=df_ts['revenue'], mode='lines+markers', name='Total Revenue'))
    if not df_anomalies.empty:
        fig.add_trace(go.Scatter(x=df_anomalies['window_start'], y=df_anomalies['revenue'], mode='markers', name='Anomaly', marker=dict(color='red', size=12, symbol='x')))
    
    fig.update_layout(title="Total Revenue with Anomaly Detection", xaxis_title="Time Window", yaxis_title="Revenue (€)", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig, use_container_width=True)

    if not df_anomalies.empty:
        st.subheader("Detected Anomalies")
        st.dataframe(df_anomalies[['window_start', 'revenue', 'rolling_mean']].rename(columns={'revenue': 'Anomalous Revenue', 'rolling_mean': 'Expected Revenue (Approx.)'}), use_container_width=True)
        # DYNAMIC INSIGHT
        latest_anomaly = df_anomalies.iloc[-1]
        st.error(f"**Latest Anomaly Alert:** Revenue of **€{latest_anomaly['revenue']:,.2f}** was detected at **{latest_anomaly['window_start'].strftime('%H:%M:%S')}**, which is outside the expected range.")


def render_forecasting_lab(r: redis.Redis):
    """Renders a predictive dashboard to forecast future revenue trends."""
    st.title("🔮 Forecasting Lab")
    st.markdown("Use historical data to predict future revenue. Adjust the forecast horizon to see short-term or long-term predictions.")

    df_products = fetch_data_from_redis(r, "trending_products")
    if df_products.empty:
        st.warning("No product data available for forecasting.")
        st.stop()

    df_ts = df_products.groupby('window_start')['revenue'].sum().reset_index()
    df_ts = df_ts.set_index('window_start').asfreq('min').fillna(0)

    if len(df_ts) < 20:
        st.info("More historical data is needed for an accurate forecast (at least 20 data points recommended).")
        st.stop()

    forecast_periods = st.slider("Forecast Horizon (in minutes)", min_value=10, max_value=120, value=30, step=10)
    
    model = ExponentialSmoothing(df_ts['revenue'], trend='add', seasonal='add', seasonal_periods=12).fit()
    forecast = model.forecast(steps=forecast_periods)
    
    forecast_df = pd.DataFrame({'timestamp': forecast.index, 'predicted_revenue': forecast.values})

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_ts.index, y=df_ts['revenue'], mode='lines', name='Historical Revenue'))
    fig.add_trace(go.Scatter(x=forecast_df['timestamp'], y=forecast_df['predicted_revenue'], mode='lines', name='Forecasted Revenue', line=dict(dash='dash')))
    
    fig.update_layout(title=f"Revenue Forecast for the Next {forecast_periods} Minutes", xaxis_title="Time", yaxis_title="Revenue (€)", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Forecasted Data Points")
    st.dataframe(forecast_df.set_index('timestamp'), use_container_width=True)
    # DYNAMIC INSIGHT
    total_forecast_revenue = forecast_df['predicted_revenue'].sum()
    st.success(f"**Forecast Summary:** The model predicts a total revenue of **€{total_forecast_revenue:,.2f}** over the next {forecast_periods} minutes.")


if __name__ == "__main__":
    main()
