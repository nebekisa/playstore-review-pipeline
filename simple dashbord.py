# simple_dashboard.py
import streamlit as st
import sqlite3
import pandas as pd

# --- 1. Configure the Dashboard ---
st.set_page_config(page_title="E-commerce Reviews - Simple Dashboard", layout="centered")

# --- 2. Load Data from SQLite ---
DB_PATH = 'reviews.db' # Path to your SQLite database file

@st.cache_data
def load_data():
    """Loads data from the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Load Processed Reviews
        query_processed = "SELECT * FROM ProcessedAppReviews"
        df_processed = pd.read_sql_query(query_processed, conn)
        conn.close()
        return df_processed
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

# --- 3. Dashboard Title ---
st.title("🛍️ Simple E-commerce App Review Dashboard")

# --- 4. Load Data ---
with st.spinner("Loading data..."):
    df = load_data()

if df.empty:
    st.warning("No data available to display.")
else:
    # --- 5. Display a Simple Metric ---
    st.header("Overview")
    total_reviews = len(df)
    st.metric(label="Total Processed Reviews", value=total_reviews)

    # --- 6. Display a Simple Chart ---
    st.header("Sentiment Distribution")
    sentiment_counts = df['VADER_Sentiment'].value_counts()
    st.bar_chart(sentiment_counts)

    # --- 7. Display Sample Data ---
    st.header("Sample Processed Reviews")
    st.dataframe(df[['AppName', 'Rating', 'ReviewText', 'CleanedReviewText', 'VADER_Sentiment']].head(10))

# --- 8. Footer ---
st.markdown("---")
st.caption("Simple Dashboard built with Streamlit.")