# standalone_dashboard/app.py (Enhanced & Corrected)
import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np
import logging

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Path to your SQLite database file on your Windows machine
# Update this path to match the location of your actual reviews.db file
DB_PATH = r"C:\Users\hp\Desktop\playstore_pipeline_final\sqlite_data\reviews.db" # Raw string to handle backslashes

# --- Helper Functions ---
@st.cache_resource
def get_db_connection(db_path):
    """Establishes and returns a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to SQLite database at {db_path}")
        return conn
    except Exception as e:
        st.error(f"Error connecting to database at {db_path}: {e}")
        logger.error(f"Error connecting to database at {db_path}: {e}")
        return None

@st.cache_data
def load_data(db_path):
    """Loads data from both RawAppReviews and ProcessedAppReviews tables."""
    logger.info("Loading data from database...")
    conn = get_db_connection(db_path)
    if not conn:
        return pd.DataFrame(), pd.DataFrame()

    try:
        # Load Raw Reviews
        query_raw = "SELECT * FROM RawAppReviews"
        df_raw = pd.read_sql_query(query_raw, conn)
        logger.info(f"Loaded {len(df_raw)} raw reviews.")

        # Load Processed Reviews
        query_processed = "SELECT * FROM ProcessedAppReviews"
        df_processed = pd.read_sql_query(query_processed, conn)
        logger.info(f"Loaded {len(df_processed)} processed reviews.")

        # --- CRUCIAL FIX: Ensure VADER_Score is Numeric ---
        # Convert VADER_Score to numeric, coercing errors to NaN
        df_processed['VADER_Score'] = pd.to_numeric(df_processed['VADER_Score'], errors='coerce')
        logger.info("Converted VADER_Score to numeric.")

        # Ensure ReviewDate is datetime
        df_raw['ReviewDate'] = pd.to_datetime(df_raw['ReviewDate'], errors='coerce')
        df_processed['ReviewDate'] = pd.to_datetime(df_processed['ReviewDate'], errors='coerce')
        logger.info("Converted ReviewDate to datetime.")

        # Derive YearMonth for grouping (handle potential NaT)
        df_raw['YearMonth'] = df_raw['ReviewDate'].dt.to_period('M').dt.start_time
        df_processed['YearMonth'] = df_processed['ReviewDate'].dt.to_period('M').dt.start_time
        logger.info("Derived YearMonth.")

        return df_raw, df_processed
    except Exception as e:
        st.error(f"Error loading data: {e}")
        logger.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

def get_top_ngrams(text_series, ngram_range=(1, 1), top_n=20):
    """Calculates top N-grams from a Pandas Series of text."""
    if text_series.empty:
        return []

    # Combine all text
    all_text = " ".join(text_series.dropna().astype(str).tolist()).lower()

    if not all_text.strip():
        return []

    # Tokenize
    try:
        tokens = word_tokenize(all_text)
    except LookupError:
        nltk.download('punkt')
        nltk.download('punkt_tab')
        tokens = word_tokenize(all_text)

    # Remove stopwords
    try:
        stop_words = set(stopwords.words('english'))
    except LookupError:
        nltk.download('stopwords')
        stop_words = set(stopwords.words('english'))

    filtered_tokens = [word for word in tokens if word.isalpha() and word not in stop_words]

    # Generate N-grams
    if ngram_range == (1, 1):
        ngrams_list = filtered_tokens
    elif ngram_range == (2, 2):
        ngrams_list = [' '.join(filtered_tokens[i:i+2]) for i in range(len(filtered_tokens)-1)]
    else:
        ngrams_list = filtered_tokens # Fallback

    # Count and get top N
    ngram_counts = Counter(ngrams_list)
    top_ngrams = ngram_counts.most_common(top_n)
    return top_ngrams

def create_wordcloud_figure(text_series):
    """Creates a matplotlib figure for a word cloud."""
    if text_series.empty:
        return None

    all_text = " ".join(text_series.dropna().astype(str).tolist())
    if not all_text.strip():
        return None

    try:
        wordcloud = WordCloud(width=800, height=400, background_color='white', max_words=100, colormap='viridis').generate(all_text)
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis("off")
        plt.tight_layout(pad=0) # Reduce padding
        return fig
    except Exception as e:
         st.warning(f"Could not generate word cloud: {e}")
         logger.warning(f"Could not generate word cloud: {e}")
         return None

# --- Main Dashboard App ---
def main():
    """Main function to run the Streamlit dashboard."""
    # --- 1. Configure Page ---
    st.set_page_config(
        page_title="E-commerce App Review Insights (Standalone)",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # --- 2. App Title & Description ---
    st.title("🛍️ E-commerce App Review Insights Dashboard (Standalone)")
    st.markdown("""
    This standalone dashboard provides interactive visualizations and insights derived from Google Play Store reviews
    for popular e-commerce applications: AliExpress, Alibaba, and Jiji.
    It reads data directly from the local SQLite database (`reviews.db`).
    Select an app from the sidebar to explore its specific review data.
    """)

    # --- 3. Check Database Connection ---
    if not os.path.exists(DB_PATH):
        st.error(f"Database file not found at {DB_PATH}. Please ensure the path is correct and the scraper/processor has run successfully.")
        st.stop()

    # --- 4. Load Data ---
    with st.spinner("Loading data from SQLite database..."):
        df_raw, df_processed = load_data(DB_PATH)

    if df_raw.empty and df_processed.empty:
        st.error("Failed to load any data from the database. Please check the database connection and table contents.")
        st.stop()
    elif df_raw.empty:
        st.warning("No raw review data found. Showing processed data only.")
    elif df_processed.empty:
        st.warning("No processed review data found. Showing raw data only.")

    # --- 5. Sidebar for App Selection ---
    st.sidebar.header("Navigation & Filters")
    # Get unique app names from either df_raw or df_processed
    app_names = sorted(list(set(df_raw['AppName'].unique().tolist() + df_processed['AppName'].unique().tolist())))
    if not app_names:
        st.error("No app names found in the data.")
        st.stop()

    selected_app = st.sidebar.selectbox(
        "Select an E-commerce App:",
        options=app_names,
        index=0 # Default to first app
    )
    st.sidebar.markdown("---")
    st.sidebar.info(f"Selected App: **{selected_app}**")

    # --- 6. Filter Data ---
    df_raw_app = df_raw[df_raw['AppName'] == selected_app].copy() if not df_raw.empty else pd.DataFrame()
    df_processed_app = df_processed[df_processed['AppName'] == selected_app].copy() if not df_processed.empty else pd.DataFrame()

    if df_raw_app.empty and df_processed_app.empty:
        st.warning(f"No data found for app '{selected_app}'.")
        st.stop()

    # --- 7. Main Dashboard Content ---
    # --- Section 1: Overview Statistics ---
    st.header(f"📊 Overview for {selected_app}", divider='rainbow')
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_reviews = len(df_raw_app)
        st.metric(label="Total Reviews", value=total_reviews if total_reviews > 0 else "N/A")
    with col2:
        if not df_raw_app.empty and 'Rating' in df_raw_app.columns:
            avg_rating = df_raw_app['Rating'].mean()
            st.metric(label="Average Rating ⭐", value=f"{avg_rating:.2f}" if pd.notnull(avg_rating) else "N/A")
        else:
            st.metric(label="Average Rating ⭐", value="N/A")
    with col3:
        if not df_processed_app.empty and 'VADER_Sentiment' in df_processed_app.columns:
            sentiment_counts = df_processed_app['VADER_Sentiment'].value_counts()
            if 'Positive' in sentiment_counts.index:
                pos_pct = (sentiment_counts.get('Positive', 0) / len(df_processed_app)) * 100
                st.metric(label="Positive Sentiment (%)", value=f"{pos_pct:.1f}%" if pd.notnull(pos_pct) else "N/A")
            else:
                st.metric(label="Positive Sentiment (%)", value="0.0%")
        else:
            st.metric(label="Positive Sentiment (%)", value="N/A")
    with col4:
         # Example: Dominant Sentiment
         if not df_processed_app.empty and 'VADER_Sentiment' in df_processed_app.columns:
             sentiment_counts_overview = df_processed_app['VADER_Sentiment'].value_counts()
             dominant_sentiment = sentiment_counts_overview.idxmax() if not sentiment_counts_overview.empty else "N/A"
             st.metric(label="Dominant Sentiment", value=dominant_sentiment)
         else:
             st.metric(label="Dominant Sentiment", value="N/A")


    # --- Section 2: Visualizations ---
    # --- 2.1 Rating Distribution ---
    if not df_raw_app.empty and 'Rating' in df_raw_app.columns:
        st.subheader("⭐ Rating Distribution", divider='gray')
        rating_counts = df_raw_app['Rating'].value_counts().sort_index()
        if not rating_counts.empty:
            fig_rating = px.bar(x=rating_counts.index, y=rating_counts.values,
                                labels={'x':'Rating (Stars)', 'y':'Number of Reviews'},
                                title=f"Rating Distribution for {selected_app}",
                                color=rating_counts.index,
                                color_continuous_scale='Bluered_r')
            fig_rating.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_rating, use_container_width=True, theme="streamlit")
        else:
            st.warning("No rating data available to plot.")

    # --- 2.2 Sentiment Distribution ---
    if not df_processed_app.empty and 'VADER_Sentiment' in df_processed_app.columns:
        st.subheader("😊 Sentiment Distribution (VADER)", divider='gray')
        sentiment_counts_viz = df_processed_app['VADER_Sentiment'].value_counts()
        if not sentiment_counts_viz.empty:
            # Ensure order
            sentiment_counts_viz = sentiment_counts_viz.reindex(['Negative', 'Neutral', 'Positive'], fill_value=0)
            fig_sentiment = px.pie(names=sentiment_counts_viz.index, values=sentiment_counts_viz.values,
                                   title=f"Sentiment Distribution for {selected_app}",
                                   color_discrete_sequence=px.colors.qualitative.Set1)
            fig_sentiment.update_layout(height=400)
            st.plotly_chart(fig_sentiment, use_container_width=True, theme="streamlit")
        else:
            st.warning("No sentiment data available to plot.")

    # --- 2.3 Sentiment Trend Over Time (Monthly) ---
    if not df_processed_app.empty and 'YearMonth' in df_processed_app.columns and 'VADER_Sentiment' in df_processed_app.columns:
        st.subheader("📈 Sentiment Trend Over Time (Monthly)", divider='gray')
        # Remove rows with NaT in YearMonth
        df_processed_app_trend = df_processed_app.dropna(subset=['YearMonth'])
        if not df_processed_app_trend.empty:
            # Group by YearMonth and Sentiment
            sentiment_trend = df_processed_app_trend.groupby(['YearMonth', 'VADER_Sentiment']).size().reset_index(name='Count')
            sentiment_trend = sentiment_trend.sort_values('YearMonth')

            if not sentiment_trend.empty:
                fig_sentiment_trend = px.line(sentiment_trend, x='YearMonth', y='Count', color='VADER_Sentiment',
                                              title=f"Monthly Sentiment Trend for {selected_app}",
                                              labels={'YearMonth':'Date', 'Count':'Number of Reviews'},
                                              markers=True,
                                              color_discrete_map={'Negative': 'red', 'Neutral': 'orange', 'Positive': 'green'})
                fig_sentiment_trend.update_xaxes(title_text='Date')
                fig_sentiment_trend.update_yaxes(title_text='Number of Reviews')
                fig_sentiment_trend.update_layout(hovermode='x unified', height=500)
                st.plotly_chart(fig_sentiment_trend, use_container_width=True, theme="streamlit")
            else:
                st.warning("No data available to plot sentiment trend.")
        else:
            st.warning("No valid date data available for sentiment trend.")

    # --- 2.4 Key Topics/Words Visualization (Tabs) ---
    if not df_processed_app.empty and 'CleanedReviewText' in df_processed_app.columns:
        st.subheader("🔍 Key Topics & Words", divider='gray')
        tab1, tab2, tab3 = st.tabs(["Top Unigrams", "Top Bigrams", "Word Cloud"])

        with tab1:
            st.write("**Most Frequent Single Words (Unigrams)**")
            top_unigrams = get_top_ngrams(df_processed_app['CleanedReviewText'], ngram_range=(1, 1), top_n=20)
            if top_unigrams:
                unigram_df = pd.DataFrame(top_unigrams, columns=['Word', 'Count'])
                fig_words_uni = px.bar(unigram_df, x='Count', y='Word', orientation='h',
                                       title="", labels={'Count':'Frequency', 'Word':'Word'},
                                       color='Count', color_continuous_scale='Viridis')
                fig_words_uni.update_layout(height=500)
                st.plotly_chart(fig_words_uni, use_container_width=True, theme="streamlit")
            else:
                st.warning("No text data available for unigram analysis.")

        with tab2:
            st.write("**Most Frequent Two-Word Phrases (Bigrams)**")
            top_bigrams = get_top_ngrams(df_processed_app['CleanedReviewText'], ngram_range=(2, 2), top_n=20)
            if top_bigrams:
                bigram_df = pd.DataFrame(top_bigrams, columns=['Phrase', 'Count'])
                fig_words_bi = px.bar(bigram_df, x='Count', y='Phrase', orientation='h',
                                      title="", labels={'Count':'Frequency', 'Phrase':'Phrase'},
                                      color='Count', color_continuous_scale='Plasma')
                fig_words_bi.update_layout(height=500)
                st.plotly_chart(fig_words_bi, use_container_width=True, theme="streamlit")
            else:
                st.warning("No text data available for bigram analysis.")

        with tab3:
            st.write("**Word Cloud Visualization**")
            fig_wc = create_wordcloud_figure(df_processed_app['CleanedReviewText'])
            if fig_wc:
                st.pyplot(fig_wc)
            else:
                st.warning("No text data available for word cloud generation.")

    # --- 2.5 Example Reviews (Tabs) ---
    if not df_raw_app.empty:
        st.subheader("📝 Example Reviews by Sentiment", divider='gray')
        # Ensure VADER_Score is numeric for sorting examples
        if not df_processed_app.empty:
             df_processed_app['VADER_Score'] = pd.to_numeric(df_processed_app['VADER_Score'], errors='coerce')
             # Merge raw and processed to get sentiment for examples
             try:
                 df_examples = df_raw_app.merge(df_processed_app[['PlayStoreReviewID', 'VADER_Sentiment', 'VADER_Score']],
                                                on='PlayStoreReviewID', how='left', suffixes=('', '_processed'))
                 logger.info("Merged raw and processed data for examples.")
             except Exception as e:
                 st.warning("Could not merge raw and processed data for examples. Showing raw data only.")
                 logger.warning(f"Merge error: {e}")
                 df_examples = df_raw_app.copy()
                 df_examples['VADER_Sentiment'] = 'Unknown'
                 df_examples['VADER_Score'] = np.nan
        else:
            df_examples = df_raw_app.copy()
            df_examples['VADER_Sentiment'] = 'Unknown'
            df_examples['VADER_Score'] = np.nan

        # Ensure VADER_Score is numeric for sorting
        df_examples['VADER_Score'] = pd.to_numeric(df_examples['VADER_Score'], errors='coerce')

        # Tabs for Positive, Negative, Neutral examples
        tab_pos, tab_neg, tab_neu = st.tabs(["😊 Positive", "😞 Negative", "😐 Neutral"])

        with tab_pos:
            st.write("**Sample Positive Reviews**")
            pos_reviews = df_examples[df_examples['VADER_Sentiment'] == 'Positive']
            if not pos_reviews.empty:
                # Ensure VADER_Score is numeric and drop NaNs for sorting
                pos_reviews_valid = pos_reviews.dropna(subset=['VADER_Score'])
                if not pos_reviews_valid.empty:
                    examples = pos_reviews_valid.nlargest(5, 'VADER_Score')
                    for idx, row in examples.iterrows():
                        with st.expander(f"Rating: {row.get('Rating', 'N/A')} ⭐ | VADER Score: {row.get('VADER_Score', 'N/A'):.3f} | Date: {row.get('ReviewDate', pd.NaT).strftime('%Y-%m-%d') if pd.notnull(row.get('ReviewDate', pd.NaT)) else 'N/A'}", expanded=False):
                            st.write(f"**Review Text:** {row.get('ReviewText', 'N/A')}")
                else:
                    st.write("No valid positive reviews with scores found.")
            else:
                st.write("No positive reviews found.")

        with tab_neg:
            st.write("**Sample Negative Reviews**")
            neg_reviews = df_examples[df_examples['VADER_Sentiment'] == 'Negative']
            if not neg_reviews.empty:
                # Ensure VADER_Score is numeric and drop NaNs for sorting
                neg_reviews_valid = neg_reviews.dropna(subset=['VADER_Score'])
                if not neg_reviews_valid.empty:
                    examples = neg_reviews_valid.nsmallest(5, 'VADER_Score')
                    for idx, row in examples.iterrows():
                        with st.expander(f"Rating: {row.get('Rating', 'N/A')} ⭐ | VADER Score: {row.get('VADER_Score', 'N/A'):.3f} | Date: {row.get('ReviewDate', pd.NaT).strftime('%Y-%m-%d') if pd.notnull(row.get('ReviewDate', pd.NaT)) else 'N/A'}", expanded=False):
                            st.write(f"**Review Text:** {row.get('ReviewText', 'N/A')}")
                else:
                     st.write("No valid negative reviews with scores found.")
            else:
                st.write("No negative reviews found.")

        with tab_neu:
            st.write("**Sample Neutral Reviews**")
            neu_reviews = df_examples[df_examples['VADER_Sentiment'] == 'Neutral']
            if not neu_reviews.empty:
                # For neutral, sample a few or take ones with scores closest to 0
                neu_reviews_valid = neu_reviews.dropna(subset=['VADER_Score'])
                if not neu_reviews_valid.empty:
                    # Sample 5 or less
                    examples = neu_reviews_valid.sample(n=min(5, len(neu_reviews_valid)), random_state=42)
                    for idx, row in examples.iterrows():
                        with st.expander(f"Rating: {row.get('Rating', 'N/A')} ⭐ | VADER Score: {row.get('VADER_Score', 'N/A'):.3f} | Date: {row.get('ReviewDate', pd.NaT).strftime('%Y-%m-%d') if pd.notnull(row.get('ReviewDate', pd.NaT)) else 'N/A'}", expanded=False):
                            st.write(f"**Review Text:** {row.get('ReviewText', 'N/A')}")
                else:
                    # If no valid scores, just sample from all neutral
                    examples = neu_reviews.sample(n=min(5, len(neu_reviews)), random_state=42)
                    for idx, row in examples.iterrows():
                         with st.expander(f"Rating: {row.get('Rating', 'N/A')} ⭐ | Date: {row.get('ReviewDate', pd.NaT).strftime('%Y-%m-%d') if pd.notnull(row.get('ReviewDate', pd.NaT)) else 'N/A'}", expanded=False):
                             st.write(f"**Review Text:** {row.get('ReviewText', 'N/A')}")
            else:
                st.write("No neutral reviews found.")

    # --- 8. Footer ---
    st.markdown("---")
    current_year = datetime.now().year
    st.caption(f"Standalone Dashboard built with Streamlit. Data sourced from Google Play Store reviews. © {current_year}")

# --- Run the App ---
if __name__ == "__main__":
    main()
