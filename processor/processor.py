# processor/processor.py
import sqlite3
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup
import re
import contractions
import logging
import os
from datetime import datetime

# --- Configuration ---
DB_PATH = os.getenv('DB_PATH', '../reviews.db')  # Use env var or default relative path

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Download NLTK Data (Run once, or ensure it's available in Docker image) ---
# Uncomment the following lines if running locally for the first time
# nltk.download('punkt')
# nltk.download('punkt_tab')  # Might be needed depending on NLTK version
# nltk.download('stopwords')
# nltk.download('wordnet')
# nltk.download('omw-1.4')


def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        logger.info(f"Connected to SQLite database at {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def create_processed_table_if_not_exists(conn):
    """Creates the ProcessedAppReviews table if it doesn't already exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ProcessedAppReviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        RawReviewID INTEGER, -- Foreign Key reference (conceptual)
        AppName TEXT NOT NULL,
        PlayStoreReviewID TEXT, -- Link back to raw data
        Rating INTEGER NOT NULL,
        ReviewDate DATE,
        ReviewText TEXT, -- Original text
        CleanedReviewText TEXT, -- The cleaned text
        ReviewWordCount INTEGER, -- Calculated word count
        VADER_Score REAL, -- Sentiment score (placeholder, filled later or here)
        VADER_Sentiment TEXT, -- Sentiment label (placeholder)
        YearMonth DATE, -- Derived date part for easier grouping
        ProcessedAt DATETIME DEFAULT CURRENT_TIMESTAMP -- When this record was processed
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("ProcessedAppReviews table checked/created successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error creating/checking ProcessedAppReviews table: {e}")
        raise


def clean_and_preprocess_text(text):
    """
    Cleans and preprocesses a single piece of text.
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    # 1. Lowercasing
    text = text.lower()

    # 2. Remove HTML tags
    text = BeautifulSoup(text, "html.parser").get_text()

    # 3. Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

    # 4. Remove Emojis (basic regex approach)
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # various symbols
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001F926-\U0001F937"  # person gestures
        u"\U0001F900-\U0001F9FF"  # supplemental symbols
        u"\U0001FA70-\U0001FAFF"  # extended-a
        u"\U00010000-\U0010FFFF"  # high Unicode planes (valid range)
        u"\u2640-\u2642"          # gender symbols
        u"\u2600-\u2B55"
        u"\u200d"                 # zero width joiner
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"                 # variation selectors
        u"\u3030"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)

    # 5. Expand Contractions (e.g., "don't" -> "do not")
    text = contractions.fix(text)

    # 6. Remove punctuation and numbers (keep only letters and spaces)
    text = re.sub(r'[^a-zA-Z\s]', '', text)

    # 7. Tokenization
    tokens = word_tokenize(text)

    # 8. Remove Stopwords
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words]

    # 9. Lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]

    # 10. Join tokens back into a single string
    cleaned_text = ' '.join(lemmatized_tokens)

    return cleaned_text


def process_reviews():
    """Main processing function."""
    logger.info("=== Data Processing (NLP Cleaning) Started ===")
    start_time = datetime.now()

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to establish database connection.")
            return

        create_processed_table_if_not_exists(conn)

        # Load raw data into a DataFrame for easier manipulation
        query = """
        SELECT id, AppName, PlayStoreReviewID, Rating, ReviewDate, ReviewText
        FROM RawAppReviews
        """
        df_raw = pd.read_sql_query(query, conn)

        if df_raw.empty:
            logger.info("No raw reviews found to process.")
            return

        logger.info(f"Loaded {len(df_raw)} raw reviews for processing.")

        # Apply cleaning function
        logger.info("Applying text cleaning and preprocessing...")
        df_raw['CleanedReviewText'] = df_raw['ReviewText'].apply(clean_and_preprocess_text)
        df_raw['ReviewWordCount'] = df_raw['CleanedReviewText'].apply(
            lambda x: len(x.split()) if isinstance(x, str) else 0
        )

        # Convert ReviewDate to datetime and handle invalid values
        df_raw['ReviewDate'] = pd.to_datetime(df_raw['ReviewDate'], errors='coerce')

        # Derive YearMonth (first day of the month)
        df_raw['YearMonth'] = df_raw['ReviewDate'].dt.to_period('M').dt.start_time

        # Select and rename columns for insertion
        df_to_insert = df_raw[[
            'id', 'AppName', 'PlayStoreReviewID', 'Rating', 'ReviewDate',
            'ReviewText', 'CleanedReviewText', 'ReviewWordCount', 'YearMonth'
        ]].copy()
        df_to_insert.rename(columns={'id': 'RawReviewID'}, inplace=True)
        df_to_insert['VADER_Score'] = None
        df_to_insert['VADER_Sentiment'] = None

        # 🔧 CRITICAL FIX: Convert Timestamps to strings for SQLite compatibility
        df_to_insert['ReviewDate'] = df_to_insert['ReviewDate'].astype(str)
        df_to_insert['YearMonth'] = df_to_insert['YearMonth'].astype(str)

        # Handle NaT (Not a Time) and NaN values
        df_to_insert = df_to_insert.fillna('')

        # Convert DataFrame to list of tuples
        records = [tuple(row) for row in df_to_insert.to_numpy()]

        # Insert into database
        insert_query = """
        INSERT OR IGNORE INTO ProcessedAppReviews
        (RawReviewID, AppName, PlayStoreReviewID, Rating, ReviewDate, ReviewText,
         CleanedReviewText, ReviewWordCount, YearMonth, VADER_Score, VADER_Sentiment)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor = conn.cursor()
        cursor.executemany(insert_query, records)
        conn.commit()
        rows_inserted = cursor.rowcount

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(
            f"=== Data Processing Completed Successfully! "
            f"Duration: {duration}, Rows Inserted: {rows_inserted} ==="
        )

    except Exception as e:
        logger.error(f"=== Data Processing Failed! Error: {e} ===", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    process_reviews()