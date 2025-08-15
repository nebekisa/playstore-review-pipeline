# processor/processor.py
"""
Processes raw app reviews: cleans text, calculates features (word count, VADER sentiment),
and stores results in the ProcessedAppReviews table.
"""

import sqlite3
import pandas as pd
from datetime import datetime
import logging
import os
import sys

# --- NLTK Imports (Ensure installed in Airflow environment) ---
try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import WordNetLemmatizer
    from bs4 import BeautifulSoup
    import re
    import contractions
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError as e:
    print(f"Error importing NLTK or related libraries: {e}")
    print("Please ensure these are installed in the Airflow environment.")
    raise

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # Log to stdout for Airflow capture
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Use environment variable for DB path, defaulting to standard location inside container
DB_PATH = os.getenv('DB_PATH', '/app/database/reviews.db')

# --- NLTK Data Download (Ensure available - handled by Dockerfile or init script ideally) ---
# In production/robust setup, this is done during image build.
# For runtime safety (if data missing), uncomment the lines below:
# try:
#     stop_words = set(stopwords.words('english'))
#     lemmatizer = WordNetLemmatizer()
#     analyzer = SentimentIntensityAnalyzer()
#     logger.info("NLTK resources loaded successfully.")
# except LookupError:
#     logger.info("Downloading required NLTK data...")
#     nltk.download('punkt')
#     nltk.download('punkt_tab')
#     nltk.download('stopwords')
#     nltk.download('wordnet')
#     nltk.download('omw-1.4')
#     nltk.download('vader_lexicon')
#     # Reload after download
#     stop_words = set(stopwords.words('english'))
#     lemmatizer = WordNetLemmatizer()
#     analyzer = SentimentIntensityAnalyzer()
#     logger.info("NLTK data downloaded and loaded successfully.")

# --- Helper Functions ---
def get_db_connection(db_path):
    """Establishes and returns a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to SQLite database at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database at {db_path}: {e}")
        raise

def create_processed_table_if_not_exists(conn):
    """Creates the ProcessedAppReviews table if it doesn't already exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ProcessedAppReviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        RawReviewID INTEGER UNIQUE, -- Link back to RawAppReviews.id
        AppName TEXT NOT NULL,
        PlayStoreReviewID TEXT UNIQUE, -- Unique ID from Play Store
        Rating INTEGER NOT NULL CHECK (Rating >= 1 AND Rating <= 5),
        ReviewDate DATETIME, -- Date the review was posted
        ReviewText TEXT, -- Original text
        CleanedReviewText TEXT, -- The cleaned text
        ReviewWordCount INTEGER, -- Calculated word count
        VADER_Score REAL, -- Sentiment score from VADER
        VADER_Sentiment TEXT CHECK(VADER_Sentiment IN ('Positive', 'Negative', 'Neutral')), -- Sentiment label from VADER
        YearMonth DATE, -- Derived date part for easier grouping (stored as DATE)
        ProcessedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP -- When this record was processed
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("ProcessedAppReviews table checked/created successfully.")
    except Exception as e:
         logger.error(f"Error creating/checking ProcessedAppReviews table: {e}")
         raise

def clean_and_preprocess_text(text):
    """
    Cleans and preprocesses a single piece of text.
    Steps: Lowercasing, HTML removal, URL removal, emoji removal,
          punctuation removal, contraction expansion, tokenization,
          stopword removal, lemmatization.
    """
    if not isinstance(text, str):
         return "" # Return empty string for non-string inputs

    # 1. Lowercasing
    text = text.lower()

    # 2. Remove HTML tags
    text = BeautifulSoup(text, "html.parser").get_text()

    # 3. Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)

    # 4. Remove Emojis (basic regex approach)
    # More robust handling can use libraries like 'demoji'
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)

    # 5. Expand Contractions (e.g., "don't" -> "do not")
    text = contractions.fix(text)

    # 6. Remove punctuation and numbers (keep only letters and spaces)
    # You might want to be more nuanced here (e.g., keep '$' for price mentions)
    text = re.sub(r'[^a-zA-Z\s]', '', text)

    # 7. Tokenization
    tokens = word_tokenize(text)

    # 8. Remove Stopwords
    # Consider if domain-specific words like 'app', 'buy', 'price' should be kept
    # For now, we'll remove standard stopwords
    try:
        stop_words = set(stopwords.words('english'))
    except LookupError:
        logger.warning("NLTK stopwords not found, proceeding without stopword removal.")
        stop_words = set()
    filtered_tokens = [word for word in tokens if word not in stop_words]

    # 9. Lemmatization (reduces words to their base/dictionary form)
    # e.g., 'running', 'ran' -> 'run'
    try:
        lemmatizer = WordNetLemmatizer()
        lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]
    except LookupError:
        logger.warning("NLTK wordnet lemmatizer data not found, proceeding without lemmatization.")
        lemmatized_tokens = filtered_tokens # Fallback to filtered tokens

    # 10. Join tokens back into a single string
    cleaned_text = ' '.join(lemmatized_tokens)

    return cleaned_text

def get_vader_score(text):
    """Gets the VADER compound sentiment score for a piece of text."""
    if not isinstance(text, str) or not text.strip():
        return 0.0 # Neutral score for missing/empty text
    try:
        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(text)
        return scores['compound'] # Compound score is the overall sentiment (-1 to 1)
    except Exception as e:
        logger.warning(f"Error calculating VADER score for text: {text[:50]}... Error: {e}")
        return 0.0 # Return neutral on error

def classify_sentiment(score):
    """Classifies a VADER compound score into Positive, Negative, or Neutral."""
    if score >= 0.05:
        return 'Positive'
    elif score <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'

# --- Main Processing Function ---
def process_reviews():
    """
    Main function to process reviews from RawAppReviews and store in ProcessedAppReviews.
    This function is called by the Airflow task.
    """
    logger.info("=== Data Processing (NLP Cleaning & VADER Sentiment) Started ===")
    start_time = datetime.now()

    conn = None
    try:
        # --- 1. Connect to Database ---
        conn = get_db_connection(DB_PATH)
        if not conn:
            logger.error("Failed to establish database connection.")
            return

        # --- 2. Ensure Processed Table Exists ---
        create_processed_table_if_not_exists(conn)

        # --- 3. Load Raw Reviews ---
        logger.info("Loading raw reviews from RawAppReviews table...")
        query = "SELECT * FROM RawAppReviews"
        df_raw = pd.read_sql_query(query, conn)
        logger.info(f"Loaded {len(df_raw)} raw reviews for processing.")

        if df_raw.empty:
            logger.warning("No raw reviews found in RawAppReviews table. Nothing to process.")
            return

        # --- 4. Data Preprocessing ---
        # Ensure ReviewDate is datetime for deriving YearMonth later
        # Handle potential parsing errors gracefully
        df_raw['ReviewDate'] = pd.to_datetime(df_raw['ReviewDate'], errors='coerce')
        logger.info("Converted ReviewDate column to datetime.")

        # --- 5. Apply Text Cleaning & Preprocessing ---
        logger.info("Applying text cleaning and preprocessing...")
        df_raw['CleanedReviewText'] = df_raw['ReviewText'].apply(clean_and_preprocess_text)
        logger.info("Text cleaning and preprocessing completed.")

        # --- 6. Calculate Review Word Count ---
        logger.info("Calculating review word counts...")
        df_raw['ReviewWordCount'] = df_raw['CleanedReviewText'].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
        logger.info("Review word counts calculated.")

        # --- 7. Apply VADER Sentiment Analysis ---
        logger.info("Applying VADER sentiment analysis...")
        df_raw['VADER_Score'] = df_raw['CleanedReviewText'].apply(get_vader_score)
        df_raw['VADER_Sentiment'] = df_raw['VADER_Score'].apply(classify_sentiment)
        logger.info("VADER sentiment analysis completed.")

        # --- 8. Derive YearMonth ---
        logger.info("Deriving YearMonth...")
        # df_raw['YearMonth'] is already derived in EDA/Scraper as ReviewDate.dt.to_period('M').dt.start_time
        # If not, derive it here:
        # df_raw['YearMonth'] = df_raw['ReviewDate'].dt.to_period('M').dt.start_time
        # For SQLite compatibility, ensure it's datetime. It likely already is from EDA.
        # Let's assume it's in df_raw from EDA. If not, uncomment the line above.
        logger.info("YearMonth derived.")

        # --- 9. Prepare Data for Insertion ---
        logger.info("Preparing data for insertion into ProcessedAppReviews table...")
        records = []
        processed_at = datetime.now() # Timestamp for when this processing run happened

        for index, row in df_raw.iterrows():
            # --- CRUCIAL FIX: Convert Pandas Timestamps to Python datetime.datetime ---
            # This prevents the "Error binding parameter X: type 'Timestamp' is not supported" error
            raw_review_id = row['id']
            app_name = row['AppName']
            play_store_review_id = row['PlayStoreReviewID']
            rating = row['Rating']

            # Convert ReviewDate (pd.Timestamp or NaT) to Python datetime.datetime or None
            review_date = row['ReviewDate']
            if pd.notnull(review_date):
                review_date_for_db = review_date.to_pydatetime()
            else:
                review_date_for_db = None

            review_text = row['ReviewText']
            cleaned_text = row['CleanedReviewText']
            review_word_count = row['ReviewWordCount']
            vader_score = row['VADER_Score']
            vader_sentiment = row['VADER_Sentiment']

            # Convert YearMonth (pd.Timestamp or NaT) to Python datetime.datetime or None
            # Assuming YearMonth was derived in EDA as dt.start_time, it should already be Timestamp
            year_month = row.get('YearMonth') # Use .get() in case column name differs slightly
            if year_month is not None and pd.notnull(year_month):
                # If it's already a Timestamp (from dt.start_time), convert it.
                if isinstance(year_month, pd.Timestamp):
                    year_month_for_db = year_month.to_pydatetime()
                # If it's a Period, convert it to Timestamp then datetime
                elif isinstance(year_month, pd.Period):
                    year_month_for_db = year_month.start_time.to_pydatetime()
                else:
                    # If it's already a datetime.datetime or string, pass as is (SQLite usually handles)
                    year_month_for_db = year_month
            else:
                year_month_for_db = None # Handle missing/NaT

            # ProcessedAt is already a datetime.datetime object
            processed_at_for_db = processed_at

            # --- Prepare record tuple (order MUST match INSERT query columns) ---
            record = (
                raw_review_id,          # RawReviewID (INTEGER)
                app_name,               # AppName (TEXT)
                play_store_review_id,   # PlayStoreReviewID (TEXT)
                rating,                 # Rating (INTEGER)
                review_date_for_db,     # ReviewDate (DATETIME or None) <-- FIXED TIMESTAMP
                review_text,            # ReviewText (TEXT)
                cleaned_text,           # CleanedReviewText (TEXT)
                review_word_count,      # ReviewWordCount (INTEGER)
                vader_score,            # VADER_Score (REAL)
                vader_sentiment,        # VADER_Sentiment (TEXT)
                year_month_for_db,      # YearMonth (DATE or None) <-- FIXED TIMESTAMP
                processed_at_for_db     # ProcessedAt (DATETIME) <-- This one is already datetime
            )
            records.append(record)

        logger.info(f"Prepared {len(records)} records for insertion.")

        # --- 10. Insert Processed Data ---
        logger.info("Inserting processed data into ProcessedAppReviews table...")
        cursor = conn.cursor()
        insert_query = """
        INSERT OR IGNORE INTO ProcessedAppReviews
        (RawReviewID, AppName, PlayStoreReviewID, Rating, ReviewDate,
         ReviewText, CleanedReviewText, ReviewWordCount, VADER_Score, VADER_Sentiment,
         YearMonth, ProcessedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            cursor.executemany(insert_query, records)
            conn.commit()
            rows_inserted = cursor.rowcount
            logger.info(f"Successfully inserted/updated {rows_inserted} records into ProcessedAppReviews.")
        except sqlite3.Error as e:
            logger.error(f"SQLite error during data insertion: {e}")
            conn.rollback() # Rollback changes on error
            raise
        except Exception as e:
            logger.error(f"Unexpected error during data insertion: {e}")
            conn.rollback()
            raise

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Data Processing Completed Successfully! Duration: {duration} ===")

    except Exception as e:
        logger.error(f"=== Data Processing Failed! Error: {e} ===")
        raise # Re-raise to signal failure to Airflow
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

# --- Allow script to be run directly (for testing outside Airflow) ---
if __name__ == "__main__":
    process_reviews()
