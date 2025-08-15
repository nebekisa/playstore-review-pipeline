# scraper/scraper.py
import sqlite3
import time
# from google_play_scraper import reviews, Sort # Moved inside scrape_and_store_reviews for clarity
from datetime import datetime
import logging
import os

# --- Configuration ---
DB_PATH = os.getenv('DB_PATH', '/app/database/reviews.db') # Use env var or default relative path
APPS_TO_SCRAPE = {
    'AliExpress': 'com.alibaba.aliexpresshd',
    'Alibaba': 'com.alibaba.intl.android.apps.poseidon',
    'Jiji': 'ng.jiji.app'
}
MAX_REVIEWS_PER_APP = int(os.getenv('MAX_REVIEWS_PER_APP', 500))  # Configurable
BATCH_SIZE = 100
DELAY_BETWEEN_BATCHES = 1  # seconds

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Import google_play_scraper inside function or at the top ---
# It's generally better to import at the top if the module is always needed.
# However, conditional imports can be useful for handling missing dependencies gracefully.
# For this script, we assume it's a dependency.
from google_play_scraper import reviews, Sort


def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Enable foreign key constraints (good practice, though not used in this specific table)
        conn.execute('PRAGMA foreign_keys = ON')
        logger.info(f"Connected to SQLite database at {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database at {DB_PATH}: {e}")
        raise # Re-raise to stop the script if connection fails
    except Exception as e: # Catch other potential errors
         logger.error(f"Unexpected error connecting to database at {DB_PATH}: {e}")
         raise

def create_raw_table_if_not_exists(conn):
    """Creates the RawAppReviews table if it doesn't already exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS RawAppReviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        IngestionTimestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        AppName TEXT NOT NULL,
        PlayStoreReviewID TEXT UNIQUE, -- Unique ID from Play Store
        UserName TEXT,
        UserImageURL TEXT,
        Rating INTEGER NOT NULL CHECK (Rating >= 1 AND Rating <= 5),
        ReviewDate DATE,
        ReviewTitle TEXT,
        ReviewText TEXT,
        ThumbsUpCount INTEGER,
        AppVersion TEXT,
        ReplyContent TEXT,
        RepliedAt DATETIME,
        ScrapedAt DATETIME -- Timestamp from the scraper run
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("RawAppReviews table checked/created successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error creating/checking RawAppReviews table: {e}")
        raise # Re-raise to stop the script if table creation fails
    except Exception as e:
         logger.error(f"Unexpected error creating/checking RawAppReviews table: {e}")
         raise

def scrape_and_store_reviews(app_name, app_id, conn, max_reviews):
    """Scrapes reviews and stores them in SQLite, handling duplicates."""
    logger.info(f"Starting to scrape reviews for {app_name} ({app_id}) - Max Reviews: {max_reviews}")
    cursor = conn.cursor()

    # Ensure table exists before inserting
    create_raw_table_if_not_exists(conn)

    # Initial request
    result, continuation_token = reviews(
        app_id,
        lang='en',
        country='us',
        sort=Sort.NEWEST, # Start with newest
        count=min(BATCH_SIZE, max_reviews) # Max 100 per request
    )

    total_reviews_scraped = 0
    total_reviews_inserted = 0 # Track actual inserts
    batch_number = 1

    while result and total_reviews_scraped < max_reviews:
        logger.info(f"  Processing batch {batch_number} for {app_name}...")
        for review in result:
            play_store_review_id = review.get('reviewId')
            user_name = review.get('userName')
            user_image_url = review.get('userImage')
            review_title = review.get('title')
            review_text = review.get('content')
            rating = review.get('score')
            thumbs_up_count = review.get('thumbsUpCount')
            app_version = review.get('reviewCreatedVersion')
            review_date = review.get('at') # This is a datetime object
            reply_content = review.get('replyContent')
            replied_at = review.get('repliedAt')
            scraped_at = datetime.utcnow() # Use UTC for consistency

            # --- Data Quality Checks for Raw Data ---
            is_valid = True
            errors = []

            # 1. Check for essential fields
            if not play_store_review_id:
                is_valid = False
                errors.append("Missing PlayStoreReviewID")
            if not isinstance(review_text, str) or not review_text.strip():
                is_valid = False
                errors.append("Missing or empty ReviewText")
            if rating is None or not (isinstance(rating, int) and 1 <= rating <= 5):
                is_valid = False
                errors.append(f"Invalid Rating: {rating}")

            # 2. Check ReviewDate (optional but good practice)
            # Ensure it's a datetime object or can be parsed
            if review_date and not isinstance(review_date, datetime):
                # Attempt to parse if it's a string (unlikely from google-play-scraper, but defensive)
                try:
                    review_date = pd.to_datetime(review_date) # This requires pandas, remove if not needed
                except (NameError, ValueError, TypeError): # NameError if pd not imported, ValueError/TypeError if parsing fails
                    # If parsing fails or pandas not available, log warning but don't discard if other data is okay
                    logger.warning(f"    Unparsable ReviewDate for review {play_store_review_id}: {review_date}. Keeping review.")
                    # Optionally set review_date to None or current time if parsing fails critically
                    # review_date = None # Or datetime.utcnow()

            # 3. Check for future dates (optional)
            if review_date and review_date > datetime.utcnow():
                logger.warning(f"    Future ReviewDate found for review {play_store_review_id}: {review_date}. Keeping review.")

            # --- END Data Quality Checks ---

            if not is_valid:
                logger.warning(f"    Skipping invalid review {play_store_review_id} for {app_name}. Errors: {', '.join(errors)}")
                continue # Skip inserting this review

            # --- Proceed with Insertion if Valid ---
            insert_sql = """
            INSERT OR IGNORE INTO RawAppReviews
            (AppName, PlayStoreReviewID, UserName, UserImageURL, ReviewTitle,
             ReviewText, Rating, ThumbsUpCount, AppVersion, ReviewDate,
             ReplyContent, RepliedAt, ScrapedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            try:
                cursor.execute(insert_sql, (
                    app_name, play_store_review_id, user_name, user_image_url,
                    review_title, review_text, rating, thumbs_up_count, app_version,
                    review_date, reply_content, replied_at, scraped_at
                ))
                # conn.commit() is called after the batch for better performance
                total_reviews_inserted += 1 # Increment only if insertion was attempted (even if IGNOREd)

            except sqlite3.IntegrityError as e:
                # This usually happens if the UNIQUE constraint on PlayStoreReviewID fails (duplicate)
                if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                    logger.debug(f"    Duplicate Review ID {play_store_review_id} found for {app_name}, skipping.")
                else:
                    logger.error(f"    Integrity Error inserting review {play_store_review_id}: {e}")
            except Exception as e:
                logger.error(f"    General Error inserting review {play_store_review_id}: {e}")
                # Depending on the error, you might want to continue or raise

            total_reviews_scraped += 1

        # Commit after processing a batch
        conn.commit()
        logger.info(f"    Inserted batch {batch_number}. Reviews processed so far: {total_reviews_scraped}, New inserts: {total_reviews_inserted}")

        if continuation_token is None or total_reviews_scraped >= max_reviews:
            break

        logger.info(f"  Pausing for {DELAY_BETWEEN_BATCHES} second(s)...")
        time.sleep(DELAY_BETWEEN_BATCHES)

        remaining_reviews = max_reviews - total_reviews_scraped
        result, continuation_token = reviews(
            app_id,
            continuation_token=continuation_token, # Use the token for next page
            lang='en',
            country='us',
            sort=Sort.NEWEST,
            count=min(BATCH_SIZE, remaining_reviews)
        )
        batch_number += 1

    logger.info(f"--- Finished scraping for {app_name}. Total reviews processed: {total_reviews_scraped}, New inserts: {total_reviews_inserted} ---")
    return total_reviews_inserted # Return count for potential use later


def main():
    """Main function to orchestrate the scraping process."""
    logger.info("=== Google Play Store Review Scraper (SQLite) Started ===")
    start_time = datetime.now()

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to establish database connection.")
            return # Exit if connection failed

        total_reviews_all_apps = 0
        for app_name, app_id in APPS_TO_SCRAPE.items():
            try:
                max_reviews = MAX_REVIEWS_PER_APP # Could be made app-specific in config
                inserted_count = scrape_and_store_reviews(app_name, app_id, conn, max_reviews)
                total_reviews_all_apps += inserted_count
            except Exception as e:
                logger.error(f"An error occurred while scraping {app_name} ({app_id}): {e}", exc_info=True) # Log full traceback
                # Decide if you want the whole pipeline to fail or continue with other apps
                # For now, let's continue with other apps
                continue

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Scraper Run Completed Successfully! Duration: {duration}, Total New Reviews Inserted: {total_reviews_all_apps} ===")

    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user.")
    except Exception as e:
        logger.error(f"=== Scraper Run Failed! Unexpected Error: {e} ===", exc_info=True) # Log full traceback
        raise # Re-raise to signal failure to orchestrator (e.g., Airflow)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
    logger.info("Scraping script finished.")
