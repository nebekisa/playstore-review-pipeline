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
def create_metadata_table_if_not_exists(conn):
    """Creates the AppScrapingMetadata table if it doesn't already exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS AppScrapingMetadata (
        AppName TEXT PRIMARY KEY, -- Unique identifier for each app
        LastScrapedAt DATETIME NOT NULL -- Timestamp of the last successful scrape for this app
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("AppScrapingMetadata table checked/created successfully.")
    except Exception as e:
         logger.error(f"Error creating/checking AppScrapingMetadata table: {e}")
         raise

def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Enable foreign key constraints (good practice, though not used in this specific table)
        conn.execute('PRAGMA foreign_keys = ON')
        create_metadata_table_if_not_exists(conn)
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
    """
    Scrapes reviews using google-play-scraper incrementally and inserts them into SQLite.
    Only fetches reviews newer than the last scraped timestamp for the app.
    """
    logger.info(f"Starting to scrape reviews for {app_name} ({app_id})...")
    cursor = conn.cursor()

    # --- 1. Get Last Scraped Timestamp for this App ---
    last_scraped_at = None
    try:
        cursor.execute("SELECT LastScrapedAt FROM AppScrapingMetadata WHERE AppName = ?", (app_name,))
        result = cursor.fetchone()
        if result:
            last_scraped_at = result[0]
            logger.info(f"  Last scraped timestamp for {app_name}: {last_scraped_at}")
        else:
            logger.info(f"  No previous scrape record found for {app_name}. Will scrape all available reviews up to max_reviews.")
    except Exception as e:
        logger.warning(f"  Could not retrieve last scraped timestamp for {app_name}: {e}. Assuming full scrape is needed.")

    # --- 2. Scrape Reviews (Incrementally) ---
    # Initial request, sorted by NEWEST
    result, continuation_token = reviews(
        app_id,
        lang='en',
        country='us',
        sort=Sort.NEWEST, # Start with newest
        count=min(100, max_reviews) # Max 100 per request
    )

    total_reviews_scraped = 0
    batch_number = 1
    current_run_max_review_date = None # Track the latest review date encountered in *this run*
    new_reviews_found = False # Flag to check if any new reviews were found

    while result and total_reviews_scraped < max_reviews:
        logger.info(f"  Processing batch {batch_number} for {app_name}...")
        batch_reviews_to_insert = []
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
            scraped_at = datetime.now() # Timestamp when this review was scraped *this time*

            # --- CRUCIAL: Check for Incrementality ---
            # If we have a last_scraped_at timestamp and this review's date is older, stop scraping
            if last_scraped_at and review_date and review_date <= pd.to_datetime(last_scraped_at):
                logger.info(f"    Stopping scrape for {app_name} at review dated {review_date} (<= last scraped {last_scraped_at}).")
                break # Stop processing this batch and exit the outer loop

            new_reviews_found = True # At least one review passed the date check

            # Track the latest review date in this run to update metadata later
            if review_date:
                if current_run_max_review_date is None or review_date > current_run_max_review_date:
                    current_run_max_review_date = review_date

            # Prepare record for batch insert
            record = (
                app_name, play_store_review_id, user_name, user_image_url,
                review_title, review_text, rating, thumbs_up_count, app_version,
                review_date, reply_content, replied_at, scraped_at
            )
            batch_reviews_to_insert.append(record)
            total_reviews_scraped += 1

        # --- 3. Insert Batch (if any new reviews were found) ---
        if batch_reviews_to_insert:
            logger.info(f"    Inserting batch {batch_number} ({len(batch_reviews_to_insert)} reviews) for {app_name}...")
            insert_query = """
            INSERT OR IGNORE INTO RawAppReviews
            (AppName, PlayStoreReviewID, UserName, UserImageURL, ReviewTitle,
             ReviewText, Rating, ThumbsUpCount, AppVersion, ReviewDate,
             ReplyContent, RepliedAt, ScrapedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            try:
                cursor.executemany(insert_query, batch_reviews_to_insert)
                conn.commit()
                logger.info(f"    Batch {batch_number} inserted successfully for {app_name}.")
            except Exception as e:
                logger.error(f"    Error inserting batch {batch_number} for {app_name}: {e}")
                conn.rollback() # Rollback on error for this batch
                # Depending on severity, you might want to raise or continue

        # --- 4. Check Stop Condition ---
        # If we stopped adding reviews due to date check, break
        if last_scraped_at and (not new_reviews_found or (current_run_max_review_date and current_run_max_review_date <= pd.to_datetime(last_scraped_at))):
             logger.info(f"    No more new reviews found for {app_name} based on date check. Stopping scrape.")
             break

        # If we've hit the max_reviews limit, break
        if total_reviews_scraped >= max_reviews:
            logger.info(f"    Reached max_reviews limit ({max_reviews}) for {app_name}. Stopping scrape.")
            break

        # --- 5. Prepare for Next Batch ---
        if continuation_token is None:
            logger.info(f"    No more pages available for {app_name}. Stopping scrape.")
            break

        logger.info(f"    Pausing for 1 second before fetching next batch...")
        time.sleep(1) # Be respectful

        remaining_reviews = max_reviews - total_reviews_scraped
        result, continuation_token = reviews(
            app_id,
            continuation_token=continuation_token, # Use the token for next page
            lang='en',
            country='us',
            sort=Sort.NEWEST,
            count=min(100, remaining_reviews)
        )
        batch_number += 1

    # --- 6. Update Last Scraped Timestamp (if new reviews were found) ---
    if new_reviews_found and current_run_max_review_date:
        try:
            update_metadata_query = """
            INSERT OR REPLACE INTO AppScrapingMetadata (AppName, LastScrapedAt)
            VALUES (?, ?)
            """
            cursor.execute(update_metadata_query, (app_name, current_run_max_review_date))
            conn.commit()
            logger.info(f"  Updated LastScrapedAt for {app_name} to {current_run_max_review_date}.")
        except Exception as e:
            logger.error(f"  Error updating LastScrapedAt for {app_name}: {e}")

    logger.info(f"--- Finished scraping for {app_name}. Total new reviews scraped/inserted: {total_reviews_scraped} ---")
    return total_reviews_scraped # Return count for potential use later

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
