# scraper/scraper.py
import sqlite3
import time
from google_play_scraper import reviews, Sort
from datetime import datetime
import logging
import os

# --- Configuration ---
DB_PATH = "/app/database/reviews.db"# Use env var or default relative path
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


def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Enable foreign key constraints
        conn.execute('PRAGMA foreign_keys = ON')
        logger.info(f"Connected to SQLite database at {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def create_raw_table_if_not_exists(conn):
    """Creates the RawAppReviews table if it doesn't already exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS RawAppReviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        IngestionTimestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        AppName TEXT NOT NULL,
        PlayStoreReviewID TEXT UNIQUE,
        UserName TEXT,
        UserImageURL TEXT,
        Rating INTEGER NOT NULL CHECK (Rating >= 1 AND Rating <= 5),
        ReviewDate DATETIME,
        ReviewTitle TEXT,
        ReviewText TEXT,
        ThumbsUpCount INTEGER,
        AppVersion TEXT,
        ReplyContent TEXT,
        RepliedAt DATETIME,
        ScrapedAt DATETIME
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info("RawAppReviews table checked/created successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error creating/checking RawAppReviews table: {e}")
        raise


def scrape_and_store_reviews(app_name, app_id, conn, max_reviews):
    """Scrapes reviews and stores them in SQLite, handling duplicates."""
    logger.info(f"Starting to scrape reviews for {app_name} ({app_id})...")
    cursor = conn.cursor()
    create_raw_table_if_not_exists(conn)  # Ensure table exists

    # Start scraping
    result, continuation_token = reviews(
        app_id,
        lang='en',
        country='us',
        sort=Sort.NEWEST,
        count=min(BATCH_SIZE, max_reviews)
    )

    total_reviews_scraped = 0
    total_reviews_inserted = 0
    batch_number = 1

    while result and total_reviews_scraped < max_reviews:
        logger.info(f"  Processing batch {batch_number} for {app_name}...")

        insert_query = """
        INSERT OR IGNORE INTO RawAppReviews
        (AppName, PlayStoreReviewID, UserName, UserImageURL, ReviewTitle, ReviewText, Rating,
         ThumbsUpCount, AppVersion, ReviewDate, ReplyContent, RepliedAt, ScrapedAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for review in result:
            play_store_review_id = review.get('reviewId')
            user_name = review.get('userName')
            user_image_url = review.get('userImage')
            review_title = review.get('title')
            review_text = review.get('content')
            rating = review.get('score')
            thumbs_up_count = review.get('thumbsUpCount')
            app_version = review.get('reviewCreatedVersion')
            review_date = review.get('at')
            reply_content = review.get('replyContent')
            replied_at = review.get('repliedAt')
            scraped_at = datetime.utcnow()  # Always use current UTC time for scraping

            try:
                cursor.execute(insert_query, (
                    app_name,
                    play_store_review_id,
                    user_name,
                    user_image_url,
                    review_title,
                    review_text,
                    rating,
                    thumbs_up_count,
                    app_version,
                    review_date,
                    reply_content,
                    replied_at,
                    scraped_at
                ))
            except sqlite3.Error as e:
                logger.error(f"    Error inserting review {play_store_review_id}: {e}")

        conn.commit()
        rows_inserted = cursor.rowcount
        total_reviews_inserted += rows_inserted
        total_reviews_scraped += len(result)

        logger.info(
            f"    Batch {batch_number} committed. "
            f"Reviews processed: {len(result)}, "
            f"New inserts: {rows_inserted}, "
            f"Total inserted: {total_reviews_inserted}"
        )

        # Break if no more reviews or reached limit
        if not continuation_token or total_reviews_scraped >= max_reviews:
            break

        logger.info(f"  Pausing for {DELAY_BETWEEN_BATCHES} second(s) before next batch...")
        time.sleep(DELAY_BETWEEN_BATCHES)

        # Request next batch
        remaining = max_reviews - total_reviews_scraped
        result, continuation_token = reviews(
            app_id,
            continuation_token=continuation_token,
            lang='en',
            country='us',
            sort=Sort.NEWEST,
            count=min(BATCH_SIZE, remaining)
        )
        batch_number += 1

    logger.info(
        f"--- Finished scraping for {app_name}. "
        f"Total processed: {total_reviews_scraped}, "
        f"Total new inserts: {total_reviews_inserted} ---"
    )
    return total_reviews_inserted


def main():
    """Main function to orchestrate the scraping process."""
    logger.info("=== Google Play Store Review Scraper (SQLite) Started ===")
    start_time = datetime.now()

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to establish database connection.")
            return

        total_reviews_all_apps = 0
        for app_name, app_id in APPS_TO_SCRAPE.items():
            try:
                inserted = scrape_and_store_reviews(app_name, app_id, conn, MAX_REVIEWS_PER_APP)
                total_reviews_all_apps += inserted
            except Exception as e:
                logger.error(f"Failed to scrape {app_name} ({app_id}): {e}", exc_info=True)

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(
            f"=== Scraper Run Completed Successfully! "
            f"Duration: {duration}, "
            f"Total New Reviews Inserted: {total_reviews_all_apps} ==="
        )

    except Exception as e:
        logger.error(f"=== Scraper Run Failed! Error: {e} ===", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()