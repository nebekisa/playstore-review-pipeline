# utils/db_utils.py (or inside scraper.py / processor.py)

from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def run_post_processing_validation(conn, table_name="RawAppReviews"):
    """
    Runs standard data quality checks on the specified table.
    Can be used in both scraper and processor pipelines.
    """
    logger.info(f"--- Running Data Quality Validation on '{table_name}' ---")
    cursor = conn.cursor()

    # 1. Check for NULL or empty AppName
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE AppName IS NULL OR TRIM(AppName) = ''")
    null_appname_count = cursor.fetchone()[0]
    if null_appname_count > 0:
        logger.warning(f"⚠️  {null_appname_count} records have NULL/empty AppName.")
    else:
        logger.info("✅ All records have valid AppName.")

    # 2. Check for NULL or empty ReviewText
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ReviewText IS NULL OR TRIM(ReviewText) = ''")
    null_text_count = cursor.fetchone()[0]
    if null_text_count > 0:
        logger.warning(f"⚠️  {null_text_count} records have NULL/empty ReviewText.")
    else:
        logger.info("✅ All records have valid ReviewText.")

    # 3. Check for Invalid Ratings (only for RawAppReviews)
    if table_name == "RawAppReviews":
        cursor.execute("SELECT COUNT(*) FROM RawAppReviews WHERE Rating < 1 OR Rating > 5")
        invalid_rating_count = cursor.fetchone()[0]
        if invalid_rating_count > 0:
            logger.warning(f"⚠️  {invalid_rating_count} records have invalid ratings (not 1–5).")
        else:
            logger.info("✅ All ratings are within valid range (1–5).")

    # 4. Check for Duplicate PlayStoreReviewID per App
    cursor.execute(f"""
        SELECT AppName, PlayStoreReviewID, COUNT(*) as cnt
        FROM {table_name}
        WHERE PlayStoreReviewID IS NOT NULL
        GROUP BY AppName, PlayStoreReviewID
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    if duplicates:
        logger.warning(f"⚠️  Found {len(duplicates)} duplicate PlayStoreReviewIDs:")
        for app_name, review_id, count in duplicates:
            logger.warning(f"   App: '{app_name}', ReviewID: {review_id}, Duplicates: {count}")
    else:
        logger.info("✅ No duplicate PlayStoreReviewIDs found.")

    # 5. Check for Future Dates
    date_column = "ReviewDate"
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {date_column} > ?", (datetime.now(),))
    future_date_count = cursor.fetchone()[0]
    if future_date_count > 0:
        logger.warning(f"⚠️  {future_date_count} records have future {date_column}.")
    else:
        logger.info(f"✅ No future {date_column} values found.")

    logger.info(f"--- Data Quality Validation on '{table_name}' Completed ---\n")