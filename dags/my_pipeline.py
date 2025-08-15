# dags/playstore_pipeline_dag.py (Corrected Professional Version)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.models import Variable # For Airflow Variables (optional)
import sys
import os
import logging # Use Python's standard logging

# --- Configure Logger for this DAG ---
# Using Airflow's logging mechanism is often preferred, but standard logging works too.
# For simplicity here, we'll stick to standard logging basics.
# In a more complex setup, you might integrate with Airflow's logger hierarchy.
logger = logging.getLogger(__name__) # Gets the logger named after this module

# --- IMPORTANT: Ensure Airflow can find your modules ---
# Paths INSIDE the Airflow container where volumes are mounted
# Check docker-compose.yml volumes for airflow-webserver/scheduler
# These paths must match the target paths in your docker-compose.yml volume mounts
SCRAPER_CODE_PATH = '/opt/airflow/scraper_code' # Matches volume mount target
PROCESSOR_CODE_PATH = '/opt/airflow/processor_code' # Matches volume mount target

# Add paths to sys.path so Python can find your modules
if SCRAPER_CODE_PATH not in sys.path:
    sys.path.insert(0, SCRAPER_CODE_PATH)
if PROCESSOR_CODE_PATH not in sys.path:
    sys.path.insert(0, PROCESSOR_CODE_PATH)

# --- Import Scraper & Processor Modules BEFORE defining tasks that use them ---
# Handle potential import errors gracefully at DAG parsing time
def import_module_safe(module_name, module_path_hint=""):
    """Attempts to import a module, logging success or failure."""
    try:
        module = __import__(module_name)
        logger.info(f"Successfully imported module: {module_name}")
        return module
    except ImportError as e:
        logger.error(f"Failed to import module '{module_name}' from path '{module_path_hint}'. Error: {e}")
        # Depending on your setup, you might want to raise here or return None
        # Raising will prevent the DAG from being parsed successfully.
        raise # Re-raise to fail the DAG parsing if module is critical

# --- Import modules at DAG parsing time ---
# This ensures any import errors are caught early
try:
    scraper_module = import_module_safe('scraper', SCRAPER_CODE_PATH)
    processor_module = import_module_safe('processor', PROCESSOR_CODE_PATH)
except Exception as e:
    # If imports fail, log the error. The DAG will not be available.
    # This is the desired behavior if dependencies are missing.
    logger.error(f"Critical error importing modules at DAG parsing time: {e}")
    # Re-raise to prevent DAG creation
    raise


# --- Task Definitions ---
def run_scraper(**context):
    """Wrapper to call the scraper's main logic with error handling."""
    logger.info("Starting Scraper Task...")
    try:
        # Access context if needed (e.g., context['dag_run'].run_id, context['task_instance'])
        # Call the main function from scraper.py
        # Ensure the function name in scraper.py is 'main'
        if hasattr(scraper_module, 'main'):
            logger.info("Calling scraper.main()...")
            scraper_module.main() # Execute the scraping logic
            logger.info("Scraper task completed successfully.")
        else:
            error_msg = "Function 'main' not found in scraper.py"
            logger.error(error_msg)
            raise AttributeError(error_msg)
    except Exception as e:
        logger.error(f"Scraper task failed with exception: {e}", exc_info=True) # exc_info=True logs the full traceback
        raise # Re-raise to fail the Airflow task

def run_processor(**context):
    """Wrapper to call the processor's main logic with error handling."""
    logger.info("Starting Processor Task...")
    try:
        # Call the processing function from processor.py
        # Check for the most likely function name first
        if hasattr(processor_module, 'process_reviews'):
            logger.info("Calling processor.process_reviews()...")
            processor_module.process_reviews() # Execute the processing logic
            logger.info("Processor task completed successfully.")
        elif hasattr(processor_module, 'main'): # Fallback if function is named 'main'
             logger.info("Calling processor.main()...")
             processor_module.main() # Execute the processing logic
             logger.info("Processor task completed successfully.")
        else:
             error_msg = "Function 'process_reviews' or 'main' not found in processor.py"
             logger.error(error_msg)
             raise AttributeError(error_msg)
    except Exception as e:
        logger.error(f"Processor task failed with exception: {e}", exc_info=True)
        raise # Re-raise to fail the Airflow task

def validate_raw_data(**context):
    """Placeholder for raw data validation logic."""
    # Implement logic to connect to DB and run checks on RawAppReviews
    # Example checks:
    # - Count total records
    # - Check for NULLs in critical columns
    # - Validate Rating range (1-5)
    # - Check for duplicates
    # Raise an exception if critical thresholds are breached
    logger.info("Running validation on RawAppReviews data (Placeholder).")
    # For now, just log. Add actual validation logic later.
    # Example (conceptual):
    # import sqlite3
    # conn = sqlite3.connect('/app/database/reviews.db') # Path inside container
    # cursor = conn.cursor()
    # cursor.execute("SELECT COUNT(*) FROM RawAppReviews")
    # count = cursor.fetchone()[0]
    # if count < 100: # Example threshold
    #     raise ValueError(f"Raw data count {count} is below expected threshold.")
    # conn.close()
    # logger.info("Raw data validation passed.")

def validate_processed_data(**context):
    """Placeholder for processed data validation logic."""
    # Implement logic to connect to DB and run checks on ProcessedAppReviews
    # Example checks:
    # - Count total records
    # - Check for NULLs in critical columns (CleanedReviewText, VADER_Score)
    # - Validate VADER_Score range (-1 to 1)
    # - Check Sentiment label consistency
    # Raise an exception if critical thresholds are breached
    logger.info("Running validation on ProcessedAppReviews data (Placeholder).")
    # For now, just log. Add actual validation logic later.
    # Example (conceptual):
    # import sqlite3
    # conn = sqlite3.connect('/app/database/reviews.db') # Path inside container
    # cursor = conn.cursor()
    # cursor.execute("SELECT COUNT(*) FROM ProcessedAppReviews")
    # count = cursor.fetchone()[0]
    # if count < 50: # Example threshold
    #     raise ValueError(f"Processed data count {count} is below expected threshold.")
    # conn.close()
    # logger.info("Processed data validation passed.")


# --- Default Arguments (Professional Defaults) ---
default_args = {
    'owner': 'data_engineering_team', # More professional owner name
    'depends_on_past': False,
    'email': ['your_email@domain.com'], # Add your email for notifications (needs SMTP config)
    'email_on_failure': False, # Enable email on failure (set to True if SMTP configured)
    'email_on_retry': False,
    'retries': 2, # Retry twice before failing
    'retry_delay': timedelta(minutes=2), # Wait 2 mins between retries
    # 'queue': 'bash_queue', # If using specific queues
    # 'pool': 'backfill', # If using pools
    # ' sla': timedelta(hours=2), # Service Level Agreement (optional)
    # 'execution_timeout': timedelta(minutes=60), # Timeout for task execution (optional)
    # 'on_failure_callback': some_function, # Function to call on task failure (optional)
    # 'on_success_callback': some_other_function, # Function to call on task success (optional)
    # 'on_retry_callback': another_function, # Function to call on task retry (optional)
    # 'sla_miss_callback': yet_another_function, # Function to call if SLA is missed (optional)
    # 'trigger_rule': 'all_success' # Default trigger rule (optional)
}

# --- Define the DAG (Professional Definition) ---
dag = DAG(
    'playstore_review_etl_pipeline', # DAG ID - should match filename ideally
    default_args=default_args,
    description='Daily ETL pipeline: Scrapes, processes, and stores Play Store app reviews for analysis.',
    # schedule_interval=timedelta(days=1), # Schedule daily
    schedule_interval='@daily', # Schedule daily at midnight UTC (shorthand)
    start_date=datetime(2024, 1, 1), # Make sure this is in the past
    catchup=False, # Don't backfill past runs automatically
    tags=['playstore', 'ecommerce', 'nlp', 'etl', 'reviews'], # Tags for organization/filtering
    # max_active_runs=1, # Prevent multiple concurrent runs (good for resource-intensive DAGs)
)

# --- Define Tasks using PythonOperator (Professional) ---
scrape_task = PythonOperator(
    task_id='extract_raw_reviews',
    python_callable=run_scraper,
    dag=dag,
    doc_md="Extracts raw review data from the Google Play Store for configured apps and stores it in the raw data database (SQLite).",
    # execution_timeout=timedelta(minutes=30), # Set a timeout for the task (optional)
)

validate_raw_task = PythonOperator(
    task_id='validate_raw_reviews',
    python_callable=validate_raw_data,
    dag=dag,
    doc_md="Performs data quality checks on the raw reviews extracted by the scraper.",
)

process_task = PythonOperator(
    task_id='transform_clean_reviews',
    python_callable=run_processor,
    dag=dag,
    doc_md="Reads raw reviews from the database, cleans and preprocesses the text (NLP), calculates features (e.g., word count, sentiment placeholder), and stores the results in the processed data database.",
    # execution_timeout=timedelta(minutes=30), # Set a timeout for the task (optional)
)

validate_processed_task = PythonOperator(
    task_id='validate_processed_reviews',
    python_callable=validate_processed_data,
    dag=dag,
    doc_md="Performs data quality checks on the processed reviews generated by the processor.",
)

# --- Define Dependencies (The order of execution) ---
# The process task should only run after the scrape task finishes successfully.
# Validation tasks run after their respective data creation tasks.
scrape_task >> validate_raw_task >> process_task >> validate_processed_task

# --- Future Task Ideas (Conceptual) ---
# analyze_sentiment_task = PythonOperator(...)
# update_dashboard_data_task = PythonOperator(...)
# scrape_task >> validate_raw_task >> process_task >> validate_processed_task >> analyze_sentiment_task >> update_dashboard_data_task
