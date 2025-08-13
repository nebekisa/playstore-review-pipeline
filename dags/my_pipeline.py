# dags/playstore_pipeline_dag.py (Professional Version)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.models import Variable # For Airflow Variables (optional)
import sys
import os
import logging # Use Python's standard logging

# --- Configure Logger for this DAG ---
logger = logging.getLogger(__name__) # Gets the logger named after this module (__main__ in DAG context)

# --- IMPORTANT: Ensure Airflow can find your modules ---
# Paths INSIDE the Airflow container where volumes are mounted
# Check docker-compose.yml volumes for airflow-webserver/scheduler
SCRAPER_CODE_PATH = '/opt/airflow/scraper_code'
PROCESSOR_CODE_PATH = '/opt/airflow/processor_code'

if SCRAPER_CODE_PATH not in sys.path:
    sys.path.insert(0, SCRAPER_CODE_PATH)
if PROCESSOR_CODE_PATH not in sys.path:
    sys.path.insert(0, PROCESSOR_CODE_PATH)

# --- Import Scraper & Processor Modules ---
# Handle potential import errors gracefully
def import_module_safe(module_name, module_path_hint=""):
    try:
        module = __import__(module_name)
        logger.info(f"Successfully imported module: {module_name}")
        return module
    except ImportError as e:
        logger.error(f"Failed to import module '{module_name}'. Check if the file exists at '{module_path_hint}' and is correctly named. Error: {e}")
        raise # Re-raise to fail the task

scraper_module = import_module_safe('scraper', SCRAPER_CODE_PATH)
processor_module = import_module_safe('processor', PROCESSOR_CODE_PATH)

# --- Task Definitions ---
def run_scraper(**context):
    """Wrapper to call the scraper's main logic with error handling."""
    logger.info("Starting Scraper Task...")
    try:
        # Access context if needed (e.g., context['dag_run'].run_id, context['task_instance'])
        # Call the main function from scraper.py
        if hasattr(scraper_module, 'main'):
            logger.info("Calling scraper.main()...")
            scraper_module.main()
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
        if hasattr(processor_module, 'process_reviews'):
            logger.info("Calling processor.process_reviews()...")
            processor_module.process_reviews()
            logger.info("Processor task completed successfully.")
        elif hasattr(processor_module, 'main'): # Fallback if function is named 'main'
             logger.info("Calling processor.main()...")
             processor_module.main()
             logger.info("Processor task completed successfully.")
        else:
             error_msg = "Function 'process_reviews' or 'main' not found in processor.py"
             logger.error(error_msg)
             raise AttributeError(error_msg)
    except Exception as e:
        logger.error(f"Processor task failed with exception: {e}", exc_info=True)
        raise # Re-raise to fail the Airflow task

# --- Default Arguments (Professional Defaults) ---
default_args = {
    'owner': 'data_engineering_team', # More professional owner name
    'depends_on_past': False,
    'email': ['your_email@domain.com'], # Add your email for notifications (needs SMTP config)
    'email_on_failure': True, # Enable email on failure
    'email_on_retry': False,
    'retries': 2, # Retry twice before failing
    'retry_delay': timedelta(minutes=2), # Wait 2 mins between retries
    # 'queue': 'bash_queue', # If using specific queues
    # 'pool': 'backfill', # If using pools
}
# --- Define the DAG (Professional Definition) ---
dag = DAG(
    'playstore_review_etl_pipeline', # More descriptive DAG ID
    default_args=default_args,
    description='Daily ETL pipeline: Scrapes, processes, and stores Play Store app reviews for analysis.',
    schedule_interval='@daily', # Schedule daily at midnight UTC (or use timedelta(days=1))
    # schedule_interval=timedelta(days=1), # Alternative way
    start_date=datetime(2024, 1, 1), # Make sure this is in the past
    catchup=False, # Don't backfill past runs automatically
    tags=['playstore', 'ecommerce', 'nlp', 'etl', 'reviews'], # Tags for organization/filtering
    # max_active_runs=1, # Prevent multiple concurrent runs (good for resource-intensive DAGs)
    # concurrency=1, # Limit concurrent task instances (if needed)
)

# --- Define Tasks using PythonOperator (Professional) ---
scrape_task = PythonOperator(
    task_id='extract_raw_reviews',
    python_callable=run_scraper,
    dag=dag,
    doc_md="Extracts raw review data from the Google Play Store for configured apps and stores it in the raw data database (SQLite).",
    # execution_timeout=timedelta(minutes=30), # Set a timeout for the task
)

process_task = PythonOperator(
    task_id='transform_clean_reviews',
    python_callable=run_processor,
    dag=dag,
    doc_md="Reads raw reviews from the database, cleans and preprocesses the text (NLP), calculates features (e.g., word count, sentiment placeholder), and stores the results in the processed data database.",
    # execution_timeout=timedelta(minutes=30),
)

# --- Define Dependencies (The order of execution) ---
scrape_task >> process_task # Process only after scrape succeeds

# --- Future Task Ideas (Conceptual) ---
# analyze_sentiment_task = PythonOperator(...)
# update_dashboard_data_task = PythonOperator(...)
# scrape_task >> process_task >> analyze_sentiment_task >> update_dashboard_data_task

