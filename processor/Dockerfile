# processor/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# NLTK Data: Download required packages during image build
# This ensures they are available when the container runs
# Adjust the list based on what your processor actually uses
RUN python -c "import nltk; nltk.download('punkt'); nltk.download('punkt_tab'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('omw-1.4'); nltk.download('vader_lexicon'); print('NLTK data downloaded.')"

# Copy the processor code
COPY processor.py .

# Command to run the processor
CMD ["python", "processor.py"]
# Dockerfile.airflow
# Use the official Airflow image as the base
FROM apache/airflow:2.9.1

# Switch to root user to install system dependencies (if needed)
# USER root

# Install system dependencies if required (e.g., build-essential for compiling some Python packages)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
# USER airflow

# Copy the requirements file for Airflow worker tasks
# This file lists the dependencies needed by your DAG tasks
COPY airflow_requirements.txt /tmp/airflow_requirements.txt

# Install the Python dependencies using pip
# Use `--user` to install in the user directory, respecting the airflow user context
# Use `--no-cache-dir` to reduce image size
# Use `--disable-pip-version-check` to avoid warnings
RUN pip install --no-cache-dir --disable-pip-version-check \
    -r /tmp/airflow_requirements.txt

# --- Optional: Install NLTK Data ---
# If your processor needs specific NLTK data, install it during image build
# This avoids downloading it every time a task runs, which can fail or be slow.
# RUN python -c "import nltk; nltk.download('punkt'); nltk.download('punkt_tab'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('omw-1.4'); nltk.download('vader_lexicon'); print('NLTK data downloaded.')"

# --- Optional: Copy your code into the image (Alternative approach) ---
# Instead of mounting volumes, you could copy code directly.
# This makes the image self-contained but requires rebuilding the image
# whenever the code changes.
# COPY ./scraper /opt/airflow/scraper_code
# COPY ./processor /opt/airflow/processor_code
# COPY ./dags /opt/airflow/dags

