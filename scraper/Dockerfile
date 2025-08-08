# scraper/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scraper code
COPY scraper.py .

# Command to run the scraper when the container starts
# The script will use environment variables for configuration
CMD ["python", "scraper.py"]