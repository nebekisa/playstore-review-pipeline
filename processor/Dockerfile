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