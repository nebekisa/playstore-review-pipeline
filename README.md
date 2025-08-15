# 🛍️ E-commerce App Review Analysis Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Apache%20Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-orange.svg)](https://airflow.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28.0-red.svg)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-20.10.0%2B-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

This project implements a complete **end-to-end data engineering pipeline** designed to automatically scrape, process, store, and visualize customer reviews for popular e-commerce applications (AliExpress, Alibaba, Jiji) from the Google Play Store. It leverages modern tools like Docker, Apache Airflow, SQLite, and Streamlit to create a robust, automated, and interactive system for gaining insights into user sentiment and behavior.

## 📌 Project Overview

The goal of this project is to extract, analyze, and visualize customer sentiment and insights from Google Play Store reviews of e-commerce apps. This pipeline addresses common data science challenges like web scraping, data cleaning, natural language processing (NLP), statistical modeling, and creating interactive dashboards, providing a comprehensive solution from raw data to actionable insights.

This project was developed as part of a Data Science & Engineering curriculum challenge, fulfilling the requirements outlined in `sample Challenge.pdf` and `Data Science Project Guidline.pdf`.

## 🔧 Features & Architecture

*   **Automated Data Ingestion:** Scrapes reviews from the Google Play Store for specified apps on a schedule.
*   **Data Storage:** Uses SQLite for lightweight, file-based storage of raw and processed review data.
*   **Data Cleaning & Preprocessing:** Applies NLP techniques (tokenization, stopword removal, lemmatization) to clean review text.
*   **Sentiment Analysis:** Employs VADER (Valence Aware Dictionary and sEntiment Reasoner) for rule-based sentiment classification of reviews.
*   **Orchestration:** Apache Airflow manages and schedules the entire ETL (Extract, Transform, Load) workflow.
*   **Containerization:** Docker & Docker Compose ensure consistent environments and easy deployment.
*   **Interactive Dashboard:** A Streamlit web application provides an intuitive interface to explore key findings, visualize trends, and view example reviews.
*   **Professional Practices:** Incorporates error handling, logging, data quality checks, and modular code structure.

## 🛠️ Technologies Used

*   **Languages:** Python
*   **Libraries & Frameworks:**
    *   `google-play-scraper`: For scraping Google Play Store reviews.
    *   `pandas`, `numpy`: For data manipulation and analysis.
    *   `sqlite3`: For database interaction.
    *   `nltk`, `beautifulsoup4`, `contractions`, `vaderSentiment`: For NLP tasks (cleaning, sentiment analysis).
    *   `matplotlib`, `seaborn`, `plotly`, `wordcloud`: For static and interactive data visualizations.
    *   `streamlit`: For building the interactive web dashboard.
    *   `apache-airflow`: For workflow orchestration.
*   **Tools & Platforms:**
    *   Docker & Docker Compose: For containerization and environment management.
    *   Git & GitHub: For version control.
    *   Visual Studio Code / Jupyter Notebook: For development.
*   **Databases:** SQLite

## 📁 Project Structure
profile
Qwen3-Coder 12:54 am

Okay, based on your project's current state (Airflow ETL pipeline, Streamlit dashboard, SQLite database) and the documents provided (sample Challenge.pdf, Data Science Project Guidline.pdf), here's a professional README.md file for your GitHub repository. 
 
README.md 
markdown
 
 
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
⌄
⌄
⌄
⌄
⌄
⌄
⌄
# 🛍️ E-commerce App Review Analysis Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Apache%20Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-orange.svg)](https://airflow.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28.0-red.svg)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-20.10.0%2B-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

This project implements a complete **end-to-end data engineering pipeline** designed to automatically scrape, process, store, and visualize customer reviews for popular e-commerce applications (AliExpress, Alibaba, Jiji) from the Google Play Store. It leverages modern tools like Docker, Apache Airflow, SQLite, and Streamlit to create a robust, automated, and interactive system for gaining insights into user sentiment and behavior.

## 📌 Project Overview

The goal of this project is to extract, analyze, and visualize customer sentiment and insights from Google Play Store reviews of e-commerce apps. This pipeline addresses common data science challenges like web scraping, data cleaning, natural language processing (NLP), statistical modeling, and creating interactive dashboards, providing a comprehensive solution from raw data to actionable insights.

This project was developed as part of a Data Science & Engineering curriculum challenge, fulfilling the requirements outlined in `sample Challenge.pdf` and `Data Science Project Guidline.pdf`.

## 🔧 Features & Architecture

*   **Automated Data Ingestion:** Scrapes reviews from the Google Play Store for specified apps on a schedule.
*   **Data Storage:** Uses SQLite for lightweight, file-based storage of raw and processed review data.
*   **Data Cleaning & Preprocessing:** Applies NLP techniques (tokenization, stopword removal, lemmatization) to clean review text.
*   **Sentiment Analysis:** Employs VADER (Valence Aware Dictionary and sEntiment Reasoner) for rule-based sentiment classification of reviews.
*   **Orchestration:** Apache Airflow manages and schedules the entire ETL (Extract, Transform, Load) workflow.
*   **Interactive Dashboard:** A Streamlit web application provides an intuitive interface to explore key findings, visualize trends, and view example reviews.
*   **Professional Practices:** Incorporates error handling, logging, data quality checks, and modular code structure.

## 🛠️ Technologies Used

*   **Languages:** Python
*   **Libraries & Frameworks:**
    *   `google-play-scraper`: For scraping Google Play Store reviews.
    *   `pandas`, `numpy`: For data manipulation and analysis.
    *   `sqlite3`: For database interaction.
    *   `nltk`, `beautifulsoup4`, `contractions`, `vaderSentiment`: For NLP tasks (cleaning, sentiment analysis).
    *   `matplotlib`, `seaborn`, `plotly`, `wordcloud`: For static and interactive data visualizations.
    *   `streamlit`: For building the interactive web dashboard.
    *   `apache-airflow`: For workflow orchestration.
*   **Tools & Platforms:**
    *   Docker & Docker Compose: For containerization and environment management.
    *   Git & GitHub: For version control.
    *   Visual Studio Code / Jupyter Notebook: For development.
*   **Databases:** SQLite

## 📁 Project Structure

 
 

playstore_review_pipeline/
│
├── dags/                          # Airflow DAG definitions
│   └── playstore_review_etl_pipeline_dag.py # Main ETL workflow definition
│
├── scraper/                       # Data Ingestion (Scraper) component
│   ├── scraper.py                 # Core scraping logic
│   └── requirements.txt           # Dependencies for the scraper
│
├── processor/                     # Data Processing (NLP/Cleaning) component
│   ├── processor.py               # Core processing logic
│   └── requirements.txt           # Dependencies for the processor
│
├── dashboard/                     # Data Visualization (Streamlit Dashboard) component
│   └── app.py                     # Streamlit dashboard application code
│
├── sqlite_data/                   # (Docker Volume) Persistent storage for SQLite database
│   └── reviews.db                 # The SQLite database file (created by pipeline)
│
├── logs/                          # (Docker Volume) Airflow logs
│
├── docker-compose.yml             # Defines and runs multi-container Docker applications
├── Dockerfile.airflow             # (Optional) Custom Airflow image definition
├── airflow_requirements.txt       # (Optional) Dependencies for Airflow tasks
├── README.md                      # This file
└── requirements.txt 
## ▶️ Getting Started

These instructions will get you a copy of the project up and running on your local machine.

### Prerequisites

*   Docker Desktop installed and running ([https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop))
*   Git installed ([https://git-scm.com/](https://git-scm.com/))

### Installation & Running the Pipeline

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/your-github-username/playstore-review-pipeline.git
    cd playstore-review-pipeline
    ```
2.  **(Optional) Build Custom Airflow Image:**
    *   If you have modified `Dockerfile.airflow` or `airflow_requirements.txt`:
        ```bash
        # Build the custom Airflow image
        docker-compose build airflow-webserver airflow-scheduler airflow-init
        ```
3.  **Start the Services:**
    ```bash
    # Start all services defined in docker-compose.yml
    docker-compose up
    # Or run in detached mode:
    # docker-compose up -d
    ```
4.  **Access Airflow UI:**
    *   Open your web browser and go to `http://localhost:8081`.
    *   Log in with the default credentials (Username: `admin`, Password: `admin`). **Change these credentials in production.**
    *   You should see the `playstore_review_etl_pipeline` DAG listed.
    *   Toggle the DAG `On`.
    *   Click the `Play` button (▶️) to trigger a manual run.
5.  **Monitor Pipeline Execution:**
    *   Watch the DAG run progress in the Airflow UI Graph or Tree view.
    *   Check task logs for details on scraping and processing.
6.  **Access the Streamlit Dashboard:**
    *   (If included as a service in `docker-compose.yml`) Open your web browser and go to `http://localhost:8501`.
    *   (If run separately) Navigate to the `dashboard/` directory and run `streamlit run app.py`. Access via `http://localhost:8501`.

## 📊 Dashboard Features

The interactive Streamlit dashboard allows users to explore insights for each app:

*   **App Selection:** Filter data by choosing AliExpress, Alibaba, or Jiji.
*   **Overview Statistics:** View total reviews, average rating, and positive sentiment percentage.
*   **Rating Distribution:** Interactive chart showing the breakdown of 1-5 star ratings.
*   **Sentiment Distribution:** Pie chart displaying the proportion of Positive, Negative, and Neutral reviews.
*   **Sentiment Trend:** Line chart showing how sentiment has changed over time (monthly).
*   **Top Words/Phrases:** Tabs for visualizing the most frequent single words (Unigrams) and two-word phrases (Bigrams), plus a Word Cloud.
*   **Example Reviews:** Tabs to view sample reviews categorized by sentiment (Positive, Negative, Neutral).

## 📈 Results & Insights (Brief)

*   **Review Volume & Trends:** Observed significant review activity surges, particularly for AliExpress in late 2025.
*   **Rating Patterns:** Jiji generally exhibited the highest average rating, while AliExpress showed more polarized ratings (many 1-star and 5-star reviews).
*   **Sentiment Analysis (VADER):** Jiji had the highest proportion of positive sentiment. AliExpress showed strong positive sentiment but also a notable number of negative reviews.
*   **Common Topics:** Identified key themes like 'price', 'shipping', 'product', 'app', and 'service' across reviews.
*   **Review Characteristics:** Found correlations between review length, rating, and sentiment (e.g., negative reviews often being longer).

## 🤝 Contributing
