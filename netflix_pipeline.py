import os
import logging
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# -------- LOAD PATHS FROM .sourcepaths --------
load_dotenv(dotenv_path=".sourcepaths")
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")

# -------- CONSTANTS --------
PROJECT_ID = "netflix-titles-463907"
DATASET = "netflix_db"
TABLE_RAW = f"{PROJECT_ID}.{DATASET}.netflix_raw"
TABLE_CLEANED = f"{PROJECT_ID}.{DATASET}.netflix_cleaned"
LOG_PATH = os.path.join(OUTPUT_DIR, "netflix_pipeline.log")

# -------- LOGGING SETUP --------
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------- GCP CLIENT SETUP --------
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
client = bigquery.Client(project=PROJECT_ID)

# -------- FUNCTIONS --------
def load_raw_data():
    logging.info("Loading raw data from BigQuery")
    query = f"SELECT * FROM `{TABLE_RAW}`"
    df = client.query(query).to_dataframe()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    logging.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")
    return df

def clean_data(df):
    logging.info("Cleaning data")
    df_cleaned = df.copy()
    df_cleaned['country'] = df_cleaned['country'].fillna("Unknown")
    df_cleaned['rating'] = df_cleaned['rating'].fillna("Unrated")
    df_cleaned['title'] = df_cleaned['title'].str.strip()
    df_cleaned['type'] = df_cleaned['type'].str.strip()
    df_cleaned['release_year'] = df_cleaned['release_year'].astype(int)
    df_cleaned = df_cleaned[df_cleaned['title'].notna()]
    df_cleaned = df_cleaned[df_cleaned['release_year'].notna()]
    logging.info(f"Cleaned data: {df_cleaned.shape}")
    return df_cleaned

def load_to_bigquery(df):
    ver_suffix = datetime.today().strftime("%Y_%m_%d")
    table_name = f"{TABLE_CLEANED}_{ver_suffix}"
    logging.info(f"Uploading cleaned data to BigQuery table: {table_name}")
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_dataframe(df, table_name, job_config=job_config)
    job.result()
    logging.info("Data uploaded to BigQuery successfully")

# -------- MAIN --------
if __name__ == "__main__":
    try:
        start = time.time()
        raw_df = load_raw_data()
        cleaned_df = clean_data(raw_df)
        load_to_bigquery(cleaned_df)
        logging.info("Pipeline completed successfully")
        end = time.time()
        logging.info(f"Pipeline completed in {end - start:.2f} seconds")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
