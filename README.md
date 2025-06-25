# Netflix Data Engineering Pipeline

This project demonstrates a real-world cloud ETL pipeline built using Python and Google BigQuery.
It reads raw Netflix titles data from BigQuery, applies cleaning transformations using pandas, and writes the cleaned data back to a new BigQuery table.
The entire pipeline is cloud-native — no local file processing, CSVs, or downloads. Logging and environment-based configuration ensure it's production-ready and modular.

---

## Features

- Extracts raw data from BigQuery (`netflix_raw` table)
- Cleans and transforms the data using Pandas
- Loads the cleaned data back into BigQuery (`netflix_cleaned` table)
- Uses proper logging and modular functions
- Secure path config via `.sourcepaths`

---

## Project Structure

netflix-explorer/
│
├── netflix_pipeline.py # Main pipeline script
├── requirements.txt # Python dependencies
├── .sourcepaths # Local path configs (not committed)
├── .gitignore # Ignores sensitive and output files
├── README.md # You're reading it!
│
├── output/ # Generated files and logs
│ └── netflix_pipeline.log

---

## Setup Instructions

### 1. Clone the repo and create `.sourcepaths` file

CREDENTIALS_PATH=your_full_path/netflix-titles.json
OUTPUT_DIR=your_output_folder_path

---

### 2. Install requirements

```bash
pip install -r requirements.txt

