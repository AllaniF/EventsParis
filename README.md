# EventsParis

## Description
Project to extract events in Paris, store them in a Data Lake (MongoDB), and transform/load them into a Data Warehouse (PostgreSQL).

## Architecture
- **Source**: OpenData Paris API
- **Data Lake**: MongoDB
- **Data Warehouse**: PostgreSQL
- **Orchestration**: Apache Airflow
- **Visualization**: Grafana

## Setup
1. Start the containers:
   ```bash
   docker-compose up -d
   ```

2. Install dependencies (if running locally):
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### 1. Extract Data (ETL - Extract)
Extracts data from the API and saves it to MongoDB.
```bash
python src/extract.py
```

### 2. Transform & Load (ETL - Transform & Load)
Reads data from MongoDB, creates the Star Schema in PostgreSQL, and loads the data.
```bash
python src/transform_load.py
```
