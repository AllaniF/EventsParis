# EventsParis

## Description
EventsParis is a comprehensive data engineering project designed to extract, transform, and visualize events data from Paris. 

## Architecture
The project follows a robust ETL (Extract, Transform, Load) architecture:
- **Source**: OpenData Paris API
- **Data Lake**: MongoDB (Stores raw JSON data)
- **Data Warehouse**: PostgreSQL (Stores structured data in a Star Schema)
- **Orchestration**: Apache Airflow (Manages workflows and scheduling)
- **Message Broker**: Apache Kafka & Zookeeper
- **Monitoring**: Prometheus
- **Visualization**: Grafana

## Prerequisites
Before you begin, ensure you have the following installed on your machine:
- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

## Installation & Setup

### 1. Clone the Project
Start by cloning the repository to your local machine:
```bash
git clone https://github.com/AllaniF/EventsParis.git_url
cd EventsParis
```

### 2. Environment Setup
The project uses Docker Compose to manage all services. No manual local installation of Python dependencies is strictly required if running within Docker, but `requirements.txt` is provided for local development.

## Running the Project

### 1. Start the Services
Launch all the containers (Airflow, Postgres, MongoDB, Kafka, Prometheus, Grafana) using Docker Compose:

```bash
docker-compose up -d
```

This command will start the following services:
- **PostgreSQL**: Port 5433 (mapped to 5432 inside container)
- **MongoDB**: Port 27017
- **Airflow Webserver**: Port 8080
- **Airflow Scheduler**: Background service
- **Kafka**: Port 9092
- **Zookeeper**: Port 2181
- **Prometheus**: Port 9090
- **Grafana**: Port 3000

### 2. Verify Services
Check if all containers are running:
```bash
docker-compose ps
```

## Accessing the Interfaces

- **Apache Airflow**: [http://localhost:8080](http://localhost:8080)
  - **Username**: `admin`
  - **Password**: `admin`
- **Grafana**: [http://localhost:3000](http://localhost:3000)
  - **Username**: `admin`
  - **Password**: `admin`
- **Prometheus**: [http://localhost:9090](http://localhost:9090)

## Project Structure

```
EventsParis/
├── airflow/                # Airflow configuration and DAGs
│   ├── dags/               # Directed Acyclic Graphs
│   ├── logs/               # Airflow logs
│   └── plugins/            # Airflow plugins
├── sql/                    # SQL scripts
│   └── init_dw.sql         # Data Warehouse initialization script
├── src/                    # Source code for ETL scripts
│   ├── extract.py          # Script to extract data from API to MongoDB
│   └── transform_load.py   # Script to transform and load data to PostgreSQL
├── tests/
│   └── verify_mongo_connection.py # Utility to check MongoDB connection
├── prometheus/             # Prometheus configuration
├── grafana/                # Grafana provisioning
├── docker-compose.yml      # Docker services configuration
├── Dockerfile              # Custom Docker image definition
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

## Usage

### Automated Workflow (Airflow)
The primary way to run the ETL process is through Apache Airflow.
1. Access the Airflow UI at [http://localhost:8080](http://localhost:8080).
2. Login with `admin` / `admin`.
3. Trigger the DAG (e.g., `extract_events_dag`) to start the pipeline.

### Manual Execution (Local)
If you wish to run the scripts manually (ensure dependencies are installed via `pip install -r requirements.txt`):

**1. Extract Data**
Fetch data from the OpenData Paris API and store it in MongoDB:
```bash
python src/extract.py
```

**2. Transform & Load**
Process data from MongoDB and load it into PostgreSQL:
```bash
python src/transform_load.py
```

## Database Credentials

- **PostgreSQL**:
  - User: `airflow`
  - Password: `airflow`
  - Database: `airflow`
  - Port: `5433` (Host), `5432` (Container)

- **MongoDB**:
  - User: `admin`
  - Password: `password`
  - Port: `27017`


## Contributors
    - Fadia ALLANI
    - Angie Pineda 