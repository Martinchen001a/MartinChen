# Marketing Data Pipeline: Airflow & dbt Orchestration

This project implements an end-to-end data engineering pipeline. It orchestrates the ingestion of multi-source marketing data (CSV/JSON) into a PostgreSQL database and manages data transformations using dbt.

## 🛠 Tech Stack
- **Orchestration**: Apache Airflow 2.x
- **Transformation**: dbt (data build tool)
- **Database**: PostgreSQL 16.4 (Containerized)
- **Data Processing**: Python 3.8+ (Pandas, SQLAlchemy)
- **Containerization**: Docker & Docker Compose

---

## 🚀 Setup Instructions

### 1. Environment Initialization
Clone the repository and set up a clean Python virtual environment:
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required dependencies
pip install --upgrade pip
pip install -r requirements.txt

```

### 2. Launch Database Service

The project relies on PostgreSQL. Use Docker Compose to spin up the database and pgAdmin:

```bash
docker-compose up -d

```

* **PostgreSQL**: Accessible at `localhost:5433` (mapped from container port 5432).
* **pgAdmin**: Accessible at `http://localhost:8080` (Login: `admin@admin.com` / Password: `password`).

### 3. Airflow Configuration

We use the project root as `AIRFLOW_HOME` to ensure environment isolation and portability:

```bash
# Set Airflow home to current directory
export AIRFLOW_HOME=$(pwd)

# Initialize Airflow metadata database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Martin \
    --lastname Chen \
    --role Admin \
    --email martin@example.com

```

### 4. Running the Pipeline

Open two separate terminal tabs. **Note**: You must run `export AIRFLOW_HOME=$(pwd)` in every new session.

* **Terminal 1 (Webserver)**:
```bash
airflow webserver --port 8081

```


* **Terminal 2 (Scheduler)**:
```bash
airflow scheduler

```



Once both services are running, visit `http://localhost:8081`, log in with `admin/admin`, and trigger the DAG: `dbt_project_orchestration_v5`.

---

## 📂 Project Structure & Logic

* **`dags/`**: Contains Airflow DAGs with dynamic path detection logic, allowing the project to run regardless of the local file system path.
* **`data_ingestion/`**: Contains `ingest.py` which cleans and loads raw data from `data/` into Postgres `stg_` tables using SQLAlchemy and the connection string `postgresql://postgres:password@localhost:5433/postgres`.
* **`dbt_project/`**: The dbt project responsible for transforming raw staged data into analytics-ready models using the Postgres profile.
* **`data/`**: Storage for raw `crm_revenue.csv`, `facebook_export.csv`, and `google_ads_api.json`.

---

## 💡 Key Features

* **Environment Agnostic**: The pipeline uses `PROJECT_ROOT` dynamic pathing, eliminating "File Not Found" errors across different machines.
* **Robust Ingestion**: The Python ingestion script handles inconsistent date formats and standardizes column naming conventions (lowercase, snake_case) automatically using regex and pandas.
* **Isolated Metadata**: By locking `AIRFLOW_HOME` to the project folder, all logs and local db files are kept separate from your system-wide Airflow configuration.
* **Containerized Database**: Uses Docker to manage PostgreSQL 16.4, ensuring a consistent database environment for all users.


