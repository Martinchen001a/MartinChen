# BlueAlpha Data Pipeline: Airflow & dbt Orchestration

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
# Clone the repository
git clone https://github.com/Martinchen001a/DE_BlueAlpha
cd DE_BlueAlpha
```

### 2. Launch the Entire Stack

This command builds the custom Airflow image (including dbt and dependencies) and starts all services.

```bash
docker-compose up -d --build

```

### 3. Access Services

Airflow UI: http://localhost:8081 (Login: admin / admin)

pgAdmin: http://localhost:8080 (Login: admin@admin.com / password)

Postgres: localhost:5433 (External access)



---

## 📂 Project Structure & Logic

* **`dags/`**: Contains Airflow DAGs with dynamic path detection logic, allowing the project to run regardless of the local file system path.
* **`data_ingestion/`**: Contains `ingest.py` which loads and cleans raw data from `data/` into Postgres `stg_` tables using SQLAlchemy and the connection string `postgresql://postgres:password@localhost:5433/postgres`.
* **`dbt_project/`**: The dbt project responsible for transforming raw staged data into analytics-ready models using the Postgres profile.
* **`data/`**: Storage for raw `crm_revenue.csv`, `facebook_export.csv`, and `google_ads_api.json`.

---

## 💡 Key Features

* **Zero Install**: No local Python or Airflow setup required; Docker handles all dependencies.
* **Robust Ingestion**: The Python ingestion script handles inconsistent date formats and standardizes column naming conventions (lowercase, snake_case, trim whitespace) automatically using regex and pandas.
* **Automated dbt Workflow**: The DAG automatically installs dbt packages and builds models within the container.
* **Persistent Storage**: Database data is preserved in Docker volumes, while logs and dags are live-synced from your local folder.


