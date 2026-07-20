# Airflow

Personal repository for Apache Airflow pipelines (DAGs) — data engineering experiments, ETL jobs, and automation projects.

## About

This repo centralizes DAGs and supporting modules used to orchestrate personal data pipelines: extraction, transformation, and loading across SQL Server, APIs, and other tools.

## Structure

```
.
├── dags/               # DAG definitions
├── plugins/            # Custom operators, hooks, and utilities
├── include/            # SQL scripts, configs, and helper files
├── docker-compose.yaml # Local Airflow environment
├── requirements.txt    # Python dependencies
└── README.md
```

## Requirements

- Docker / Docker Compose (or Colima on macOS)
- Apache Airflow 3.x
- Python 3.11+

## Running locally

```bash
# Start the environment
docker compose up -d

# Access the Airflow UI
# http://localhost:8080
```

## Conventions

- Each DAG should have a unique, descriptive `dag_id` (e.g. `spar_agregado_sync`, `ans_matching_daily`).
- Sensitive variables and credentials should be configured via **Airflow Variables/Connections**, never hardcoded.
- Tasks requiring isolated Python dependencies should use `ExternalPythonOperator` with `uv`-managed environments.
