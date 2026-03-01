#Week 3

This project implements an end-to-end data pipeline reading from CSV, JSON, and Excel sources, clearing/standardizing the data, and loading it into an internal OLTP database before transforming it into a Data Warehouse Star Schema.

## Option Chosen: Option B
- **Database**: SQLite (built-in Python sqlite3)
- **Orchestration**: Apache Airflow
- **Partitioning**: Simulated in SQLite (monthly views over separate tables)

## Project Structure
```text
.
├── .gitignore
├── README.md
├── requirements.txt
├── data/
│   ├── customers.csv
│   ├── orders (1).json
│   └── payments.xlsx
├── dags/
│   └── assignment_week_3_dag.py
└── tests/
    └── test_cleaning.py
```

## Setup Instructions
The steps outline how to deploy this pipeline on a local machine using a Python virtual environment.

### 1. Requirements
- Python 3.9, 3.10, or 3.11 installed.

### 2. Virtual Environment and Dependencies
Create a Virtual Environment and install the Python dependencies:
```bash
python -m venv .venv
# Activate the environment on Windows
.venv\Scripts\activate
# Install the requirements
pip install -r requirements.txt
```

### 3. Initialize Airflow
We'll run Airflow standalone for local development. First, set the `AIRFLOW_HOME` to your project directory.
```bash
# On Windows PowerShell
$env:AIRFLOW_HOME = $PWD

airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 4. Running the Pipeline
Run the Airflow webserver and scheduler:
```bash
airflow standalone
```

Visit the Airflow UI at `http://localhost:8080`, unpause the `week3_data_pipeline` DAG, and trigger it to start the execution. Once the DAG completes successfully, a new SQLite database file `assignment_week3.db` will be populated in your `data/` directory.

### 5. Running Tests
To run quality gates / tests:
```bash
pytest tests/
```
