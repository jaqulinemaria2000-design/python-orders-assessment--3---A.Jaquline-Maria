"""
pipeline_dag.py - Apache Airflow DAG for the data pipeline.

Orchestrates the full ETL pipeline:
  Task 1: ingest_data    → Read CSV, JSON, Excel files
  Task 2: clean_data     → Apply quality rules and standardize
  Task 3: load_oltp      → Load into normalized OLTP schema
  Task 4: transform_dw   → Build star schema data warehouse
  Task 5: quality_check  → Run quality verification

Dependencies: ingest → clean → load_oltp → transform_dw → quality_check
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─── Project path setup ───────────────────────
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)
sys.path.insert(0, PROJECT_ROOT)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OLTP_DB = os.path.join(PROJECT_ROOT, "db", "oltp.db")
DW_DB = os.path.join(PROJECT_ROOT, "db", "warehouse.db")


# ─── Task Functions ───────────────────────────

def task_ingest(**kwargs):
    """Task 1: Ingest data from all source files."""
    from src.ingestion import ingest_all
    data = ingest_all(DATA_DIR)
    # Store row counts for downstream verification
    kwargs["ti"].xcom_push(key="raw_counts", value={
        k: len(v) for k, v in data.items()
    })
    # Serialize DataFrames to JSON for XCom
    serialized = {}
    for key, df in data.items():
        serialized[key] = df.to_json(date_format="iso")
    kwargs["ti"].xcom_push(key="raw_data", value=serialized)
    return "Ingestion complete"


def task_clean(**kwargs):
    """Task 2: Clean and standardize all data."""
    import pandas as pd
    from src.cleaning import clean_all

    # Pull raw data from XCom
    ti = kwargs["ti"]
    serialized = ti.xcom_pull(task_ids="ingest_data", key="raw_data")
    data = {}
    for key, json_str in serialized.items():
        data[key] = pd.read_json(json_str)

    clean_data = clean_all(data)

    # Push cleaned data
    serialized_clean = {}
    for key, df in clean_data.items():
        serialized_clean[key] = df.to_json(date_format="iso")
    ti.xcom_push(key="clean_data", value=serialized_clean)

    ti.xcom_push(key="clean_counts", value={
        k: len(v) for k, v in clean_data.items()
    })
    return "Cleaning complete"


def task_load_oltp(**kwargs):
    """Task 3: Load cleaned data into OLTP database."""
    import pandas as pd
    from src.oltp import load_to_oltp

    ti = kwargs["ti"]
    serialized = ti.xcom_pull(task_ids="clean_data", key="clean_data")
    data = {}
    for key, json_str in serialized.items():
        data[key] = pd.read_json(json_str)

    load_to_oltp(data, OLTP_DB)
    return "OLTP load complete"


def task_transform_dw(**kwargs):
    """Task 4: Transform OLTP into star schema data warehouse."""
    from src.data_warehouse import transform_to_dw
    transform_to_dw(OLTP_DB, DW_DB)
    return "DW transformation complete"


def task_quality_check(**kwargs):
    """Task 5: Run quality verification on the final databases."""
    from sqlalchemy import create_engine, text

    # Verify OLTP
    oltp_engine = create_engine(f"sqlite:///{OLTP_DB}")
    with oltp_engine.connect() as conn:
        for table in ["customers", "orders", "order_items", "payments"]:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"OLTP {table}: {count} rows")
            assert count > 0, f"OLTP table {table} is empty!"

    # Verify DW
    dw_engine = create_engine(f"sqlite:///{DW_DB}")
    with dw_engine.connect() as conn:
        for table in ["dim_customer", "dim_date", "dim_payment_method"]:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"DW {table}: {count} rows")
            assert count > 0, f"DW table {table} is empty!"

        # Verify partitioned fact tables
        view_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders_all")).scalar()
        print(f"DW fact_orders_all (view): {view_count} rows")
        assert view_count > 0, "No data in fact_orders_all view!"

    print("✅ All quality checks passed!")
    return "Quality checks passed"


# ─── DAG Definition ───────────────────────────

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_dag",
    default_args=default_args,
    description="End-to-end data pipeline: Ingest → Clean → OLTP → DW",
    schedule=timedelta(days=1),
    catchup=False,
    tags=["data-pipeline", "assignment-week3"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=task_ingest,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=task_clean,
    )

    load_oltp = PythonOperator(
        task_id="load_oltp",
        python_callable=task_load_oltp,
    )

    transform_dw = PythonOperator(
        task_id="transform_dw",
        python_callable=task_transform_dw,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    # Define task dependencies
    ingest >> clean >> load_oltp >> transform_dw >> quality_check
