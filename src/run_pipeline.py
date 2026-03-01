"""
run_pipeline.py - Standalone pipeline runner.

Executes the full ETL pipeline without Airflow:
  1. Ingest data from CSV, JSON, Excel
  2. Clean and standardize
  3. Load into OLTP (SQLite)
  4. Transform into Data Warehouse (Star Schema)
  5. Run quality checks
"""
import os
import sys
import logging

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.ingestion import ingest_all
from src.cleaning import clean_all
from src.oltp import load_to_oltp
from src.data_warehouse import transform_to_dw

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Execute the full data pipeline."""
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    OLTP_DB = os.path.join(PROJECT_ROOT, "db", "oltp.db")
    DW_DB = os.path.join(PROJECT_ROOT, "db", "warehouse.db")

    logger.info("=" * 60)
    logger.info("STARTING DATA PIPELINE")
    logger.info("=" * 60)

    # Step 1: Ingest
    logger.info("\n>>> STEP 1: INGESTION")
    raw_data = ingest_all(DATA_DIR)

    # Step 2: Clean
    logger.info("\n>>> STEP 2: CLEANING")
    clean_data = clean_all(raw_data)

    # Step 3: Load to OLTP
    logger.info("\n>>> STEP 3: LOAD TO OLTP")
    load_to_oltp(clean_data, OLTP_DB)

    # Step 4: Transform to DW
    logger.info("\n>>> STEP 4: TRANSFORM TO DATA WAREHOUSE")
    transform_to_dw(OLTP_DB, DW_DB)

    # Step 5: Quality Summary
    logger.info("\n>>> STEP 5: QUALITY SUMMARY")
    logger.info("Pipeline completed successfully!")
    logger.info(f"  OLTP Database: {OLTP_DB}")
    logger.info(f"  DW Database:   {DW_DB}")

    return True


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
