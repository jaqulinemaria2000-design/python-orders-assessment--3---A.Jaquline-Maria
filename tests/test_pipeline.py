"""
test_pipeline.py - Pipeline tests and quality gates.

Tests:
  1. Data ingestion from all file formats
  2. Data cleaning rules
  3. OLTP loading and schema verification
  4. DW transformation and partition verification
  5. End-to-end pipeline
"""
import os
import sys
import tempfile
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src.ingestion import ingest_all
from src.cleaning import clean_all
from src.oltp import load_to_oltp, get_oltp_engine
from src.data_warehouse import transform_to_dw, get_dw_engine

DATA_DIR = os.path.join(PROJECT_ROOT, "data")


class TestIngestion(unittest.TestCase):
    """Test data ingestion from all file formats."""

    def test_ingest_all(self):
        data = ingest_all(DATA_DIR)
        self.assertIn("customers", data)
        self.assertIn("orders", data)
        self.assertIn("order_items", data)
        self.assertIn("payments", data)
        self.assertGreater(len(data["customers"]), 0)
        self.assertGreater(len(data["orders"]), 0)
        self.assertGreater(len(data["order_items"]), 0)
        self.assertGreater(len(data["payments"]), 0)

    def test_customers_columns(self):
        data = ingest_all(DATA_DIR)
        expected_cols = {"customer_id", "name", "email", "phone", "address", "city", "state", "signup_date"}
        self.assertTrue(expected_cols.issubset(set(data["customers"].columns)))

    def test_orders_columns(self):
        data = ingest_all(DATA_DIR)
        expected_cols = {"order_id", "customer_id", "order_date", "status", "total_amount"}
        self.assertTrue(expected_cols.issubset(set(data["orders"].columns)))


class TestCleaning(unittest.TestCase):
    """Test data cleaning and standardization."""

    def setUp(self):
        self.raw_data = ingest_all(DATA_DIR)
        self.clean_data = clean_all(self.raw_data)

    def test_customer_names_title_case(self):
        names = self.clean_data["customers"]["name"]
        for name in names:
            self.assertEqual(name, name.title(), f"Name not title case: {name}")

    def test_customer_emails_lowercase(self):
        emails = self.clean_data["customers"]["email"]
        for email in emails:
            self.assertEqual(email, email.lower(), f"Email not lowercase: {email}")

    def test_no_duplicate_emails(self):
        emails = self.clean_data["customers"]["email"]
        self.assertEqual(len(emails), len(emails.unique()), "Duplicate emails found")

    def test_payment_methods_standardized(self):
        valid_methods = {"credit_card", "debit_card", "paypal"}
        methods = set(self.clean_data["payments"]["method"].unique())
        self.assertTrue(methods.issubset(valid_methods),
                        f"Non-standard methods found: {methods - valid_methods}")


class TestOLTP(unittest.TestCase):
    """Test OLTP loading and schema verification."""

    def test_oltp_load(self):
        raw_data = ingest_all(DATA_DIR)
        clean_data = clean_all(raw_data)

        tmpdir = os.path.join(PROJECT_ROOT, "db", "test")
        os.makedirs(tmpdir, exist_ok=True)
        db_path = os.path.join(tmpdir, "test_oltp.db")

        try:
            engine = load_to_oltp(clean_data, db_path)

            from sqlalchemy import text
            with engine.connect() as conn:
                # Verify tables exist and have data
                for table in ["customers", "orders", "order_items", "payments"]:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    self.assertGreater(count, 0, f"{table} is empty")

                # Verify FK integrity
                orphans = conn.execute(text(
                    "SELECT COUNT(*) FROM orders o "
                    "WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)"
                )).scalar()
                self.assertEqual(orphans, 0, "Orphan orders found")

            engine.dispose()
        finally:
            # Clean up test DB
            try:
                if os.path.exists(db_path):
                    os.remove(db_path)
                if os.path.exists(tmpdir):
                    os.rmdir(tmpdir)
            except PermissionError:
                pass  # Windows may still hold lock


class TestDataWarehouse(unittest.TestCase):
    """Test DW transformation and partitioning."""

    def test_dw_transform(self):
        raw_data = ingest_all(DATA_DIR)
        clean_data = clean_all(raw_data)

        tmpdir = os.path.join(PROJECT_ROOT, "db", "test")
        os.makedirs(tmpdir, exist_ok=True)
        oltp_path = os.path.join(tmpdir, "test_oltp.db")
        dw_path = os.path.join(tmpdir, "test_dw.db")

        try:
            load_to_oltp(clean_data, oltp_path)
            dw_engine = transform_to_dw(oltp_path, dw_path)

            from sqlalchemy import text, inspect
            inspector = inspect(dw_engine)

            # Verify dimension tables
            tables = inspector.get_table_names()
            self.assertIn("dim_customer", tables)
            self.assertIn("dim_date", tables)
            self.assertIn("dim_payment_method", tables)

            # Verify partition tables exist
            partition_tables = [t for t in tables if t.startswith("fact_orders_")]
            self.assertGreater(len(partition_tables), 0, "No partition tables created")

            # Verify union view
            views = inspector.get_view_names()
            self.assertIn("fact_orders_all", views)

            with dw_engine.connect() as conn:
                # Verify view has data
                view_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders_all")).scalar()
                self.assertGreater(view_count, 0, "fact_orders_all view is empty")

                # Verify partition sum matches view
                total = 0
                for pt in partition_tables:
                    total += conn.execute(text(f"SELECT COUNT(*) FROM {pt}")).scalar()
                self.assertEqual(view_count, total, "View count != partition sum")

            dw_engine.dispose()
        finally:
            # Clean up test DBs
            try:
                for p in [oltp_path, dw_path]:
                    if os.path.exists(p):
                        os.remove(p)
                if os.path.exists(tmpdir):
                    os.rmdir(tmpdir)
            except PermissionError:
                pass  # Windows may still hold lock


if __name__ == "__main__":
    unittest.main(verbosity=2)
