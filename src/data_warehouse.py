"""
data_warehouse.py - Star Schema Data Warehouse with Simulated Partitioning.

Transforms OLTP data into a star schema:
  - dim_customer:       Customer dimension
  - dim_date:           Date dimension (year, quarter, month, day)
  - dim_payment_method: Payment method dimension
  - fact_orders:        Central fact table (order measures)

Simulated Partitioning (Option B - SQLite):
  - Monthly tables: fact_orders_YYYYMM
  - Union view: fact_orders_all → UNION ALL of all monthly tables
"""
import os
import logging
from sqlalchemy import create_engine, text, inspect
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Database Setup
# ──────────────────────────────────────────────

def get_dw_engine(db_path: str = None):
    """Create and return engine for the data warehouse database."""
    if db_path is None:
        db_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "db", "warehouse.db"
        )
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    return engine


# ──────────────────────────────────────────────
# Dimension Table Builders
# ──────────────────────────────────────────────

def build_dim_customer(oltp_engine, dw_engine):
    """Build the customer dimension from OLTP customers table."""
    logger.info("Building dim_customer...")
    df = pd.read_sql("SELECT * FROM customers", oltp_engine)
    dim = df.rename(columns={"customer_id": "customer_key"})
    dim = dim[["customer_key", "name", "email", "city", "state"]]

    with dw_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_customer"))
    dim.to_sql("dim_customer", dw_engine, index=False, if_exists="replace")
    logger.info(f"  → dim_customer: {len(dim)} rows")
    return dim


def build_dim_date(oltp_engine, dw_engine):
    """Build the date dimension from order dates in OLTP."""
    logger.info("Building dim_date...")
    orders_df = pd.read_sql("SELECT order_date FROM orders", oltp_engine)
    payments_df = pd.read_sql("SELECT payment_date FROM payments WHERE payment_date IS NOT NULL", oltp_engine)

    # Collect all unique dates
    all_dates = set()
    for date_str in orders_df["order_date"].dropna():
        all_dates.add(pd.Timestamp(date_str))
    for date_str in payments_df["payment_date"].dropna():
        all_dates.add(pd.Timestamp(date_str))

    # Generate a complete date range for the year
    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
        date_range = pd.date_range(
            start=min_date.replace(month=1, day=1),
            end=max_date.replace(month=12, day=31),
            freq="D"
        )
    else:
        date_range = pd.date_range(start="2024-01-01", end="2024-12-31", freq="D")

    dim = pd.DataFrame({
        "date_key": [int(d.strftime("%Y%m%d")) for d in date_range],
        "date": [d.date() for d in date_range],
        "year": [d.year for d in date_range],
        "quarter": [d.quarter for d in date_range],
        "month": [d.month for d in date_range],
        "month_name": [d.strftime("%B") for d in date_range],
        "day": [d.day for d in date_range],
        "day_of_week": [d.strftime("%A") for d in date_range],
        "is_weekend": [1 if d.weekday() >= 5 else 0 for d in date_range],
    })

    with dw_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_date"))
    dim.to_sql("dim_date", dw_engine, index=False, if_exists="replace")
    logger.info(f"  → dim_date: {len(dim)} rows")
    return dim


def build_dim_payment_method(oltp_engine, dw_engine):
    """Build the payment method dimension from OLTP payments table."""
    logger.info("Building dim_payment_method...")
    df = pd.read_sql("SELECT DISTINCT method FROM payments", oltp_engine)

    dim = pd.DataFrame({
        "payment_method_key": range(1, len(df) + 1),
        "method": df["method"].values,
    })

    with dw_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_payment_method"))
    dim.to_sql("dim_payment_method", dw_engine, index=False, if_exists="replace")
    logger.info(f"  → dim_payment_method: {len(dim)} rows")
    return dim


# ──────────────────────────────────────────────
# Fact Table Builder
# ──────────────────────────────────────────────

def build_fact_orders(oltp_engine, dw_engine, dim_payment_method: pd.DataFrame):
    """
    Build the fact_orders table by joining OLTP orders, order_items, and payments.
    Returns the fact DataFrame for subsequent partitioning.
    """
    logger.info("Building fact_orders...")

    # Join orders with item counts
    orders_df = pd.read_sql("""
        SELECT
            o.order_id,
            o.customer_id   AS customer_key,
            o.order_date,
            o.status,
            o.total_amount,
            COUNT(oi.item_id) AS item_count
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id
    """, oltp_engine)

    # Get payment method per order
    payments_df = pd.read_sql("""
        SELECT order_id, method, status AS payment_status
        FROM payments
    """, oltp_engine)

    # Merge fact with payment info
    fact = orders_df.merge(payments_df, on="order_id", how="left")

    # Map payment method to key
    method_map = dict(zip(dim_payment_method["method"], dim_payment_method["payment_method_key"]))
    fact["payment_method_key"] = fact["method"].map(method_map).fillna(0).astype(int)

    # Build date_key from order_date
    fact["order_date"] = pd.to_datetime(fact["order_date"])
    fact["date_key"] = fact["order_date"].dt.strftime("%Y%m%d").astype(int)

    # Build partition key (YYYYMM)
    fact["partition_key"] = fact["order_date"].dt.strftime("%Y%m")

    # Select final columns
    fact = fact.rename(columns={"order_id": "order_key"})
    fact_final = fact[[
        "order_key", "customer_key", "date_key", "payment_method_key",
        "total_amount", "item_count", "status", "partition_key"
    ]]

    logger.info(f"  → fact_orders: {len(fact_final)} rows")
    return fact_final


# ──────────────────────────────────────────────
# Simulated Partitioning (Option B)
# ──────────────────────────────────────────────

def create_partitioned_tables(dw_engine, fact_df: pd.DataFrame):
    """
    Simulate monthly partitioning by creating separate tables per month.

    Creates:
      - fact_orders_YYYYMM for each month with data
      - fact_orders_all view (UNION ALL of all monthly tables)
    """
    logger.info("Creating simulated monthly partitions...")

    # Get unique months
    months = sorted(fact_df["partition_key"].unique())
    logger.info(f"  Months to partition: {months}")

    with dw_engine.begin() as conn:
        # Drop existing partition tables and view
        conn.execute(text("DROP VIEW IF EXISTS fact_orders_all"))
        for month in months:
            conn.execute(text(f"DROP TABLE IF EXISTS fact_orders_{month}"))

    # Create monthly tables
    for month in months:
        month_data = fact_df[fact_df["partition_key"] == month].copy()
        table_name = f"fact_orders_{month}"
        month_data.to_sql(table_name, dw_engine, index=False, if_exists="replace")
        logger.info(f"  → {table_name}: {len(month_data)} rows")

    # Create UNION ALL view
    union_parts = [f"SELECT * FROM fact_orders_{m}" for m in months]
    union_sql = " UNION ALL ".join(union_parts)
    view_sql = f"CREATE VIEW fact_orders_all AS {union_sql}"

    with dw_engine.begin() as conn:
        conn.execute(text(view_sql))
    logger.info(f"  → Created fact_orders_all view ({len(months)} partitions)")


# ──────────────────────────────────────────────
# Main Entry Point
# ──────────────────────────────────────────────

def transform_to_dw(oltp_db_path: str = None, dw_db_path: str = None):
    """
    Transform OLTP database into a star schema data warehouse.

    Steps:
      1. Build dimension tables
      2. Build fact table
      3. Create simulated monthly partitions
      4. Verify the warehouse
    """
    logger.info("=" * 60)
    logger.info("TRANSFORMING OLTP → DATA WAREHOUSE")
    logger.info("=" * 60)

    oltp_engine = _get_oltp_engine_helper(oltp_db_path)
    dw_engine = get_dw_engine(dw_db_path)

    # Step 1: Build dimensions
    dim_customer = build_dim_customer(oltp_engine, dw_engine)
    dim_date = build_dim_date(oltp_engine, dw_engine)
    dim_payment = build_dim_payment_method(oltp_engine, dw_engine)

    # Step 2: Build fact table
    fact_df = build_fact_orders(oltp_engine, dw_engine, dim_payment)

    # Step 3: Create simulated partitions
    create_partitioned_tables(dw_engine, fact_df)

    # Step 4: Verify
    verify_dw(dw_engine)

    logger.info("Data warehouse transformation complete!")
    return dw_engine


def _get_oltp_engine_helper(oltp_db_path: str = None):
    """Helper to get OLTP engine."""
    if oltp_db_path is None:
        oltp_db_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "db", "oltp.db"
        )
    from sqlalchemy import create_engine
    return create_engine(f"sqlite:///{oltp_db_path}", echo=False)


def verify_dw(engine):
    """Verify the data warehouse by checking table/view counts."""
    logger.info("Verifying Data Warehouse...")
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    views = inspector.get_view_names()

    logger.info(f"  Tables: {tables}")
    logger.info(f"  Views: {views}")

    with engine.connect() as conn:
        # Check dimension tables
        for dim in ["dim_customer", "dim_date", "dim_payment_method"]:
            if dim in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {dim}")).scalar()
                logger.info(f"  {dim}: {count} rows")

        # Check partition tables
        partition_tables = [t for t in tables if t.startswith("fact_orders_")]
        total_fact_rows = 0
        for pt in sorted(partition_tables):
            count = conn.execute(text(f"SELECT COUNT(*) FROM {pt}")).scalar()
            logger.info(f"  {pt}: {count} rows")
            total_fact_rows += count

        # Check the union view
        if "fact_orders_all" in views:
            view_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders_all")).scalar()
            logger.info(f"  fact_orders_all (view): {view_count} rows")
            assert view_count == total_fact_rows, \
                f"View count ({view_count}) != sum of partitions ({total_fact_rows})"
            logger.info("  ✓ View count matches sum of partition tables")
