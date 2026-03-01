"""
oltp.py - OLTP Schema Definition and Data Loading.

Defines a normalized (3NF) relational schema using SQLAlchemy ORM
and provides functions to load cleaned DataFrames into SQLite.

Tables:
- customers (PK: customer_id)
- orders (PK: order_id, FK: customer_id)
- order_items (PK: item_id, FK: order_id)
- payments (PK: payment_id, FK: order_id)
"""
import os
import logging
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, ForeignKey,
    inspect,
)
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

Base = declarative_base()

# ──────────────────────────────────────────────
# ORM Models
# ──────────────────────────────────────────────

class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    phone = Column(String(20))
    address = Column(String(200))
    city = Column(String(50))
    state = Column(String(2))
    signup_date = Column(Date)


class Order(Base):
    __tablename__ = "orders"

    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=False)
    order_date = Column(Date, nullable=False)
    status = Column(String(20), nullable=False)
    total_amount = Column(Float, default=0)


class OrderItem(Base):
    __tablename__ = "order_items"

    item_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.order_id"), nullable=False)
    product_name = Column(String(100), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)


class Payment(Base):
    __tablename__ = "payments"

    payment_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.order_id"), nullable=False)
    payment_date = Column(Date)
    amount = Column(Float, nullable=False)
    method = Column(String(30), nullable=False)
    status = Column(String(20), nullable=False)


# ──────────────────────────────────────────────
# Database Setup
# ──────────────────────────────────────────────

def get_oltp_engine(db_path: str = None):
    """Create and return a SQLAlchemy engine for the OLTP database."""
    if db_path is None:
        db_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "db", "oltp.db"
        )
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    return engine


def create_oltp_schema(engine):
    """Create all OLTP tables."""
    logger.info("Creating OLTP schema...")
    Base.metadata.drop_all(engine)   # Start fresh
    Base.metadata.create_all(engine)
    logger.info("OLTP schema created successfully.")


# ──────────────────────────────────────────────
# Data Loading
# ──────────────────────────────────────────────

def load_customers(session, df: pd.DataFrame):
    """Load cleaned customers into the OLTP database."""
    logger.info(f"Loading {len(df)} customers...")
    for _, row in df.iterrows():
        customer = Customer(
            customer_id=int(row["customer_id"]),
            name=row["name"],
            email=row["email"],
            phone=row.get("phone", "N/A"),
            address=row.get("address", "N/A"),
            city=row.get("city", ""),
            state=row.get("state", ""),
            signup_date=row["signup_date"].date() if pd.notna(row["signup_date"]) else None,
        )
        session.merge(customer)
    session.commit()
    logger.info("  → Customers loaded.")


def load_orders(session, df: pd.DataFrame):
    """Load cleaned orders into the OLTP database."""
    logger.info(f"Loading {len(df)} orders...")
    for _, row in df.iterrows():
        order = Order(
            order_id=int(row["order_id"]),
            customer_id=int(row["customer_id"]),
            order_date=row["order_date"].date() if pd.notna(row["order_date"]) else None,
            status=row["status"],
            total_amount=float(row["total_amount"]),
        )
        session.merge(order)
    session.commit()
    logger.info("  → Orders loaded.")


def load_order_items(session, df: pd.DataFrame):
    """Load cleaned order items into the OLTP database."""
    logger.info(f"Loading {len(df)} order items...")
    for _, row in df.iterrows():
        item = OrderItem(
            item_id=int(row["item_id"]),
            order_id=int(row["order_id"]),
            product_name=row["product_name"],
            quantity=int(row["quantity"]),
            unit_price=float(row["unit_price"]),
        )
        session.merge(item)
    session.commit()
    logger.info("  → Order items loaded.")


def load_payments(session, df: pd.DataFrame):
    """Load cleaned payments into the OLTP database."""
    logger.info(f"Loading {len(df)} payments...")
    for _, row in df.iterrows():
        payment = Payment(
            payment_id=int(row["payment_id"]),
            order_id=int(row["order_id"]),
            payment_date=row["payment_date"].date() if pd.notna(row["payment_date"]) else None,
            amount=float(row["amount"]),
            method=row["method"],
            status=row["status"],
        )
        session.merge(payment)
    session.commit()
    logger.info("  → Payments loaded.")


def load_to_oltp(data: dict, db_path: str = None):
    """
    Load all cleaned DataFrames into the OLTP database.

    Args:
        data: dict with keys 'customers', 'orders', 'order_items', 'payments'
        db_path: optional path to SQLite database
    """
    engine = get_oltp_engine(db_path)
    create_oltp_schema(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        load_customers(session, data["customers"])
        load_orders(session, data["orders"])
        load_order_items(session, data["order_items"])
        load_payments(session, data["payments"])
        logger.info("All data loaded into OLTP database!")
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading data: {e}")
        raise
    finally:
        session.close()

    # Verification
    verify_oltp(engine)
    return engine


def verify_oltp(engine):
    """Verify OLTP load by checking row counts and FK integrity."""
    logger.info("Verifying OLTP database...")
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    logger.info(f"  Tables: {tables}")

    with engine.connect() as conn:
        from sqlalchemy import text
        for table in ["customers", "orders", "order_items", "payments"]:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            logger.info(f"  {table}: {count} rows")

        # FK integrity check: all orders reference valid customers
        orphan_orders = conn.execute(text(
            "SELECT COUNT(*) FROM orders o "
            "WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)"
        )).scalar()
        logger.info(f"  Orphan orders (no matching customer): {orphan_orders}")

        # FK integrity check: all payments reference valid orders
        orphan_payments = conn.execute(text(
            "SELECT COUNT(*) FROM payments p "
            "WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.order_id = p.order_id)"
        )).scalar()
        logger.info(f"  Orphan payments (no matching order): {orphan_payments}")
