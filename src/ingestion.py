"""
ingestion.py - Multi-format data ingestion module.

Reads data from CSV, JSON, and Excel files into pandas DataFrames.
"""
import pandas as pd
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def ingest_csv(filepath: str) -> pd.DataFrame:
    """Read customers data from a CSV file."""
    logger.info(f"Ingesting CSV: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  → Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def ingest_json(filepath: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read orders data from a JSON file.
    Returns two DataFrames: (orders, order_items).
    The nested 'items' array is flattened into a separate DataFrame.
    """
    logger.info(f"Ingesting JSON: {filepath}")
    with open(filepath, "r") as f:
        raw_data = json.load(f)

    # Extract order-level data (without nested items)
    orders = []
    order_items = []
    item_id_counter = 1

    for record in raw_data:
        orders.append({
            "order_id": record["order_id"],
            "customer_id": record["customer_id"],
            "order_date": record["order_date"],
            "status": record["status"],
            "total_amount": record["total_amount"],
        })

        # Flatten nested items
        for item in record.get("items", []):
            order_items.append({
                "item_id": item_id_counter,
                "order_id": record["order_id"],
                "product_name": item["product_name"],
                "quantity": item["quantity"],
                "unit_price": item["unit_price"],
            })
            item_id_counter += 1

    orders_df = pd.DataFrame(orders)
    items_df = pd.DataFrame(order_items)
    logger.info(f"  → Loaded {len(orders_df)} orders, {len(items_df)} order items")
    return orders_df, items_df


def ingest_excel(filepath: str) -> pd.DataFrame:
    """Read payments data from an Excel file."""
    logger.info(f"Ingesting Excel: {filepath}")
    df = pd.read_excel(filepath, engine="openpyxl")
    logger.info(f"  → Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def ingest_all(data_dir: str) -> dict:
    """
    Ingest all data files from the specified directory.

    Returns:
        dict with keys: 'customers', 'orders', 'order_items', 'payments'
    """
    logger.info(f"Starting full ingestion from: {data_dir}")

    customers = ingest_csv(os.path.join(data_dir, "customers.csv"))
    orders, order_items = ingest_json(os.path.join(data_dir, "orders.json"))
    payments = ingest_excel(os.path.join(data_dir, "payments.xlsx"))

    result = {
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
    }

    logger.info("Ingestion complete!")
    return result
