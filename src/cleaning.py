"""
cleaning.py - Data cleaning and standardization module.

Applies quality rules to raw DataFrames:
- Column name standardization
- Duplicate removal
- Null handling
- Data type validation & conversion
- Text normalization
"""
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _log_stats(name: str, before: int, after: int, issues: list[str]):
    """Log cleaning statistics."""
    logger.info(f"  [{name}] Rows: {before} → {after} | Issues: {len(issues)}")
    for issue in issues:
        logger.info(f"    - {issue}")


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize customer data."""
    logger.info("Cleaning customers...")
    before = len(df)
    issues = []

    # Standardize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Strip whitespace from text fields
    for col in ["name", "email", "phone", "address", "city", "state"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Title case names
    df["name"] = df["name"].str.title()

    # Lowercase emails
    df["email"] = df["email"].str.lower()

    # Title case cities
    df["city"] = df["city"].str.title()

    # Uppercase state codes
    df["state"] = df["state"].str.upper()

    # Handle missing phone numbers
    missing_phone = df["phone"].isin(["", "nan", "None"]).sum()
    if missing_phone > 0:
        issues.append(f"{missing_phone} missing phone numbers → replaced with 'N/A'")
        df["phone"] = df["phone"].replace({"": "N/A", "nan": "N/A", "None": "N/A"})

    # Handle missing addresses
    missing_addr = df["address"].isin(["", "nan", "None"]).sum()
    if missing_addr > 0:
        issues.append(f"{missing_addr} missing addresses → replaced with 'N/A'")
        df["address"] = df["address"].replace({"": "N/A", "nan": "N/A", "None": "N/A"})

    # Remove duplicate emails (keep first occurrence)
    dup_emails = df["email"].duplicated().sum()
    if dup_emails > 0:
        issues.append(f"{dup_emails} duplicate email(s) → removed")
        df = df.drop_duplicates(subset=["email"], keep="first")

    # Parse dates
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    invalid_dates = df["signup_date"].isna().sum()
    if invalid_dates > 0:
        issues.append(f"{invalid_dates} invalid signup date(s)")

    df = df.reset_index(drop=True)
    _log_stats("customers", before, len(df), issues)
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize order data."""
    logger.info("Cleaning orders...")
    before = len(df)
    issues = []

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Parse dates
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    invalid_dates = df["order_date"].isna().sum()
    if invalid_dates > 0:
        issues.append(f"{invalid_dates} invalid order date(s)")

    # Standardize status
    df["status"] = df["status"].str.strip().str.lower()

    # Validate amounts
    negative_amounts = (df["total_amount"] < 0).sum()
    if negative_amounts > 0:
        issues.append(f"{negative_amounts} negative amount(s) → set to 0")
        df.loc[df["total_amount"] < 0, "total_amount"] = 0

    # Ensure numeric types
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0)

    # Remove duplicates
    dups = df["order_id"].duplicated().sum()
    if dups > 0:
        issues.append(f"{dups} duplicate order(s) → removed")
        df = df.drop_duplicates(subset=["order_id"], keep="first")

    df = df.reset_index(drop=True)
    _log_stats("orders", before, len(df), issues)
    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize order items data."""
    logger.info("Cleaning order items...")
    before = len(df)
    issues = []

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Strip product names
    df["product_name"] = df["product_name"].str.strip().str.title()

    # Validate quantities
    invalid_qty = (df["quantity"] <= 0).sum()
    if invalid_qty > 0:
        issues.append(f"{invalid_qty} invalid quantity(s) → set to 1")
        df.loc[df["quantity"] <= 0, "quantity"] = 1

    # Validate unit prices
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)

    df = df.reset_index(drop=True)
    _log_stats("order_items", before, len(df), issues)
    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize payment data."""
    logger.info("Cleaning payments...")
    before = len(df)
    issues = []

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Parse dates (handle empty strings)
    df["payment_date"] = df["payment_date"].replace({"": None})
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")
    invalid_dates = df["payment_date"].isna().sum()
    if invalid_dates > 0:
        issues.append(f"{invalid_dates} missing/invalid payment date(s)")

    # Standardize payment methods → lowercase_with_underscores
    method_map = {
        "credit_card": "credit_card",
        "Credit Card": "credit_card",
        "credit card": "credit_card",
        "debit_card": "debit_card",
        "Debit Card": "debit_card",
        "debit card": "debit_card",
        "paypal": "paypal",
        "PayPal": "paypal",
        "Paypal": "paypal",
    }
    unmapped = set(df["method"].unique()) - set(method_map.keys())
    if unmapped:
        issues.append(f"Unknown payment methods: {unmapped}")
    df["method"] = df["method"].map(method_map).fillna(df["method"].str.lower().str.replace(" ", "_"))

    # Standardize status
    df["status"] = df["status"].str.strip().str.lower()

    # Validate amounts
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # Remove duplicates
    dups = df["payment_id"].duplicated().sum()
    if dups > 0:
        issues.append(f"{dups} duplicate payment(s) → removed")
        df = df.drop_duplicates(subset=["payment_id"], keep="first")

    df = df.reset_index(drop=True)
    _log_stats("payments", before, len(df), issues)
    return df


def clean_all(data: dict) -> dict:
    """
    Clean all ingested DataFrames.

    Args:
        data: dict with keys 'customers', 'orders', 'order_items', 'payments'

    Returns:
        dict with cleaned DataFrames
    """
    logger.info("Starting full data cleaning...")
    return {
        "customers": clean_customers(data["customers"]),
        "orders": clean_orders(data["orders"]),
        "order_items": clean_order_items(data["order_items"]),
        "payments": clean_payments(data["payments"]),
    }
