from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class TransformConfig:
    raw_root: Path = PROJECT_ROOT / "data" / "raw"
    silver_root: Path = PROJECT_ROOT / "data" / "silver"


def find_latest_raw_file(entity: str, raw_root: Path) -> Path:
    entity_dir = raw_root / entity
    if not entity_dir.exists():
        raise FileNotFoundError(f"Raw entity folder not found: {entity_dir}")

    candidate_files = sorted(entity_dir.glob("ingestion_date=*/" + f"{entity}.json"))
    if not candidate_files:
        raise FileNotFoundError(f"No raw JSON files found for entity '{entity}' in {entity_dir}")

    return candidate_files[-1]


def load_raw_payload(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if "metadata" not in payload or "data" not in payload:
        raise ValueError(f"Invalid raw payload structure in {path}")

    return payload


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def standardize_city(city: Any) -> Any:
    if pd.isna(city):
        return city
    return str(city).strip().title()


def transform_products(payload: dict[str, Any]) -> pd.DataFrame:
    metadata = payload["metadata"]
    rows = []

    for product in payload["data"]:
        rating = product.get("rating", {}) or {}

        rows.append(
            {
                "product_id": product.get("id"),
                "title": product.get("title"),
                "price": product.get("price"),
                "description": product.get("description"),
                "category": product.get("category"),
                "image_url": product.get("image"),
                "rating_rate": rating.get("rate"),
                "rating_count": rating.get("count"),
                "ingested_at_utc": metadata.get("ingested_at_utc"),
                "batch_id": metadata.get("batch_id"),
            }
        )

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.astype(
            {
                "product_id": "Int64",
                "price": "float64",
                "rating_rate": "float64",
                "rating_count": "Int64",
            }
        )

    return df


def transform_customers(payload: dict[str, Any]) -> pd.DataFrame:
    metadata = payload["metadata"]
    rows = []

    for user in payload["data"]:
        name = user.get("name", {}) or {}
        address = user.get("address", {}) or {}
        geolocation = address.get("geolocation", {}) or {}

        rows.append(
            {
                "customer_id": user.get("id"),
                "email": user.get("email"),
                "username": user.get("username"),
                "first_name": name.get("firstname"),
                "last_name": name.get("lastname"),
                "phone": user.get("phone"),
                "city": standardize_city(address.get("city")),
                "street": address.get("street"),
                "street_number": address.get("number"),
                "zipcode": address.get("zipcode"),
                "latitude": geolocation.get("lat"),
                "longitude": geolocation.get("long"),
                "ingested_at_utc": metadata.get("ingested_at_utc"),
                "batch_id": metadata.get("batch_id"),
            }
        )

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.astype(
            {
                "customer_id": "Int64",
                "street_number": "Int64",
            }
        )
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    return df


def transform_orders(payload: dict[str, Any]) -> pd.DataFrame:
    metadata = payload["metadata"]
    rows = []

    for cart in payload["data"]:
        products = cart.get("products", []) or []

        rows.append(
            {
                "order_id": cart.get("id"),
                "customer_id": cart.get("userId"),
                "order_date": pd.to_datetime(cart.get("date"), errors="coerce", utc=True),
                "product_line_count": len(products),
                "total_quantity": sum(item.get("quantity", 0) for item in products),
                "ingested_at_utc": metadata.get("ingested_at_utc"),
                "batch_id": metadata.get("batch_id"),
            }
        )

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.astype(
            {
                "order_id": "Int64",
                "customer_id": "Int64",
                "product_line_count": "Int64",
                "total_quantity": "Int64",
            }
        )

    return df


def transform_order_items(payload: dict[str, Any]) -> pd.DataFrame:
    metadata = payload["metadata"]
    rows = []

    for cart in payload["data"]:
        order_id = cart.get("id")
        products = cart.get("products", []) or []

        for line_number, item in enumerate(products, start=1):
            rows.append(
                {
                    "order_item_id": f"{order_id}_{line_number}",
                    "order_id": order_id,
                    "product_id": item.get("productId"),
                    "quantity": item.get("quantity"),
                    "ingested_at_utc": metadata.get("ingested_at_utc"),
                    "batch_id": metadata.get("batch_id"),
                }
            )

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.astype(
            {
                "order_id": "Int64",
                "product_id": "Int64",
                "quantity": "Int64",
            }
        )

    return df


def run_quality_checks(
    products_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
) -> None:
    if products_df["product_id"].isna().any():
        raise ValueError("Null product_id found in silver_products")

    if customers_df["customer_id"].isna().any():
        raise ValueError("Null customer_id found in silver_customers")

    if orders_df["order_id"].isna().any():
        raise ValueError("Null order_id found in silver_orders")

    if order_items_df["order_id"].isna().any() or order_items_df["product_id"].isna().any():
        raise ValueError("Null order_id or product_id found in silver_order_items")

    if products_df["product_id"].duplicated().any():
        raise ValueError("Duplicate product_id found in silver_products")

    if customers_df["customer_id"].duplicated().any():
        raise ValueError("Duplicate customer_id found in silver_customers")

    if orders_df["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id found in silver_orders")

    missing_customer_ids = set(orders_df["customer_id"].dropna()) - set(customers_df["customer_id"].dropna())
    if missing_customer_ids:
        raise ValueError(f"Orders reference missing customer_ids: {sorted(missing_customer_ids)}")

    missing_product_ids = set(order_items_df["product_id"].dropna()) - set(products_df["product_id"].dropna())
    if missing_product_ids:
        raise ValueError(f"Order items reference missing product_ids: {sorted(missing_product_ids)}")


def main() -> int:
    config = TransformConfig()

    try:
        products_path = find_latest_raw_file("products", config.raw_root)
        users_path = find_latest_raw_file("users", config.raw_root)
        carts_path = find_latest_raw_file("carts", config.raw_root)

        print(f"[OK] Found raw products file: {products_path}")
        print(f"[OK] Found raw users file:    {users_path}")
        print(f"[OK] Found raw carts file:    {carts_path}")

        products_payload = load_raw_payload(products_path)
        users_payload = load_raw_payload(users_path)
        carts_payload = load_raw_payload(carts_path)

        silver_products_df = transform_products(products_payload)
        silver_customers_df = transform_customers(users_payload)
        silver_orders_df = transform_orders(carts_payload)
        silver_order_items_df = transform_order_items(carts_payload)

        run_quality_checks(
            products_df=silver_products_df,
            customers_df=silver_customers_df,
            orders_df=silver_orders_df,
            order_items_df=silver_order_items_df,
        )

        products_output = config.silver_root / "products" / "products.parquet"
        customers_output = config.silver_root / "customers" / "customers.parquet"
        orders_output = config.silver_root / "orders" / "orders.parquet"
        order_items_output = config.silver_root / "order_items" / "order_items.parquet"

        write_parquet(silver_products_df, products_output)
        write_parquet(silver_customers_df, customers_output)
        write_parquet(silver_orders_df, orders_output)
        write_parquet(silver_order_items_df, order_items_output)

        print(f"[OK] Wrote silver_products:    {products_output} ({len(silver_products_df)} rows)")
        print(f"[OK] Wrote silver_customers:   {customers_output} ({len(silver_customers_df)} rows)")
        print(f"[OK] Wrote silver_orders:      {orders_output} ({len(silver_orders_df)} rows)")
        print(f"[OK] Wrote silver_order_items: {order_items_output} ({len(silver_order_items_df)} rows)")
        print("[OK] Silver transformation complete.")

        return 0

    except Exception as e:
        print(f"[ERROR] Silver transformation failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
