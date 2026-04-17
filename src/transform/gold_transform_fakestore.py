from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class TransformConfig:
    silver_root: Path = PROJECT_ROOT / "data" / "silver"
    gold_root: Path = PROJECT_ROOT / "data" / "gold"


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required parquet file not found: {path}")
    return pd.read_parquet(path)


def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def build_dim_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    dim_customers = customers_df[
        [
            "customer_id",
            "email",
            "username",
            "first_name",
            "last_name",
            "phone",
            "city",
            "street",
            "street_number",
            "zipcode",
            "latitude",
            "longitude",
        ]
    ].copy()

    dim_customers = dim_customers.drop_duplicates(subset=["customer_id"]).reset_index(drop=True)
    dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)

    return dim_customers


def build_dim_products(products_df: pd.DataFrame) -> pd.DataFrame:
    dim_products = products_df[
        [
            "product_id",
            "title",
            "category",
            "price",
            "description",
            "image_url",
            "rating_rate",
            "rating_count",
        ]
    ].copy()

    dim_products = dim_products.drop_duplicates(subset=["product_id"]).reset_index(drop=True)
    dim_products = dim_products.sort_values("product_id").reset_index(drop=True)

    return dim_products


def build_dim_date(orders_df: pd.DataFrame) -> pd.DataFrame:
    order_dates = pd.to_datetime(orders_df["order_date"], errors="coerce", utc=True)

    dim_date = pd.DataFrame(
        {
            "date": order_dates.dt.date,
        }
    ).dropna().drop_duplicates().sort_values("date").reset_index(drop=True)

    dim_date["date_key"] = pd.to_datetime(dim_date["date"]).dt.strftime("%Y%m%d").astype("Int64")
    date_ts = pd.to_datetime(dim_date["date"])

    dim_date["year"] = date_ts.dt.year.astype("Int64")
    dim_date["quarter"] = date_ts.dt.quarter.astype("Int64")
    dim_date["month"] = date_ts.dt.month.astype("Int64")
    dim_date["month_name"] = date_ts.dt.month_name()
    dim_date["day"] = date_ts.dt.day.astype("Int64")
    dim_date["day_of_week"] = date_ts.dt.dayofweek.astype("Int64")
    dim_date["day_name"] = date_ts.dt.day_name()
    dim_date["is_weekend"] = dim_date["day_of_week"].isin([5, 6])

    dim_date = dim_date[
        [
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "is_weekend",
        ]
    ]

    return dim_date


def build_fact_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    fact_orders = orders_df[
        [
            "order_id",
            "customer_id",
            "order_date",
            "product_line_count",
            "total_quantity",
        ]
    ].copy()

    fact_orders["order_date"] = pd.to_datetime(fact_orders["order_date"], errors="coerce", utc=True)
    fact_orders["order_date_key"] = fact_orders["order_date"].dt.strftime("%Y%m%d").astype("Int64")

    fact_orders = fact_orders[
        [
            "order_id",
            "customer_id",
            "order_date",
            "order_date_key",
            "product_line_count",
            "total_quantity",
        ]
    ]

    fact_orders = fact_orders.drop_duplicates(subset=["order_id"]).reset_index(drop=True)
    fact_orders = fact_orders.sort_values("order_id").reset_index(drop=True)

    return fact_orders


def build_fact_order_items(order_items_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    product_prices = products_df[["product_id", "price"]].copy()

    fact_order_items = order_items_df.merge(
        product_prices,
        on="product_id",
        how="left",
        validate="many_to_one",
    )

    fact_order_items["line_total"] = fact_order_items["quantity"] * fact_order_items["price"]

    fact_order_items = fact_order_items[
        [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "price",
            "line_total",
        ]
    ]

    fact_order_items = fact_order_items.drop_duplicates(subset=["order_item_id"]).reset_index(drop=True)

    fact_order_items["order_id_num"] = pd.to_numeric(fact_order_items["order_id"], errors="coerce")
    fact_order_items["line_num"] = (
        fact_order_items["order_item_id"]
        .astype(str)
        .str.split("_")
        .str[-1]
        .pipe(pd.to_numeric, errors="coerce")
    )

    fact_order_items = fact_order_items.sort_values(
        ["order_id_num", "line_num"],
        na_position="last",
    ).reset_index(drop=True)

    fact_order_items = fact_order_items.drop(columns=["order_id_num", "line_num"])

    return fact_order_items


def run_quality_checks(
    dim_customers: pd.DataFrame,
    dim_products: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_orders: pd.DataFrame,
    fact_order_items: pd.DataFrame,
) -> None:
    if dim_customers["customer_id"].isna().any():
        raise ValueError("Null customer_id found in dim_customers")

    if dim_products["product_id"].isna().any():
        raise ValueError("Null product_id found in dim_products")

    if dim_date["date_key"].isna().any():
        raise ValueError("Null date_key found in dim_date")

    if fact_orders["order_id"].isna().any():
        raise ValueError("Null order_id found in fact_orders")

    if fact_order_items["order_item_id"].isna().any():
        raise ValueError("Null order_item_id found in fact_order_items")

    if dim_customers["customer_id"].duplicated().any():
        raise ValueError("Duplicate customer_id found in dim_customers")

    if dim_products["product_id"].duplicated().any():
        raise ValueError("Duplicate product_id found in dim_products")

    if dim_date["date_key"].duplicated().any():
        raise ValueError("Duplicate date_key found in dim_date")

    if fact_orders["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id found in fact_orders")

    if fact_order_items["order_item_id"].duplicated().any():
        raise ValueError("Duplicate order_item_id found in fact_order_items")

    missing_customer_ids = set(fact_orders["customer_id"].dropna()) - set(dim_customers["customer_id"].dropna())
    if missing_customer_ids:
        raise ValueError(f"fact_orders references missing customer_ids: {sorted(missing_customer_ids)}")

    missing_product_ids = set(fact_order_items["product_id"].dropna()) - set(dim_products["product_id"].dropna())
    if missing_product_ids:
        raise ValueError(f"fact_order_items references missing product_ids: {sorted(missing_product_ids)}")

    missing_date_keys = set(fact_orders["order_date_key"].dropna()) - set(dim_date["date_key"].dropna())
    if missing_date_keys:
        raise ValueError(f"fact_orders references missing date_keys: {sorted(missing_date_keys)}")

    if fact_order_items["price"].isna().any():
        raise ValueError("Null price found in fact_order_items after joining products")

    if (fact_order_items["quantity"] < 0).any():
        raise ValueError("Negative quantity found in fact_order_items")

    if (fact_order_items["line_total"] < 0).any():
        raise ValueError("Negative line_total found in fact_order_items")


def main() -> int:
    config = TransformConfig()

    try:
        customers_path = config.silver_root / "customers" / "customers.parquet"
        products_path = config.silver_root / "products" / "products.parquet"
        orders_path = config.silver_root / "orders" / "orders.parquet"
        order_items_path = config.silver_root / "order_items" / "order_items.parquet"

        print(f"[OK] Loading silver_customers:   {customers_path}")
        print(f"[OK] Loading silver_products:    {products_path}")
        print(f"[OK] Loading silver_orders:      {orders_path}")
        print(f"[OK] Loading silver_order_items: {order_items_path}")

        customers_df = load_parquet(customers_path)
        products_df = load_parquet(products_path)
        orders_df = load_parquet(orders_path)
        order_items_df = load_parquet(order_items_path)

        dim_customers = build_dim_customers(customers_df)
        dim_products = build_dim_products(products_df)
        dim_date = build_dim_date(orders_df)
        fact_orders = build_fact_orders(orders_df)
        fact_order_items = build_fact_order_items(order_items_df, products_df)

        run_quality_checks(
            dim_customers=dim_customers,
            dim_products=dim_products,
            dim_date=dim_date,
            fact_orders=fact_orders,
            fact_order_items=fact_order_items,
        )

        dim_customers_output = config.gold_root / "dim_customers" / "dim_customers.parquet"
        dim_products_output = config.gold_root / "dim_products" / "dim_products.parquet"
        dim_date_output = config.gold_root / "dim_date" / "dim_date.parquet"
        fact_orders_output = config.gold_root / "fact_orders" / "fact_orders.parquet"
        fact_order_items_output = config.gold_root / "fact_order_items" / "fact_order_items.parquet"

        write_parquet(dim_customers, dim_customers_output)
        write_parquet(dim_products, dim_products_output)
        write_parquet(dim_date, dim_date_output)
        write_parquet(fact_orders, fact_orders_output)
        write_parquet(fact_order_items, fact_order_items_output)

        print(f"[OK] Wrote dim_customers:    {dim_customers_output} ({len(dim_customers)} rows)")
        print(f"[OK] Wrote dim_products:     {dim_products_output} ({len(dim_products)} rows)")
        print(f"[OK] Wrote dim_date:         {dim_date_output} ({len(dim_date)} rows)")
        print(f"[OK] Wrote fact_orders:      {fact_orders_output} ({len(fact_orders)} rows)")
        print(f"[OK] Wrote fact_order_items: {fact_order_items_output} ({len(fact_order_items)} rows)")
        print("[OK] Gold transformation complete.")

        return 0

    except Exception as e:
        print(f"[ERROR] Gold transformation failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
