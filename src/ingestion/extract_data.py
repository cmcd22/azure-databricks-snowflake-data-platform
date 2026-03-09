from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests



BASE_URL = "https://fakestoreapi.com"
ENDPOINTS = {
    "products": "/products",
    "users": "/users",
    "carts": "/carts",
}


PROJECT_ROOT = Path(__file__).resolve().parents[2]

@dataclass
class IngestionConfig:
    base_url: str = BASE_URL
    output_root: Path = PROJECT_ROOT / "data" / "raw"
    timeout_seconds: int = 30


def get_json(url: str, timeout_seconds: int) -> Any:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()


def build_output_path(output_root: Path, entity: str, ingestion_date: str) -> Path:
    return output_root / entity / f"ingestion_date={ingestion_date}" / f"{entity}.json"


def write_json(payload: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def ingest_entity(entity: str, endpoint: str, config: IngestionConfig, batch_id: str) -> Path:
    url = f"{config.base_url}{endpoint}"
    ingested_at = datetime.now(timezone.utc)
    ingestion_date = ingested_at.strftime("%Y-%m-%d")

    data = get_json(url, config.timeout_seconds)

    if isinstance(data, list):
        record_count = len(data)
    else:
        record_count = 1

    payload = {
        "metadata": {
            "source": "fakestoreapi",
            "entity": entity,
            "source_url": url,
            "ingested_at_utc": ingested_at.isoformat(),
            "ingestion_date": ingestion_date,
            "batch_id": batch_id,
            "record_count": record_count,
        },
        "data": data,
    }

    output_path = build_output_path(config.output_root, entity, ingestion_date)
    write_json(payload, output_path)
    return output_path


def main() -> int:
    config = IngestionConfig()
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    print("Starting Fake Store API ingestion...")

    try:
        for entity, endpoint in ENDPOINTS.items():
            output_path = ingest_entity(entity, endpoint, config, batch_id)
            print(f"[OK] {entity:<10} -> {output_path}")
    except requests.HTTPError as e:
        print(f"[ERROR] HTTP error during ingestion: {e}", file=sys.stderr)
        return 1
    except requests.RequestException as e:
        print(f"[ERROR] Request failed: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[ERROR] Unexpected failure: {e}", file=sys.stderr)
        return 1

    print("Ingestion complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
