#!/usr/bin/env python3
"""Runnable end-to-end demo: messy CSV → cleaned records → SQLite.

    python demo.py

Reads ``sample_data/customers_raw.csv`` (which deliberately contains
whitespace, inconsistent casing, a missing email, and duplicate rows),
cleans and dedupes it, loads it into ``output/customers.db``, then reads
the table back and prints it as proof.
"""

import json
import sqlite3
from pathlib import Path

from etl_pipeline import CSVExtractor, DataCleaner, Deduplicator, ETLPipeline, FieldMapper, SQLiteLoader

HERE = Path(__file__).parent
DB_PATH = HERE / "output" / "customers.db"


def main() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)

    pipeline = (
        ETLPipeline("customer_import")
        .add_extractor(CSVExtractor({
            "source_name": "customers_csv",
            "file_path": HERE / "sample_data" / "customers_raw.csv",
        }))
        .add_transformer(DataCleaner({
            "null_handling": "drop",          # rows missing an email are unusable
            "trim_strings": True,
            "lowercase_fields": ["user_email"],
        }))
        .add_transformer(FieldMapper({
            "user_id": "id",
            "user_name": "name",
            "user_email": "email",
            "signup_country": "country",
        }))
        .add_transformer(Deduplicator(key_fields=["id"]))
        .set_loader(SQLiteLoader(str(DB_PATH), "customers", if_exists="replace"))
    )

    report = pipeline.run()
    print("\n--- Run report ---")
    print(json.dumps(report, indent=2))

    print("\n--- Loaded table (read back from SQLite) ---")
    with sqlite3.connect(DB_PATH) as conn:
        for row in conn.execute("SELECT id, name, email, country FROM customers ORDER BY id"):
            print(row)


if __name__ == "__main__":
    main()
