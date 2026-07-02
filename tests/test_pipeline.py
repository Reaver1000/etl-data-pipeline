"""Behaviour spec for the ETL framework — extractors, transformers, loaders, pipeline."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest
import requests

from etl_pipeline import (
    APIExtractor,
    CSVExtractor,
    CSVLoader,
    DataCleaner,
    Deduplicator,
    ETLPipeline,
    FieldMapper,
    SQLiteLoader,
)


# ---------- CSVExtractor ----------

def test_csv_extractor_reads_records(tmp_path):
    csv = tmp_path / "in.csv"
    csv.write_text("a,b\n1,x\n2,y\n")

    result = CSVExtractor({"source_name": "test", "file_path": csv}).extract()

    assert result.success
    assert result.record_count == 2
    assert result.data[0] == {"a": 1, "b": "x"}
    assert result.metadata["columns"] == ["a", "b"]


def test_csv_extractor_missing_file_captures_error_not_raises(tmp_path):
    result = CSVExtractor({"source_name": "test", "file_path": tmp_path / "nope.csv"}).extract()

    assert not result.success
    assert result.data == []
    assert result.errors


# ---------- APIExtractor (requests mocked — no network in tests) ----------

def _mock_response(payload):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


@pytest.mark.parametrize(
    "payload,expected_count",
    [
        ([{"id": 1}, {"id": 2}], 2),          # bare list
        ({"data": [{"id": 1}]}, 1),            # {"data": [...]}
        ({"items": [{"id": 1}, {"id": 2}]}, 2),  # {"items": [...]}
        ({"id": 1}, 1),                          # single object
    ],
)
def test_api_extractor_handles_common_shapes(payload, expected_count):
    with patch("etl_pipeline.requests.get", return_value=_mock_response(payload)):
        result = APIExtractor({"source_name": "api", "base_url": "https://x"}).extract()

    assert result.success
    assert result.record_count == expected_count


def test_api_extractor_request_failure_captured():
    with patch("etl_pipeline.requests.get", side_effect=requests.ConnectionError("down")):
        result = APIExtractor({"source_name": "api", "base_url": "https://x"}).extract()

    assert not result.success
    assert result.data == []
    assert "down" in result.errors[0]


# ---------- Transformers ----------

def test_cleaner_drops_null_rows():
    data = [{"a": "x", "b": 1}, {"a": None, "b": 2}]
    result = DataCleaner({"null_handling": "drop"}).transform(data)
    assert [r["b"] for r in result.data] == [1]


def test_cleaner_fills_nulls():
    data = [{"a": None, "b": 2}]
    result = DataCleaner({"null_handling": "fill", "fill_value": "?"}).transform(data)
    assert result.data[0]["a"] == "?"


def test_cleaner_trims_and_lowercases():
    data = [{"email": "  ALICE@X.COM  ", "name": "  Alice "}]
    result = DataCleaner({"null_handling": "drop", "lowercase_fields": ["email"]}).transform(data)
    assert result.data[0] == {"email": "alice@x.com", "name": "Alice"}


def test_cleaner_empty_input():
    result = DataCleaner({}).transform([])
    assert result.data == []
    assert result.success


def test_field_mapper_renames_and_whitelists():
    data = [{"user_id": 1, "user_name": "A", "internal_junk": "drop me"}]
    result = FieldMapper({"user_id": "id", "user_name": "name"}).transform(data)
    assert result.data == [{"id": 1, "name": "A"}]


def test_deduplicator_keeps_first_occurrence():
    data = [{"id": 1, "v": "first"}, {"id": 2, "v": "x"}, {"id": 1, "v": "second"}]
    result = Deduplicator(key_fields=["id"]).transform(data)
    assert len(result.data) == 2
    assert result.data[0]["v"] == "first"


# ---------- SQLiteLoader ----------

def test_sqlite_loader_roundtrip(tmp_path):
    db = tmp_path / "t.db"
    data = [{"id": 1, "name": "A", "score": 9.5}, {"id": 2, "name": "B", "score": 7.0}]

    result = SQLiteLoader(str(db), "people").load(data)

    assert result.success
    assert result.records_loaded == 2
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT id, name, score FROM people ORDER BY id").fetchall()
    assert rows == [(1, "A", 9.5), (2, "B", 7.0)]


def test_sqlite_loader_infers_types(tmp_path):
    db = tmp_path / "t.db"
    SQLiteLoader(str(db), "typed").load([{"i": 1, "f": 1.5, "s": "x"}])

    with sqlite3.connect(db) as conn:
        cols = {row[1]: row[2] for row in conn.execute("PRAGMA table_info(typed)")}
    assert cols == {"i": "INTEGER", "f": "REAL", "s": "TEXT"}


def test_sqlite_loader_append_vs_replace(tmp_path):
    db = tmp_path / "t.db"
    SQLiteLoader(str(db), "t").load([{"id": 1}])
    SQLiteLoader(str(db), "t", if_exists="append").load([{"id": 2}])
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2

    SQLiteLoader(str(db), "t", if_exists="replace").load([{"id": 3}])
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT id FROM t").fetchall() == [(3,)]


def test_sqlite_loader_rejects_malicious_table_name(tmp_path):
    with pytest.raises(ValueError):
        SQLiteLoader(str(tmp_path / "t.db"), "users; DROP TABLE x--")


def test_sqlite_loader_empty_data_is_noop(tmp_path):
    result = SQLiteLoader(str(tmp_path / "t.db"), "t").load([])
    assert result.success
    assert result.records_loaded == 0


# ---------- CSVLoader ----------

def test_csv_loader_writes_file(tmp_path):
    out = tmp_path / "out.csv"
    result = CSVLoader(str(out)).load([{"a": 1, "b": "x"}])
    assert result.success
    assert "a,b" in out.read_text()


# ---------- End-to-end pipeline ----------

def test_pipeline_end_to_end(tmp_path):
    """Messy CSV in → cleaned, mapped, deduped rows in SQLite out."""
    csv = tmp_path / "raw.csv"
    csv.write_text(
        "user_id,user_email\n"
        "1,  ALICE@X.COM \n"
        "2,\n"              # null email — dropped by cleaner
        "1,alice@x.com\n"   # duplicate id — dropped by deduplicator
        "3,bob@x.com\n"
    )
    db = tmp_path / "out.db"

    report = (
        ETLPipeline("test_run")
        .add_extractor(CSVExtractor({"source_name": "csv", "file_path": csv}))
        .add_transformer(DataCleaner({"null_handling": "drop", "lowercase_fields": ["user_email"]}))
        .add_transformer(FieldMapper({"user_id": "id", "user_email": "email"}))
        .add_transformer(Deduplicator(key_fields=["id"]))
        .set_loader(SQLiteLoader(str(db), "users"))
        .run()
    )

    assert report["extractions"][0]["records"] == 4
    assert report["load"]["records_loaded"] == 2
    assert report["load"]["success"]

    with sqlite3.connect(db) as conn:
        rows = dict(conn.execute("SELECT id, email FROM users").fetchall())
    assert rows == {1: "alice@x.com", 3: "bob@x.com"}


def test_pipeline_reports_extractor_failure_and_continues(tmp_path):
    report = (
        ETLPipeline("failing_run")
        .add_extractor(CSVExtractor({"source_name": "missing", "file_path": tmp_path / "no.csv"}))
        .set_loader(SQLiteLoader(str(tmp_path / "out.db"), "t"))
        .run()
    )
    assert report["extractions"][0]["success"] is False
    assert report["load"]["records_loaded"] == 0
