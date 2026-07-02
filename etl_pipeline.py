#!/usr/bin/env python3
"""ETL Pipeline Framework.

A small, composable extract-transform-load framework:

- Extractors pull records from a source (CSV file, JSON API) into a common
  list-of-dicts shape and never raise — failures are captured on the result.
- Transformers reshape/clean the records (null handling, trimming, field
  mapping) and report what they did.
- Loaders persist the records; ``SQLiteLoader`` infers a table schema from
  the data and writes with parameterised statements.
- ``ETLPipeline`` chains any number of each and returns a run report.

See ``demo.py`` for a runnable end-to-end example against the bundled
sample data, and ``tests/`` for the behaviour spec.
"""

import logging
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ExtractionResult:
    """Outcome of one extractor run."""

    source: str
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    extracted_at: datetime = field(default_factory=_utcnow)

    @property
    def success(self) -> bool:
        return not self.errors

    @property
    def record_count(self) -> int:
        return len(self.data)


@dataclass
class TransformResult:
    """Outcome of one transformer run."""

    data: List[Dict[str, Any]]
    schema: Dict[str, str] = field(default_factory=dict)
    validation_errors: List[Dict[str, Any]] = field(default_factory=list)
    transformed_at: datetime = field(default_factory=_utcnow)

    @property
    def success(self) -> bool:
        return not self.validation_errors


@dataclass
class LoadResult:
    """Outcome of one loader run."""

    destination: str
    records_loaded: int
    errors: List[str] = field(default_factory=list)
    loaded_at: datetime = field(default_factory=_utcnow)

    @property
    def success(self) -> bool:
        return not self.errors


class Extractor(ABC):
    """Base class for data extractors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_name = config.get("source_name", "unknown")

    @abstractmethod
    def extract(self) -> ExtractionResult:
        """Extract data from the source. Must not raise — capture errors."""


class APIExtractor(Extractor):
    """Extract records from a JSON REST endpoint.

    Handles the three common response shapes: a bare list, an object with a
    ``data``/``items`` list, or a single object.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url")
        self.headers = config.get("headers", {})
        self.auth = config.get("auth")

    def extract(self) -> ExtractionResult:
        data: List[Dict[str, Any]] = []
        errors: List[str] = []
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                auth=self.auth,
                timeout=self.config.get("timeout", 30),
            )
            response.raise_for_status()
            result = response.json()
            if isinstance(result, dict):
                data = result.get("data", result.get("items", [result]))
            elif isinstance(result, list):
                data = result
            else:
                data = [result]
            logger.info("Extracted %d records from %s", len(data), self.source_name)
        except requests.RequestException as e:
            errors.append(f"API request failed: {e}")
            logger.error("Extraction error for %s: %s", self.source_name, e)
        return ExtractionResult(source=self.source_name, data=data, errors=errors)


class CSVExtractor(Extractor):
    """Extract records from a CSV file via pandas."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = config.get("file_path")

    def extract(self) -> ExtractionResult:
        try:
            df = pd.read_csv(self.file_path)
            data = df.to_dict("records")
            logger.info("Extracted %d records from %s", len(data), self.file_path)
            return ExtractionResult(
                source=self.source_name,
                data=data,
                metadata={"columns": list(df.columns), "shape": df.shape},
            )
        except (OSError, ValueError, pd.errors.ParserError) as e:
            logger.error("CSV extraction error: %s", e)
            return ExtractionResult(source=self.source_name, data=[], errors=[str(e)])


class Transformer(ABC):
    """Base class for data transformers."""

    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        """Transform extracted records."""


class DataCleaner(Transformer):
    """Standardise records: null handling, whitespace, casing.

    Config keys:
        null_handling: ``"drop"`` (default) drops rows with any null,
                       ``"fill"`` replaces nulls with ``fill_value``.
        trim_strings:  strip leading/trailing whitespace on string columns.
        lowercase_fields: list of columns to force to lowercase.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.null_handling = config.get("null_handling", "drop")
        self.trim_strings = config.get("trim_strings", True)
        self.lowercase_fields = config.get("lowercase_fields", [])

    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        if not data:
            return TransformResult(data=[])

        df = pd.DataFrame(data)

        if self.null_handling == "drop":
            df = df.dropna()
        elif self.null_handling == "fill":
            df = df.fillna(self.config.get("fill_value", ""))

        if self.trim_strings:
            # The map guards with isinstance, so this is safe on every column
            # (and avoids select_dtypes' object/str dtype split across pandas 2/3).
            for col in df.columns:
                df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)

        for col in self.lowercase_fields:
            if col in df.columns:
                df[col] = df[col].map(lambda v: v.lower() if isinstance(v, str) else v)

        schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
        return TransformResult(data=df.to_dict("records"), schema=schema)


class FieldMapper(Transformer):
    """Rename fields between schemas; unmapped fields are dropped.

    Dropping unmapped fields is deliberate — the mapper doubles as a
    column whitelist so unexpected upstream fields never reach the loader.
    """

    def __init__(self, mapping: Dict[str, str]):
        self.mapping = mapping

    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        transformed = [
            {new: record[old] for old, new in self.mapping.items() if old in record}
            for record in data
        ]
        return TransformResult(data=transformed)


class Deduplicator(Transformer):
    """Drop records that duplicate an earlier record on the key fields."""

    def __init__(self, key_fields: List[str]):
        self.key_fields = key_fields

    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        seen = set()
        unique = []
        for record in data:
            key = tuple(record.get(k) for k in self.key_fields)
            if key not in seen:
                seen.add(key)
                unique.append(record)
        return TransformResult(data=unique)


class Loader(ABC):
    """Base class for data loaders."""

    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> LoadResult:
        """Persist records to the destination."""


_SQLITE_TYPES = {"int": "INTEGER", "float": "REAL", "bool": "INTEGER"}


class SQLiteLoader(Loader):
    """Load records into a SQLite table.

    The table is created if missing, with column types inferred from the
    first record (int/float map to INTEGER/REAL, everything else TEXT).
    Inserts are parameterised; the table name is validated because DDL
    cannot be parameterised.
    """

    def __init__(self, db_path: str, table_name: str, if_exists: str = "append"):
        if not table_name.replace("_", "").isalnum():
            raise ValueError(f"Invalid table name: {table_name!r}")
        if if_exists not in ("append", "replace"):
            raise ValueError("if_exists must be 'append' or 'replace'")
        self.db_path = db_path
        self.table_name = table_name
        self.if_exists = if_exists

    @staticmethod
    def _column_type(value: Any) -> str:
        for prefix, sql_type in _SQLITE_TYPES.items():
            if type(value).__name__.startswith(prefix):
                return sql_type
        return "TEXT"

    def load(self, data: List[Dict[str, Any]]) -> LoadResult:
        if not data:
            return LoadResult(destination=self.table_name, records_loaded=0)
        try:
            columns = list(data[0].keys())
            col_defs = ", ".join(f'"{c}" {self._column_type(data[0][c])}' for c in columns)
            placeholders = ", ".join("?" for _ in columns)
            quoted_cols = ", ".join(f'"{c}"' for c in columns)

            with sqlite3.connect(self.db_path) as conn:
                if self.if_exists == "replace":
                    conn.execute(f'DROP TABLE IF EXISTS "{self.table_name}"')
                conn.execute(f'CREATE TABLE IF NOT EXISTS "{self.table_name}" ({col_defs})')
                conn.executemany(
                    f'INSERT INTO "{self.table_name}" ({quoted_cols}) VALUES ({placeholders})',
                    [tuple(record.get(c) for c in columns) for record in data],
                )
            logger.info("Loaded %d records into %s.%s", len(data), self.db_path, self.table_name)
            return LoadResult(destination=self.table_name, records_loaded=len(data))
        except sqlite3.Error as e:
            logger.error("Load error: %s", e)
            return LoadResult(destination=self.table_name, records_loaded=0, errors=[str(e)])


class CSVLoader(Loader):
    """Write records to a CSV file."""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def load(self, data: List[Dict[str, Any]]) -> LoadResult:
        if not data:
            return LoadResult(destination=self.file_path, records_loaded=0)
        try:
            pd.DataFrame(data).to_csv(self.file_path, index=False)
            logger.info("Wrote %d records to %s", len(data), self.file_path)
            return LoadResult(destination=self.file_path, records_loaded=len(data))
        except OSError as e:
            return LoadResult(destination=self.file_path, records_loaded=0, errors=[str(e)])


class ETLPipeline:
    """Chain extractors, transformers, and a loader; report on the run."""

    def __init__(self, name: str):
        self.name = name
        self.extractors: List[Extractor] = []
        self.transformers: List[Transformer] = []
        self.loader: Optional[Loader] = None

    def add_extractor(self, extractor: Extractor) -> "ETLPipeline":
        self.extractors.append(extractor)
        return self

    def add_transformer(self, transformer: Transformer) -> "ETLPipeline":
        self.transformers.append(transformer)
        return self

    def set_loader(self, loader: Loader) -> "ETLPipeline":
        self.loader = loader
        return self

    def run(self) -> Dict[str, Any]:
        """Execute extract → transform → load and return a run report."""
        logger.info("Starting ETL pipeline: %s", self.name)
        start_time = _utcnow()
        report: Dict[str, Any] = {
            "pipeline_name": self.name,
            "started_at": start_time.isoformat(),
            "extractions": [],
            "transformations": [],
            "load": None,
        }

        all_data: List[Dict[str, Any]] = []
        for extractor in self.extractors:
            result = extractor.extract()
            report["extractions"].append(
                {"source": result.source, "records": result.record_count,
                 "success": result.success, "errors": result.errors}
            )
            all_data.extend(result.data)

        data = all_data
        for transformer in self.transformers:
            result = transformer.transform(data)
            report["transformations"].append(
                {"transformer": type(transformer).__name__,
                 "records_in": len(data), "records_out": len(result.data),
                 "success": result.success}
            )
            data = result.data

        if self.loader:
            load_result = self.loader.load(data)
            report["load"] = {
                "destination": load_result.destination,
                "records_loaded": load_result.records_loaded,
                "success": load_result.success,
                "errors": load_result.errors,
            }

        end_time = _utcnow()
        report["completed_at"] = end_time.isoformat()
        report["duration_seconds"] = (end_time - start_time).total_seconds()
        logger.info("Pipeline completed: %s in %.2fs", self.name, report["duration_seconds"])
        return report
