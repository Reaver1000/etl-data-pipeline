# ETL Data Pipeline

[![CI](https://github.com/Reaver1000/etl-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Reaver1000/etl-data-pipeline/actions/workflows/ci.yml)

A small, composable extract-transform-load framework in Python, with a runnable end-to-end demo and a full test suite. It demonstrates pluggable stages, explicit error capture, schema mapping, deduplication, SQLite loading, and structured run reports.

```
CSV / JSON API          clean → map → dedupe            SQLite / CSV
┌────────────┐          ┌──────────────────┐          ┌────────────┐
│ Extractors │ ───────▶ │   Transformers   │ ───────▶ │   Loader   │
└────────────┘          └──────────────────┘          └────────────┘
        each stage reports into a structured run report
```

## Quick start

```bash
pip install -r requirements.txt
python demo.py      # end-to-end run against the bundled messy sample data
pytest              # 21 tests
```

The demo ingests `sample_data/customers_raw.csv` — which deliberately contains stray whitespace, inconsistent casing, a missing email, and duplicate rows — cleans it, loads it into `output/customers.db`, and reads the table back to prove the roundtrip.

## What's in the box

| Component | Behaviour |
|---|---|
| `CSVExtractor` / `APIExtractor` | Pull records into a common list-of-dicts shape. Failures are captured on the result object, never raised — one bad source doesn't kill the run. The API extractor handles the three common JSON response shapes (bare list, `{"data": [...]}`/`{"items": [...]}`, single object). |
| `DataCleaner` | Null handling (drop or fill), whitespace trimming, per-field lowercasing. |
| `FieldMapper` | Renames fields between schemas — and deliberately drops unmapped fields, so it doubles as a column whitelist. |
| `Deduplicator` | Drops records duplicating an earlier record on the configured key fields. |
| `SQLiteLoader` | Creates the table if missing (types inferred from the data), inserts with parameterised statements, validates the table name (DDL can't be parameterised). `append` or `replace` modes. |
| `ETLPipeline` | Chains it all and returns a run report: per-stage record counts, successes, errors, timing. |

## Design notes

- **Errors are data, not exceptions.** Extractors and loaders return result objects with an `errors` list; the pipeline records failures and carries on. This is the property you actually want in a scheduled job — a full crash report at the end beats a stack trace halfway through.
- **The mapper whitelists.** Upstream APIs grow fields without warning; because `FieldMapper` only copies mapped fields, new upstream columns can never reach the database unreviewed.
- **SQL identifiers are validated, values are parameterised.** Table/column names can't be bound as parameters in SQLite, so the loader validates the table name instead and quotes identifiers — the injection test in the suite documents this edge.
- **Tests never touch the network.** `APIExtractor` tests mock `requests.get`; everything else runs against pytest `tmp_path`.

## Extending

Subclass `Extractor`, `Transformer`, or `Loader` and implement the one abstract method — the pipeline treats all stages uniformly. Obvious next steps: a Parquet loader, retry-with-backoff in the API extractor, and per-record validation errors on the cleaner.
