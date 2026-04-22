# ETL Data Pipeline

Production-grade ETL pipeline demonstrating data engineering best practices: extraction, transformation, loading, and monitoring.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │────▶│  MONITOR    │
│             │     │             │     │             │     │             │
│ • APIs      │     │ • Clean     │     │ • Upsert    │     │ • Logging   │
│ • Database  │     │ • Validate  │     │ • Dedupe    │     │ • Alerting  │
│ • Files     │     │ • Aggregate │     │ • Index     │     │ • Metrics   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Features

| Component | Implementation |
|-----------|----------------|
| **Extraction** | REST APIs, PostgreSQL, CSV/JSON files |
| **Transformation** | Pandas, SQL transforms, data validation |
| **Loading** | Upsert pattern, incremental loads |
| **Scheduling** | Airflow DAGs or cron jobs |
| **Monitoring** | Logging, Slack alerts, metrics |
| **Error Handling** | Retry logic, dead letter queue |

## Quick Start

```bash
# Install dependencies
pip install pandas sqlalchemy psycopg2-binary requests

# Run single ETL job
python scripts/run_etl.py --job orders --date 2024-01-15

# Run all jobs
python scripts/run_etl.py --all

# Run with Airflow
airflow dags trigger etl_daily
```

## Pipeline Components

### 1. Extractors (`scripts/extractors.py`)

```python
class BaseExtractor:
    """Base class for all data extractors."""
    
    def extract(self, since: datetime) -> pd.DataFrame:
        raise NotImplementedError
    
    def get_source_name(self) -> str:
        return self.__class__.__name__

class APIExtractor(BaseExtractor):
    """Extract data from REST APIs with pagination."""
    
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key
    
    def extract(self, since: datetime) -> pd.DataFrame:
        all_records = []
        page = 1
        
        while True:
            response = requests.get(
                self.endpoint,
                params={'since': since.isoformat(), 'page': page},
                headers={'Authorization': f'Bearer {self.api_key}'}
            )
            data = response.json()
            
            if not data['records']:
                break
                
            all_records.extend(data['records'])
            page += 1
            
            if page > data['total_pages']:
                break
        
        return pd.DataFrame(all_records)

class DatabaseExtractor(BaseExtractor):
    """Extract data from PostgreSQL with incremental loading."""
    
    def __init__(self, connection_string: str, query: str):
        self.engine = create_engine(connection_string)
        self.query = query
    
    def extract(self, since: datetime) -> pd.DataFrame:
        query = self.query.replace(':since', since.isoformat())
        return pd.read_sql(query, self.engine)
```

### 2. Transformers (`scripts/transformers.py`)

```python
class BaseTransformer:
    """Base class for data transformations."""
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

class DataCleaner(BaseTransformer):
    """Clean and standardize data."""
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df = df.fillna({'status': 'unknown', 'amount': 0})
        
        # Standardize strings
        string_cols = df.select_dtypes(include='object').columns
        for col in string_cols:
            df[col] = df[col].str.strip().str.lower()
        
        return df

class DataValidator(BaseTransformer):
    """Validate data quality."""
    
    def __init__(self, rules: dict):
        self.rules = rules
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        errors = []
        
        for col, rule in self.rules.items():
            if col not in df.columns:
                errors.append(f"Missing column: {col}")
                continue
            
            if rule.get('required') and df[col].isna().any():
                errors.append(f"Null values in required column: {col}")
            
            if rule.get('min_value') and df[col].min() < rule['min_value']:
                errors.append(f"Values below minimum in {col}")
            
            if rule.get('pattern') and not df[col].str.match(rule['pattern']).all():
                errors.append(f"Pattern mismatch in {col}")
        
        if errors:
            raise ValidationError(f"Data validation failed: {errors}")
        
        return df

class DataAggregator(BaseTransformer):
    """Aggregate data for analytics."""
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby(['customer_id', 'date']).agg({
            'order_id': 'count',
            'amount': 'sum',
            'items': 'sum'
        }).reset_index()
```

### 3. Loaders (`scripts/loaders.py`)

```python
class BaseLoader:
    """Base class for data loading."""
    
    def load(self, df: pd.DataFrame, table: str) -> int:
        raise NotImplementedError

class UpsertLoader(BaseLoader):
    """Upsert pattern with deduplication."""
    
    def __init__(self, connection_string: str, primary_keys: list):
        self.engine = create_engine(connection_string)
        self.primary_keys = primary_keys
    
    def load(self, df: pd.DataFrame, table: str) -> int:
        # Get existing records
        existing = pd.read_sql(
            f"SELECT {','.join(self.primary_keys)} FROM {table}",
            self.engine
        )
        
        # Filter to new/updated records
        new_records = df[~df[self.primary_keys].isin(existing).all(axis=1)]
        
        if len(new_records) == 0:
            return 0
        
        # Use temp table for upsert
        temp_table = f"temp_{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        new_records.to_sql(temp_table, self.engine, if_exists='replace')
        
        # Upsert query
        columns = ', '.join(df.columns)
        values = ', '.join([f'EXCLUDED.{c}' for c in df.columns if c not in self.primary_keys])
        
        upsert_sql = f"""
            INSERT INTO {table} ({columns})
            SELECT {columns} FROM {temp_table}
            ON CONFLICT ({','.join(self.primary_keys)})
            DO UPDATE SET {values}
        """
        
        with self.engine.begin() as conn:
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
        
        return len(new_records)
```

## Airflow DAG (`dags/etl_daily.py`)

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_daily',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 6 AM daily
    catchup=False
) as dag:
    
    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_task,
        op_kwargs={'date': '{{ ds }}'}
    )
    
    extract_customers = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers_task,
        op_kwargs={'date': '{{ ds }}'}
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task
    )
    
    load_to_warehouse = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_task
    )
    
    send_report = PythonOperator(
        task_id='send_report',
        python_callable=send_report_task
    )
    
    [extract_orders, extract_customers] >> transform_data >> load_to_warehouse >> send_report
```

## Configuration (`config/settings.py`)

```python
import os
from dataclasses import dataclass

@dataclass
class ETLConfig:
    """ETL pipeline configuration."""
    
    # Source databases
    source_postgres: str = os.getenv('SOURCE_POSTGRES_URL')
    source_mysql: str = os.getenv('SOURCE_MYSQL_URL')
    
    # Target warehouse
    warehouse_postgres: str = os.getenv('WAREHOUSE_URL')
    
    # APIs
    api_base_url: str = os.getenv('API_BASE_URL')
    api_key: str = os.getenv('API_KEY')
    
    # Monitoring
    slack_webhook: str = os.getenv('SLACK_WEBHOOK_URL')
    
    # Performance
    batch_size: int = 10000
    max_workers: int = 4
    
    @classmethod
    def from_env(cls) -> 'ETLConfig':
        return cls()
```

## Error Handling

```python
class ETLError(Exception):
    """Base ETL exception."""
    pass

class ExtractionError(ETLError):
    """Error during data extraction."""
    pass

class TransformationError(ETLError):
    """Error during data transformation."""
    pass

class LoadingError(ETLError):
    """Error during data loading."""
    pass

class DeadLetterQueue:
    """Store failed records for retry."""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def add(self, record: dict, error: str, pipeline: str):
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO dead_letter_queue (record, error, pipeline, created_at)
                VALUES (:record, :error, :pipeline, :created_at)
            """), {
                'record': json.dumps(record),
                'error': str(error),
                'pipeline': pipeline,
                'created_at': datetime.utcnow()
            })
    
    def get_pending(self, limit: int = 100) -> list:
        return pd.read_sql(
            f"SELECT * FROM dead_letter_queue WHERE processed = false LIMIT {limit}",
            self.engine
        )
```

## Metrics & Monitoring

```python
import time
from prometheus_client import Counter, Histogram, Gauge

# Prometheus metrics
RECORDS_PROCESSED = Counter(
    'etl_records_processed_total',
    'Total records processed',
    ['pipeline', 'stage']
)

PROCESSING_TIME = Histogram(
    'etl_processing_seconds',
    'Processing time in seconds',
    ['pipeline', 'stage']
)

ERRORS = Counter(
    'etl_errors_total',
    'Total errors',
    ['pipeline', 'stage', 'error_type']
)

PIPELINE_STATUS = Gauge(
    'etl_pipeline_status',
    'Pipeline status (1=running, 0=stopped)',
    ['pipeline']
)

def track_metrics(pipeline: str, stage: str):
    """Decorator to track processing metrics."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                RECORDS_PROCESSED.labels(pipeline, stage).inc(len(result))
                return result
            except Exception as e:
                ERRORS.labels(pipeline, stage, type(e).__name__).inc()
                raise
            finally:
                PROCESSING_TIME.labels(pipeline, stage).observe(time.time() - start)
        return wrapper
    return decorator
```

## Data Quality Checks

```sql
-- Row count validation
SELECT 
    'orders' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(created_at) as min_date,
    MAX(created_at) as max_date
FROM orders
WHERE created_at >= :date;

-- Null check
SELECT 
    COUNT(*) as null_orders
FROM orders
WHERE order_id IS NULL OR customer_id IS NULL;

-- Duplicate check
SELECT 
    order_id, COUNT(*) as count
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Referential integrity
SELECT COUNT(*) as orphan_orders
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

## Directory Structure

```
etl-data-pipeline/
├── README.md
├── config/
│   ├── settings.py         # Configuration
│   └── logging.py           # Logging setup
├── scripts/
│   ├── run_etl.py           # Main entry point
│   ├── extractors.py        # Extract classes
│   ├── transformers.py     # Transform classes
│   ├── loaders.py           # Load classes
│   └── utils.py             # Helpers
├── dags/
│   └── etl_daily.py        # Airflow DAG
├── data/
│   ├── raw/                # Raw extracted data
│   ├── processed/          # Transformed data
│   └── archive/            # Archived data
├── tests/
│   ├── test_extractors.py
│   ├── test_transformers.py
│   └── test_loaders.py
└── requirements.txt
```

## Requirements

```txt
pandas>=2.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
requests>=2.31.0
apache-airflow>=2.7.0
prometheus-client>=0.17.0
```

## License

MIT
