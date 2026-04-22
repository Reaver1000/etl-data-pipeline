#!/usr/bin/env python3
"""
ETL Pipeline Framework
Modular extraction, transformation, and loading framework.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result of an extraction operation."""
    source: str
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    extracted_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def success(self) -> bool:
        return len(self.errors) == 0
    
    @property
    def record_count(self) -> int:
        return len(self.data)


@dataclass
class TransformResult:
    """Result of a transformation operation."""
    data: List[Dict[str, Any]]
    schema: Dict[str, str] = field(default_factory=dict)
    validation_errors: List[Dict[str, Any]] = field(default_factory=list)
    transformed_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def success(self) -> bool:
        return len(self.validation_errors) == 0


@dataclass
class LoadResult:
    """Result of a load operation."""
    destination: str
    records_loaded: int
    errors: List[str] = field(default_factory=list)
    loaded_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def success(self) -> bool:
        return len(self.errors) == 0


class Extractor(ABC):
    """Abstract base class for data extractors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_name = config.get('source_name', 'unknown')
    
    @abstractmethod
    def extract(self) -> ExtractionResult:
        """Extract data from source."""
        pass
    
    def validate_config(self) -> bool:
        """Validate extractor configuration."""
        return True


class APIExtractor(Extractor):
    """Extract data from REST APIs."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('base_url')
        self.headers = config.get('headers', {})
        self.auth = config.get('auth')
    
    def extract(self) -> ExtractionResult:
        """Extract data from API endpoint."""
        data = []
        errors = []
        
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                auth=self.auth,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Handle paginated responses
            if isinstance(result, dict):
                data = result.get('data', result.get('items', [result]))
            elif isinstance(result, list):
                data = result
            else:
                data = [result]
            
            logger.info(f"Extracted {len(data)} records from {self.source_name}")
            
        except requests.RequestException as e:
            errors.append(f"API request failed: {str(e)}")
            logger.error(f"Extraction error for {self.source_name}: {e}")
        
        return ExtractionResult(
            source=self.source_name,
            data=data,
            errors=errors
        )


class CSVExtractor(Extractor):
    """Extract data from CSV files."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = config.get('file_path')
    
    def extract(self) -> ExtractionResult:
        """Extract data from CSV file."""
        try:
            df = pd.read_csv(self.file_path)
            data = df.to_dict('records')
            logger.info(f"Extracted {len(data)} records from {self.file_path}")
            
            return ExtractionResult(
                source=self.source_name,
                data=data,
                metadata={'columns': list(df.columns), 'shape': df.shape}
            )
        except Exception as e:
            logger.error(f"CSV extraction error: {e}")
            return ExtractionResult(
                source=self.source_name,
                data=[],
                errors=[str(e)]
            )


class Transformer(ABC):
    """Abstract base class for data transformers."""
    
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        """Transform extracted data."""
        pass


class DataCleaner(Transformer):
    """Clean and standardize data."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.null_handling = config.get('null_handling', 'drop')
        self.trim_strings = config.get('trim_strings', True)
        self.lowercase_fields = config.get('lowercase_fields', [])
    
    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        """Clean data according to configuration."""
        df = pd.DataFrame(data)
        validation_errors = []
        
        # Handle nulls
        if self.null_handling == 'drop':
            df = df.dropna()
        elif self.null_handling == 'fill':
            df = df.fillna(self.config.get('fill_value', ''))
        
        # Trim strings
        if self.trim_strings:
            string_cols = df.select_dtypes(include=['object']).columns
            df[string_cols] = df[string_cols].apply(lambda x: x.str.strip())
        
        # Lowercase specified fields
        for field in self.lowercase_fields:
            if field in df.columns:
                df[field] = df[field].str.lower()
        
        # Detect schema
        schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
        
        return TransformResult(
            data=df.to_dict('records'),
            schema=schema,
            validation_errors=validation_errors
        )


class FieldMapper(Transformer):
    """Map fields between different schemas."""
    
    def __init__(self, mapping: Dict[str, str]):
        self.mapping = mapping
    
    def transform(self, data: List[Dict[str, Any]]) -> TransformResult:
        """Apply field mapping."""
        transformed = []
        
        for record in data:
            new_record = {}
            for old_field, new_field in self.mapping.items():
                if old_field in record:
                    new_record[new_field] = record[old_field]
            transformed.append(new_record)
        
        return TransformResult(data=transformed)


class Loader(ABC):
    """Abstract base class for data loaders."""
    
    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> LoadResult:
        """Load data to destination."""
        pass


class DatabaseLoader(Loader):
    """Load data to SQL database."""
    
    def __init__(self, connection_string: str, table_name: str):
        self.connection_string = connection_string
        self.table_name = table_name
    
    def load(self, data: List[Dict[str, Any]]) -> LoadResult:
        """Load data to database table."""
        try:
            df = pd.DataFrame(data)
            # In production, use SQLAlchemy or similar
            # df.to_sql(self.table_name, self.connection_string, if_exists='append', index=False)
            
            logger.info(f"Would load {len(df)} records to {self.table_name}")
            
            return LoadResult(
                destination=self.table_name,
                records_loaded=len(df)
            )
        except Exception as e:
            logger.error(f"Load error: {e}")
            return LoadResult(
                destination=self.table_name,
                records_loaded=0,
                errors=[str(e)]
            )


class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, name: str):
        self.name = name
        self.extractors: List[Extractor] = []
        self.transformers: List[Transformer] = []
        self.loader: Optional[Loader] = None
    
    def add_extractor(self, extractor: Extractor) -> 'ETLPipeline':
        """Add an extractor to the pipeline."""
        self.extractors.append(extractor)
        return self
    
    def add_transformer(self, transformer: Transformer) -> 'ETLPipeline':
        """Add a transformer to the pipeline."""
        self.transformers.append(transformer)
        return self
    
    def set_loader(self, loader: Loader) -> 'ETLPipeline':
        """Set the loader for the pipeline."""
        self.loader = loader
        return self
    
    def run(self) -> Dict[str, Any]:
        """Execute the ETL pipeline."""
        logger.info(f"Starting ETL pipeline: {self.name}")
        start_time = datetime.utcnow()
        
        results = {
            'pipeline_name': self.name,
            'started_at': start_time.isoformat(),
            'extractions': [],
            'transformations': [],
            'load': None
        }
        
        # Extract phase
        all_data = []
        for extractor in self.extractors:
            extraction_result = extractor.extract()
            results['extractions'].append({
                'source': extraction_result.source,
                'records': extraction_result.record_count,
                'success': extraction_result.success
            })
            all_data.extend(extraction_result.data)
        
        # Transform phase
        transformed_data = all_data
        for transformer in self.transformers:
            transform_result = transformer.transform(transformed_data)
            results['transformations'].append({
                'records_in': len(transformed_data),
                'records_out': len(transform_result.data),
                'success': transform_result.success
            })
            transformed_data = transform_result.data
        
        # Load phase
        if self.loader:
            load_result = self.loader.load(transformed_data)
            results['load'] = {
                'destination': load_result.destination,
                'records_loaded': load_result.records_loaded,
                'success': load_result.success
            }
        
        end_time = datetime.utcnow()
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        logger.info(f"Pipeline completed: {self.name} in {results['duration_seconds']:.2f}s")
        
        return results


if __name__ == '__main__':
    # Example usage
    pipeline = ETLPipeline('user_sync')
    
    # Add extractors
    pipeline.add_extractor(APIExtractor({
        'source_name': 'users_api',
        'base_url': 'https://api.example.com/users',
        'headers': {'Authorization': 'Bearer token'}
    }))
    
    # Add transformers
    pipeline.add_transformer(DataCleaner({
        'null_handling': 'drop',
        'trim_strings': True
    }))
    
    pipeline.add_transformer(FieldMapper({
        'user_id': 'id',
        'user_name': 'name',
        'user_email': 'email'
    }))
    
    # Run pipeline
    result = pipeline.run()
    print(json.dumps(result, indent=2))