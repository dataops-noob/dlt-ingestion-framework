"""
Delta Live Tables Ingestion Pipeline
Configuration-driven data ingestion with built-in auditing and monitoring.
"""

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
import yaml
import json
from typing import Dict, List, Any, Optional

# Import utilities
import sys
sys.path.append('/Workspace/Users/shared/dlt-ingestion-framework')
from src.utils.config_loader import ConfigLoader
from src.utils.connection_manager import ConnectionManager
from src.audit.audit_logger import AuditLogger


class DLTIngestionPipeline:
    """
    Main pipeline class that orchestrates data ingestion from multiple sources
    using Delta Live Tables with built-in auditing capabilities.
    """
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.load()
        self.connection_manager = ConnectionManager()
        self.audit_logger = AuditLogger(
            catalog=self.config['global']['catalog'],
            schema=self.config['global']['audit_schema']
        )
        self.spark = spark  # Databricks runtime provides this
        
    def get_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get configuration for a specific source."""
        for source in self.config['sources']:
            if source['name'] == source_name:
                return source
        raise ValueError(f"Source '{source_name}' not found in configuration")
    
    def get_table_config(self, source_name: str, table_name: str) -> Dict[str, Any]:
        """Get configuration for a specific table within a source."""
        source = self.get_source_config(source_name)
        for table in source['tables']:
            if table['name'] == table_name:
                return {**source, **table, 'source_name': source_name}
        raise ValueError(f"Table '{table_name}' not found in source '{source_name}'")


# =============================================================================
# DLT TABLE DEFINITIONS
# =============================================================================

@dlt.table(
    name="audit_batch_runs",
    comment="Audit log for all batch ingestion runs",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def audit_batch_runs():
    """
    Delta Live Table for tracking all batch runs.
    Records start time, end time, status, and metrics for each ingestion batch.
    """
    schema = StructType([
        StructField("batch_id", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("run_start_time", TimestampType(), False),
        StructField("run_end_time", TimestampType(), True),
        StructField("status", StringType(), False),  # RUNNING, SUCCESS, FAILED
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("records_failed", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("watermark_value", TimestampType(), True),
        StructField("pipeline_version", StringType(), False),
        StructField("spark_version", StringType(), False),
        StructField("cluster_id", StringType(), True),
        StructField("execution_duration_ms", LongType(), True)
    ])
    
    # Return empty DataFrame with schema (will be populated by pipeline runs)
    return spark.createDataFrame([], schema)


@dlt.table(
    name="audit_data_quality",
    comment="Data quality check results",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def audit_data_quality():
    """
    Tracks data quality check results for each batch.
    """
    schema = StructType([
        StructField("check_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_timestamp", TimestampType(), False),
        StructField("rule_name", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("check_type", StringType(), False),  # NOT_NULL, RANGE, CUSTOM
        StructField("records_checked", LongType(), False),
        StructField("records_passed", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("failure_percentage", DoubleType(), False),
        StructField("severity", StringType(), False),  # ERROR, WARN
        StructField("is_quarantined", BooleanType(), False)
    ])
    
    return spark.createDataFrame([], schema)


@dlt.table(
    name="audit_schema_evolution",
    comment="Schema evolution tracking",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def audit_schema_evolution():
    """
    Tracks schema changes over time for each table.
    """
    schema = StructType([
        StructField("evolution_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("change_timestamp", TimestampType(), False),
        StructField("previous_schema", StringType(), False),
        StructField("new_schema", StringType(), False),
        StructField("change_type", StringType(), False),  # ADD_COLUMN, DROP_COLUMN, TYPE_CHANGE
        StructField("column_name", StringType(), True),
        StructField("previous_type", StringType(), True),
        StructField("new_type", StringType(), True)
    ])
    
    return spark.createDataFrame([], schema)


# =============================================================================
# INGESTION FUNCTIONS
# =============================================================================

def create_bronze_table(source_name: str, table_config: Dict[str, Any]):
    """
    Factory function to create DLT bronze table definitions dynamically.
    
    Args:
        source_name: Name of the source system
        table_config: Configuration dictionary for the table
    """
    table_name = table_config['name']
    full_table_name = f"bronze_{source_name}_{table_name}"
    
    @dlt.table(
        name=full_table_name,
        comment=f"Bronze layer data from {source_name}.{table_name}",
        table_properties={
            "quality": "bronze",
            "source": source_name,
            "pipelines.autoOptimize.managed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }
    )
    @dlt.expect_or_fail("valid_primary_key", f"{table_config.get('primary_key', 'id')} IS NOT NULL")
    def bronze_table():
        """Dynamic bronze table definition."""
        return IngestionExecutor().ingest_table(source_name, table_config)
    
    return bronze_table


class IngestionExecutor:
    """
    Handles the actual data ingestion logic for different source types.
    """
    
    def __init__(self):
        self.config_loader = ConfigLoader("config/pipeline_config.yaml")
        self.config = self.config_loader.load()
        self.audit_logger = AuditLogger(
            catalog=self.config['global']['catalog'],
            schema=self.config['global']['audit_schema']
        )
        self.batch_id = self._generate_batch_id()
        
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID."""
        return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    def ingest_table(self, source_name: str, table_config: Dict[str, Any]) -> DataFrame:
        """
        Main ingestion method that routes to appropriate source handler.
        
        Args:
            source_name: Name of the source system
            table_config: Table configuration
            
        Returns:
            DataFrame with ingested data
        """
        source_type = table_config.get('type', 'unknown')
        
        # Log batch start
        self.audit_logger.log_batch_start(
            batch_id=self.batch_id,
            source_name=source_name,
            table_name=table_config['name']
        )
        
        try:
            # Route to appropriate ingestion method
            if source_type == 'jdbc':
                df = self._ingest_jdbc(source_name, table_config)
            elif source_type == 'api':
                df = self._ingest_api(source_name, table_config)
            elif source_type == 'kafka':
                df = self._ingest_kafka(source_name, table_config)
            elif source_type == 'cloud_storage':
                df = self._ingest_cloud_storage(source_name, table_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Add audit columns
            df = self._add_audit_columns(df, source_name, table_config)
            
            # Log batch success
            record_count = df.count()
            self.audit_logger.log_batch_success(
                batch_id=self.batch_id,
                source_name=source_name,
                table_name=table_config['name'],
                records_read=record_count,
                records_written=record_count
            )
            
            return df
            
        except Exception as e:
            # Log batch failure
            self.audit_logger.log_batch_failure(
                batch_id=self.batch_id,
                source_name=source_name,
                table_name=table_config['name'],
                error_message=str(e)
            )
            raise
    
    def _ingest_jdbc(self, source_name: str, table_config: Dict[str, Any]) -> DataFrame:
        """Ingest data from JDBC source."""
        connection = table_config.get('connection', {})
        credentials = table_config.get('credentials', {})
        
        jdbc_url = f"jdbc:postgresql://{connection['host']}:{connection['port']}/{connection['database']}"
        
        # Build query based on load type
        source_table = table_config.get('source_table', '')
        load_type = table_config.get('load_type', 'full')
        
        if load_type == 'incremental':
            incremental_col = table_config.get('incremental_column')
            watermark = self._get_watermark(source_name, table_config['name'], incremental_col)
            query = f"(SELECT * FROM {source_table} WHERE {incremental_col} > '{watermark}') AS tmp"
        else:
            query = f"(SELECT * FROM {source_table}) AS tmp"
        
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", credentials.get('username')) \
            .option("password", credentials.get('password')) \
            .option("driver", connection.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _ingest_api(self, source_name: str, table_config: Dict[str, Any]) -> DataFrame:
        """Ingest data from REST API source."""
        # Implementation for API sources (Salesforce, etc.)
        # This would use appropriate connectors or REST API calls
        connection = table_config.get('connection', {})
        base_url = connection.get('base_url', '')
        
        # Placeholder - actual implementation would use API connector
        # For now, return empty DataFrame with expected schema
        return spark.createDataFrame([], StructType([]))
    
    def _ingest_kafka(self, source_name: str, table_config: Dict[str, Any]) -> DataFrame:
        """Ingest data from Kafka topic."""
        connection = table_config.get('connection', {})
        topic = table_config.get('topic', '')
        
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", connection.get('bootstrap_servers', '')) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("kafka.security.protocol", connection.get('security_protocol', 'PLAINTEXT')) \
            .load()
        
        # Parse JSON value
        df = df.select(
            F.from_json(F.col("value").cast("string"), self._get_schema(table_config)).alias("data"),
            F.col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        return df
    
    def _ingest_cloud_storage(self, source_name: str, table_config: Dict[str, Any]) -> DataFrame:
        """Ingest data from cloud storage (Azure Blob, S3, GCS)."""
        connection = table_config.get('connection', {})
        storage_account = connection.get('storage_account', '')
        container = connection.get('container', '')
        path = table_config.get('path', '')
        file_format = table_config.get('format', 'csv')
        
        full_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"
        
        # Get file pattern if specified
        file_pattern = table_config.get('file_pattern', '*')
        
        # Read with format-specific options
        reader = spark.read.format(file_format)
        
        # Apply format options
        options = table_config.get('options', {})
        for key, value in options.items():
            reader = reader.option(key, value)
        
        df = reader.load(f"{full_path}/{file_pattern}")
        
        return df
    
    def _add_audit_columns(self, df: DataFrame, source_name: str, table_config: Dict[str, Any]) -> DataFrame:
        """Add standard audit columns to the DataFrame."""
        return df \
            .withColumn("_ingestion_timestamp", F.current_timestamp()) \
            .withColumn("_batch_id", F.lit(self.batch_id)) \
            .withColumn("_source_system", F.lit(source_name)) \
            .withColumn("_source_table", F.lit(table_config.get('source_table', table_config.get('source_object', '')))) \
            .withColumn("_load_type", F.lit(table_config.get('load_type', 'unknown'))) \
            .withColumn("_pipeline_version", F.lit("1.0.0"))
    
    def _get_watermark(self, source_name: str, table_name: str, incremental_col: str) -> str:
        """Get the watermark value for incremental loads."""
        # Query audit table for last successful watermark
        try:
            watermark_df = spark.sql(f"""
                SELECT MAX(watermark_value) as watermark
                FROM {self.config['global']['catalog']}.{self.config['global']['audit_schema']}.audit_batch_runs
                WHERE source_name = '{source_name}'
                  AND table_name = '{table_name}'
                  AND status = 'SUCCESS'
                  AND watermark_value IS NOT NULL
            """)
            
            watermark_row = watermark_df.collect()[0]
            if watermark_row['watermark']:
                return watermark_row['watermark']
        except:
            pass
        
        # Default to epoch if no previous watermark
        return "1970-01-01 00:00:00"
    
    def _get_schema(self, table_config: Dict[str, Any]) -> StructType:
        """Get or infer schema for the table."""
        # This would typically load from a schema registry or config
        # For now, return a flexible schema
        return StructType([])  # Schema inference enabled


# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================

def apply_data_quality_checks(df: DataFrame, table_config: Dict[str, Any], batch_id: str) -> DataFrame:
    """
    Apply configured data quality checks to the DataFrame.
    
    Args:
        df: Input DataFrame
        table_config: Table configuration
        batch_id: Current batch ID
        
    Returns:
        DataFrame with quality check results
    """
    quality_config = table_config.get('data_quality', {})
    
    if not quality_config.get('enabled', False):
        return df
    
    rules = quality_config.get('rules', [])
    quarantine_enabled = quality_config.get('quarantine_enabled', True)
    
    for rule in rules:
        rule_name = rule.get('name')
        condition = rule.get('condition')
        severity = rule.get('severity', 'error')
        
        # Apply the check
        if severity == 'error':
            df = df.filter(condition)
        elif severity == 'warn':
            # Log warning but don't filter
            violation_count = df.filter(~F.expr(condition)).count()
            if violation_count > 0:
                print(f"WARNING: {violation_count} records violate rule '{rule_name}'")
    
    return df


# =============================================================================
# PIPELINE INITIALIZATION
# =============================================================================

def initialize_pipeline(config_path: str = "config/pipeline_config.yaml"):
    """
    Initialize the pipeline by creating all configured bronze tables.
    
    Args:
        config_path: Path to configuration file
    """
    config_loader = ConfigLoader(config_path)
    config = config_loader.load()
    
    # Create bronze tables for each source and table
    for source in config['sources']:
        source_name = source['name']
        for table in source['tables']:
            table_config = {**source, **table, 'source_name': source_name}
            create_bronze_table(source_name, table_config)
    
    print(f"Pipeline initialized with {len(config['sources'])} sources")


# Initialize when module is loaded
if __name__ == "__main__":
    initialize_pipeline()
