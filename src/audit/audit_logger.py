"""
Audit logging module for tracking all ingestion activities.
Provides comprehensive logging for batch runs, data quality checks, and schema evolution.
"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
from typing import Dict, Any, Optional, List
import uuid


class AuditLogger:
    """
    Handles all audit logging for the DLT Ingestion Framework.
    Logs batch runs, data quality checks, and schema changes.
    """
    
    def __init__(self, catalog: str, schema: str):
        """
        Initialize the audit logger.
        
        Args:
            catalog: Unity Catalog name
            schema: Schema name for audit tables
        """
        self.catalog = catalog
        self.schema = schema
        self.spark = SparkSession.getActiveSession()
        
        # Ensure audit tables exist
        self._initialize_audit_tables()
    
    def _initialize_audit_tables(self) -> None:
        """Create audit tables if they don't exist."""
        # Create schema if not exists
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}
        """)
        
        # Create audit_batch_runs table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.audit_batch_runs (
                batch_id STRING NOT NULL,
                source_name STRING NOT NULL,
                table_name STRING NOT NULL,
                run_start_time TIMESTAMP NOT NULL,
                run_end_time TIMESTAMP,
                status STRING NOT NULL,
                records_read BIGINT,
                records_written BIGINT,
                records_failed BIGINT,
                error_message STRING,
                watermark_value TIMESTAMP,
                pipeline_version STRING NOT NULL,
                spark_version STRING NOT NULL,
                cluster_id STRING,
                execution_duration_ms BIGINT
            ) USING DELTA
        """)
        
        # Create audit_data_quality table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.audit_data_quality (
                check_id STRING NOT NULL,
                batch_id STRING NOT NULL,
                source_name STRING NOT NULL,
                table_name STRING NOT NULL,
                check_timestamp TIMESTAMP NOT NULL,
                rule_name STRING NOT NULL,
                column_name STRING,
                check_type STRING NOT NULL,
                records_checked BIGINT NOT NULL,
                records_passed BIGINT NOT NULL,
                records_failed BIGINT NOT NULL,
                failure_percentage DOUBLE NOT NULL,
                severity STRING NOT NULL,
                is_quarantined BOOLEAN NOT NULL
            ) USING DELTA
        """)
        
        # Create audit_schema_evolution table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.audit_schema_evolution (
                evolution_id STRING NOT NULL,
                batch_id STRING NOT NULL,
                source_name STRING NOT NULL,
                table_name STRING NOT NULL,
                change_timestamp TIMESTAMP NOT NULL,
                previous_schema STRING NOT NULL,
                new_schema STRING NOT NULL,
                change_type STRING NOT NULL,
                column_name STRING,
                previous_type STRING,
                new_type STRING
            ) USING DELTA
        """)
        
        # Create audit_lineage table for data lineage tracking
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.audit_lineage (
                lineage_id STRING NOT NULL,
                batch_id STRING NOT NULL,
                source_name STRING NOT NULL,
                source_table STRING NOT NULL,
                target_catalog STRING NOT NULL,
                target_schema STRING NOT NULL,
                target_table STRING NOT NULL,
                operation STRING NOT NULL,
                operation_timestamp TIMESTAMP NOT NULL,
                source_record_count BIGINT,
                target_record_count BIGINT,
                transformation_logic STRING
            ) USING DELTA
        """)
    
    def _generate_id(self, prefix: str = "id") -> str:
        """Generate a unique ID."""
        return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    def log_batch_start(self, batch_id: str, source_name: str, table_name: str) -> None:
        """
        Log the start of a batch run.
        
        Args:
            batch_id: Unique batch identifier
            source_name: Name of the source system
            table_name: Name of the table being ingested
        """
        spark_version = self.spark.conf.get("spark.app.name", "unknown")
        cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
        
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.audit_batch_runs (
                batch_id, source_name, table_name, run_start_time, status,
                pipeline_version, spark_version, cluster_id
            ) VALUES (
                '{batch_id}', '{source_name}', '{table_name}', 
                current_timestamp(), 'RUNNING',
                '1.0.0', '{spark_version}', '{cluster_id}'
            )
        """)
    
    def log_batch_success(self, batch_id: str, source_name: str, table_name: str,
                         records_read: int, records_written: int,
                         watermark_value: Optional[str] = None) -> None:
        """
        Log successful completion of a batch run.
        
        Args:
            batch_id: Unique batch identifier
            source_name: Name of the source system
            table_name: Name of the table being ingested
            records_read: Number of records read from source
            records_written: Number of records written to target
            watermark_value: Watermark value for incremental loads
        """
        watermark_clause = f"'{watermark_value}'" if watermark_value else "NULL"
        
        self.spark.sql(f"""
            UPDATE {self.catalog}.{self.schema}.audit_batch_runs
            SET 
                run_end_time = current_timestamp(),
                status = 'SUCCESS',
                records_read = {records_read},
                records_written = {records_written},
                records_failed = {records_read - records_written},
                watermark_value = {watermark_clause},
                execution_duration_ms = 
                    CAST((current_timestamp() - run_start_time) * 1000 AS BIGINT)
            WHERE batch_id = '{batch_id}'
        """)
    
    def log_batch_failure(self, batch_id: str, source_name: str, table_name: str,
                         error_message: str) -> None:
        """
        Log failure of a batch run.
        
        Args:
            batch_id: Unique batch identifier
            source_name: Name of the source system
            table_name: Name of the table being ingested
            error_message: Error message describing the failure
        """
        # Escape single quotes in error message
        escaped_error = error_message.replace("'", "''")
        
        self.spark.sql(f"""
            UPDATE {self.catalog}.{self.schema}.audit_batch_runs
            SET 
                run_end_time = current_timestamp(),
                status = 'FAILED',
                error_message = '{escaped_error}',
                execution_duration_ms = 
                    CAST((current_timestamp() - run_start_time) * 1000 AS BIGINT)
            WHERE batch_id = '{batch_id}'
        """)
    
    def log_data_quality_check(self, batch_id: str, source_name: str, table_name: str,
                              rule_name: str, check_type: str,
                              records_checked: int, records_passed: int,
                              column_name: Optional[str] = None,
                              severity: str = "error",
                              is_quarantined: bool = False) -> None:
        """
        Log results of a data quality check.
        
        Args:
            batch_id: Unique batch identifier
            source_name: Name of the source system
            table_name: Name of the table being ingested
            rule_name: Name of the quality rule
            check_type: Type of check (NOT_NULL, RANGE, CUSTOM, etc.)
            records_checked: Total records checked
            records_passed: Records that passed the check
            column_name: Column being checked (optional)
            severity: Severity level (error or warn)
            is_quarantined: Whether failed records were quarantined
        """
        check_id = self._generate_id("dq")
        records_failed = records_checked - records_passed
        failure_percentage = (records_failed / records_checked * 100) if records_checked > 0 else 0
        
        col_clause = f"'{column_name}'" if column_name else "NULL"
        
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.audit_data_quality (
                check_id, batch_id, source_name, table_name, check_timestamp,
                rule_name, column_name, check_type, records_checked, records_passed,
                records_failed, failure_percentage, severity, is_quarantined
            ) VALUES (
                '{check_id}', '{batch_id}', '{source_name}', '{table_name}',
                current_timestamp(), '{rule_name}', {col_clause}, '{check_type}',
                {records_checked}, {records_passed}, {records_failed},
                {failure_percentage}, '{severity}', {str(is_quarantined).lower()}
            )
        """)
    
    def log_schema_evolution(self, batch_id: str, source_name: str, table_name: str,
                            previous_schema: str, new_schema: str,
                            change_type: str, column_name: Optional[str] = None,
                            previous_type: Optional[str] = None,
                            new_type: Optional[str] = None) -> None:
        """
        Log schema evolution events.
        
        Args:
            batch_id: Unique batch identifier
            source_name: Name of the source system
            table_name: Name of the table being ingested
            previous_schema: JSON string of previous schema
            new_schema: JSON string of new schema
            change_type: Type of change (ADD_COLUMN, DROP_COLUMN, TYPE_CHANGE)
            column_name: Name of the column affected (optional)
            previous_type: Previous data type (optional)
            new_type: New data type (optional)
        """
        evolution_id = self._generate_id("evol")
        
        # Escape single quotes in schema strings
        prev_schema = previous_schema.replace("'", "''")
        new_schema_clean = new_schema.replace("'", "''")
        
        col_clause = f"'{column_name}'" if column_name else "NULL"
        prev_type_clause = f"'{previous_type}'" if previous_type else "NULL"
        new_type_clause = f"'{new_type}'" if new_type else "NULL"
        
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.audit_schema_evolution (
                evolution_id, batch_id, source_name, table_name, change_timestamp,
                previous_schema, new_schema, change_type, column_name,
                previous_type, new_type
            ) VALUES (
                '{evolution_id}', '{batch_id}', '{source_name}', '{table_name}',
                current_timestamp(), '{prev_schema}', '{new_schema_clean}',
                '{change_type}', {col_clause}, {prev_type_clause}, {new_type_clause}
            )
        """)
    
    def log_lineage(self, batch_id: str, source_name: str, source_table: str,
                   target_catalog: str, target_schema: str, target_table: str,
                   operation: str, source_record_count: int, target_record_count: int,
                   transformation_logic: Optional[str] = None) -> None:
        """
        Log data lineage information.
        
        Args:
            batch_id: Unique batch identifier
            source_name: Name of the source system
            source_table: Source table name
            target_catalog: Target catalog name
            target_schema: Target schema name
            target_table: Target table name
            operation: Type of operation (INSERT, UPDATE, MERGE, etc.)
            source_record_count: Number of records from source
            target_record_count: Number of records written to target
            transformation_logic: Description of transformations applied (optional)
        """
        lineage_id = self._generate_id("lineage")
        
        logic_clause = f"'{transformation_logic.replace(\"'\", \"''\")}'" if transformation_logic else "NULL"
        
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.audit_lineage (
                lineage_id, batch_id, source_name, source_table,
                target_catalog, target_schema, target_table, operation,
                operation_timestamp, source_record_count, target_record_count,
                transformation_logic
            ) VALUES (
                '{lineage_id}', '{batch_id}', '{source_name}', '{source_table}',
                '{target_catalog}', '{target_schema}', '{target_table}',
                '{operation}', current_timestamp(), {source_record_count},
                {target_record_count}, {logic_clause}
            )
        """)
    
    def get_batch_history(self, source_name: Optional[str] = None,
                         table_name: Optional[str] = None,
                         limit: int = 100) -> DataFrame:
        """
        Get batch run history.
        
        Args:
            source_name: Filter by source name (optional)
            table_name: Filter by table name (optional)
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with batch history
        """
        query = f"""
            SELECT * FROM {self.catalog}.{self.schema}.audit_batch_runs
            WHERE 1=1
        """
        
        if source_name:
            query += f" AND source_name = '{source_name}'"
        if table_name:
            query += f" AND table_name = '{table_name}'"
        
        query += f" ORDER BY run_start_time DESC LIMIT {limit}"
        
        return self.spark.sql(query)
    
    def get_data_quality_summary(self, source_name: Optional[str] = None,
                                 days: int = 7) -> DataFrame:
        """
        Get data quality summary statistics.
        
        Args:
            source_name: Filter by source name (optional)
            days: Number of days to look back
            
        Returns:
            DataFrame with quality summary
        """
        query = f"""
            SELECT 
                source_name,
                table_name,
                rule_name,
                COUNT(*) as total_checks,
                SUM(records_checked) as total_records_checked,
                SUM(records_failed) as total_records_failed,
                AVG(failure_percentage) as avg_failure_percentage,
                SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) as error_count,
                SUM(CASE WHEN severity = 'warn' THEN 1 ELSE 0 END) as warn_count
            FROM {self.catalog}.{self.schema}.audit_data_quality
            WHERE check_timestamp >= current_timestamp() - INTERVAL {days} DAYS
        """
        
        if source_name:
            query += f" AND source_name = '{source_name}'"
        
        query += " GROUP BY source_name, table_name, rule_name"
        
        return self.spark.sql(query)
    
    def get_failed_batches(self, hours: int = 24) -> DataFrame:
        """
        Get list of failed batches.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            DataFrame with failed batch details
        """
        return self.spark.sql(f"""
            SELECT 
                batch_id,
                source_name,
                table_name,
                run_start_time,
                run_end_time,
                error_message,
                execution_duration_ms
            FROM {self.catalog}.{self.schema}.audit_batch_runs
            WHERE status = 'FAILED'
              AND run_start_time >= current_timestamp() - INTERVAL {hours} HOURS
            ORDER BY run_start_time DESC
        """)
    
    def get_pipeline_metrics(self, days: int = 30) -> Dict[str, Any]:
        """
        Get overall pipeline metrics.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with pipeline metrics
        """
        metrics_df = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_batches,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_batches,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_batches,
                SUM(records_read) as total_records_read,
                SUM(records_written) as total_records_written,
                AVG(execution_duration_ms) as avg_duration_ms
            FROM {self.catalog}.{self.schema}.audit_batch_runs
            WHERE run_start_time >= current_timestamp() - INTERVAL {days} DAYS
        """)
        
        row = metrics_df.collect()[0]
        
        return {
            'total_batches': row['total_batches'],
            'successful_batches': row['successful_batches'],
            'failed_batches': row['failed_batches'],
            'success_rate': (row['successful_batches'] / row['total_batches'] * 100) 
                          if row['total_batches'] > 0 else 0,
            'total_records_read': row['total_records_read'],
            'total_records_written': row['total_records_written'],
            'avg_duration_ms': row['avg_duration_ms']
        }
