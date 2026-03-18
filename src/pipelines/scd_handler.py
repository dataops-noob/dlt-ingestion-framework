"""
SCD (Slowly Changing Dimensions) Handler for DLT Ingestion Framework
Supports SCD Type 1, Type 2, and Upsert patterns
"""

import dlt
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
from typing import Dict, Any, Optional, List


class SCDHandler:
    """
    Handles Slowly Changing Dimensions (SCD) patterns for Delta Live Tables.
    
    Supports:
    - SCD Type 0: Fixed dimensions (no changes allowed)
    - SCD Type 1: Overwrite (latest value only)
    - SCD Type 2: Historical tracking (add new row with effective dates)
    - SCD Type 3: Partial history (keep previous value in separate column)
    - Upsert: Merge based on primary key
    - Append: Insert only (no updates)
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def apply_scd(
        self,
        source_df: DataFrame,
        target_table: str,
        table_config: Dict[str, Any],
        batch_id: str
    ) -> DataFrame:
        """
        Apply the appropriate SCD pattern based on configuration.
        
        Args:
            source_df: Source data DataFrame
            target_table: Target table name
            table_config: Table configuration with SCD settings
            batch_id: Current batch ID
            
        Returns:
            DataFrame with SCD transformations applied
        """
        scd_type = table_config.get('scd_type', 'append').upper()
        
        # Add SCD metadata columns
        source_df = self._add_scd_metadata(source_df, batch_id)
        
        if scd_type == 'SCD0':
            return self._apply_scd_type_0(source_df, target_table, table_config)
        elif scd_type == 'SCD1':
            return self._apply_scd_type_1(source_df, target_table, table_config)
        elif scd_type == 'SCD2':
            return self._apply_scd_type_2(source_df, target_table, table_config)
        elif scd_type == 'SCD3':
            return self._apply_scd_type_3(source_df, target_table, table_config)
        elif scd_type == 'UPSERT':
            return self._apply_upsert(source_df, target_table, table_config)
        elif scd_type == 'APPEND':
            return self._apply_append(source_df, table_config)
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")
    
    def _add_scd_metadata(self, df: DataFrame, batch_id: str) -> DataFrame:
        """Add SCD metadata columns to the DataFrame."""
        return df \
            .withColumn("_scd_batch_id", F.lit(batch_id)) \
            .withColumn("_scd_ingestion_timestamp", F.current_timestamp()) \
            .withColumn("_scd_effective_date", F.current_timestamp()) \
            .withColumn("_scd_end_date", F.lit(None).cast(TimestampType())) \
            .withColumn("_scd_is_current", F.lit(True))
    
    def _apply_scd_type_0(self, source_df: DataFrame, target_table: str, 
                          table_config: Dict[str, Any]) -> DataFrame:
        """
        SCD Type 0: Fixed dimension - no changes allowed.
        Only insert new records, ignore updates to existing records.
        """
        primary_key = table_config.get('primary_key')
        
        # Only return records that don't exist in target
        # This is handled by DLT's expect logic
        return source_df \
            .withColumn("_scd_type", F.lit("SCD0")) \
            .withColumn("_scd_action", F.lit("INSERT"))
    
    def _apply_scd_type_1(self, source_df: DataFrame, target_table: str,
                          table_config: Dict[str, Any]) -> DataFrame:
        """
        SCD Type 1: Overwrite - keep only the latest value.
        Updates existing records in place, no history kept.
        """
        primary_key = table_config.get('primary_key')
        
        return source_df \
            .withColumn("_scd_type", F.lit("SCD1")) \
            .withColumn("_scd_action", 
                       F.when(F.col(f"{primary_key}").isNotNull(), "UPSERT")
                        .otherwise("INSERT"))
    
    def _apply_scd_type_2(self, source_df: DataFrame, target_table: str,
                          table_config: Dict[str, Any]) -> DataFrame:
        """
        SCD Type 2: Historical tracking - keep full history.
        Adds new row for each change with effective date range.
        """
        primary_key = table_config.get('primary_key')
        scd_columns = table_config.get('scd_columns', [])  # Columns to track changes
        
        # If no specific columns defined, track all non-key columns
        if not scd_columns:
            scd_columns = [c for c in source_df.columns 
                          if c not in [primary_key, '_scd_batch_id', 
                                      '_scd_ingestion_timestamp', '_scd_effective_date',
                                      '_scd_end_date', '_scd_is_current']]
        
        # Create hash of tracked columns for change detection
        hash_expr = F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in scd_columns]))
        
        return source_df \
            .withColumn("_scd_type", F.lit("SCD2")) \
            .withColumn("_scd_hash", hash_expr) \
            .withColumn("_scd_action", F.lit("SCD2_MERGE"))
    
    def _apply_scd_type_3(self, source_df: DataFrame, target_table: str,
                          table_config: Dict[str, Any]) -> DataFrame:
        """
        SCD Type 3: Partial history - keep previous value in separate column.
        Adds columns like 'previous_value' and 'change_date'.
        """
        primary_key = table_config.get('primary_key')
        scd_columns = table_config.get('scd_columns', [])
        
        # Add previous value columns
        for col in scd_columns:
            source_df = source_df \
                .withColumn(f"{col}_previous", F.lit(None).cast(source_df.schema[col].dataType)) \
                .withColumn(f"{col}_change_date", F.lit(None).cast(TimestampType()))
        
        return source_df \
            .withColumn("_scd_type", F.lit("SCD3")) \
            .withColumn("_scd_action", F.lit("UPSERT_WITH_HISTORY"))
    
    def _apply_upsert(self, source_df: DataFrame, target_table: str,
                     table_config: Dict[str, Any]) -> DataFrame:
        """
        Upsert pattern: Insert new records, update existing ones.
        Similar to SCD Type 1 but without the SCD metadata overhead.
        """
        primary_key = table_config.get('primary_key')
        
        return source_df \
            .withColumn("_scd_type", F.lit("UPSERT")) \
            .withColumn("_scd_action", F.lit("MERGE"))
    
    def _apply_append(self, source_df: DataFrame, table_config: Dict[str, Any]) -> DataFrame:
        """
        Append pattern: Insert only, no updates.
        Used for immutable data like events, logs, transactions.
        """
        return source_df \
            .withColumn("_scd_type", F.lit("APPEND")) \
            .withColumn("_scd_action", F.lit("INSERT"))
    
    def generate_scd2_merge_sql(
        self,
        target_table: str,
        source_table: str,
        primary_key: str,
        scd_columns: List[str]
    ) -> str:
        """
        Generate MERGE SQL for SCD Type 2 pattern.
        
        Args:
            target_table: Full target table name (catalog.schema.table)
            source_table: Source table or subquery
            primary_key: Primary key column
            scd_columns: Columns to track for changes
            
        Returns:
            MERGE SQL statement
        """
        # Build column list for hash comparison
        hash_cols = "||".join([f"CAST(s.{c} AS STRING)" for c in scd_columns])
        
        merge_sql = f"""
        MERGE INTO {target_table} t
        USING (
            SELECT 
                s.*,
                MD5(CONCAT_WS("||", {hash_cols})) as _scd_hash
            FROM {source_table} s
        ) s
        ON t.{primary_key} = s.{primary_key} AND t._scd_is_current = true
        
        -- Close current record when changed
        WHEN MATCHED AND t._scd_hash != s._scd_hash THEN
            UPDATE SET 
                _scd_end_date = current_timestamp(),
                _scd_is_current = false
        
        -- Insert new record for changes
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        return merge_sql


# =============================================================================
# DLT TABLE DEFINITIONS WITH SCD SUPPORT
# =============================================================================

def create_scd_table_definition(source_name: str, table_config: dict):
    """
    Create a DLT table definition with SCD support.
    
    Args:
        source_name: Name of the source system
        table_config: Configuration for the specific table including SCD settings
    """
    table_name = table_config['name']
    full_table_name = f"silver_{source_name}_{table_name}"
    scd_type = table_config.get('scd_type', 'append').upper()
    primary_key = table_config.get('primary_key', 'id')
    
    # Define expectations based on SCD type
    expectations = {}
    
    if scd_type == 'SCD0':
        # Type 0: No duplicates allowed
        expectations["unique_primary_key"] = f"{primary_key} IS NOT NULL"
    elif scd_type == 'SCD1':
        # Type 1: Primary key must be unique
        expectations["valid_primary_key"] = f"{primary_key} IS NOT NULL"
    elif scd_type == 'SCD2':
        # Type 2: Multiple rows per key allowed (different time periods)
        expectations["valid_dates"] = "_scd_effective_date IS NOT NULL"
    
    @dlt.table(
        name=full_table_name,
        comment=f"Silver layer: {source_name}.{table_name} (SCD {scd_type})",
        table_properties={
            "quality": "silver",
            "scd_type": scd_type,
            "source": source_name,
            "pipelines.autoOptimize.managed": "true",
            "delta.autoOptimize.optimizeWrite": "true"
        }
    )
    @dlt.expect_all(expectations)
    def scd_table():
        """Dynamic SCD table definition."""
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        # Read from bronze layer
        bronze_df = dlt.read(f"bronze_{source_name}_{table_name}")
        
        # Apply SCD transformations
        scd_handler = SCDHandler(spark)
        return scd_handler.apply_scd(
            source_df=bronze_df,
            target_table=full_table_name,
            table_config=table_config,
            batch_id="batch_from_config"
        )
    
    return scd_table


# =============================================================================
# SCD VIEW DEFINITIONS
# =============================================================================

def create_scd_current_view(source_name: str, table_config: dict):
    """
    Create a view showing only current records (for SCD Type 2 tables).
    
    Args:
        source_name: Name of the source system
        table_config: Table configuration
    """
    table_name = table_config['name']
    scd_type = table_config.get('scd_type', 'append').upper()
    
    if scd_type != 'SCD2':
        return None  # Only for SCD Type 2
    
    view_name = f"silver_{source_name}_{table_name}_current"
    
    @dlt.view(
        name=view_name,
        comment=f"Current records view for {table_name}"
    )
    def current_view():
        """View showing only current active records."""
        return dlt.read(f"silver_{source_name}_{table_name}") \
            .filter(F.col("_scd_is_current") == True)
    
    return current_view


def create_scd_history_view(source_name: str, table_config: dict):
    """
    Create a view showing full history (for SCD Type 2 tables).
    
    Args:
        source_name: Name of the source system
        table_config: Table configuration
    """
    table_name = table_config['name']
    scd_type = table_config.get('scd_type', 'append').upper()
    
    if scd_type != 'SCD2':
        return None
    
    view_name = f"silver_{source_name}_{table_name}_history"
    
    @dlt.view(
        name=view_name,
        comment=f"Full history view for {table_name}"
    )
    def history_view():
        """View showing all historical records."""
        return dlt.read(f"silver_{source_name}_{table_name}")
    
    return history_view
