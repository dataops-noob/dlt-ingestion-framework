"""
Delta Live Tables Definitions
This module defines all DLT tables dynamically based on configuration.
"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
sys.path.append('/Workspace/Users/shared/dlt-ingestion-framework')

from src.utils.config_loader import ConfigLoader
from src.pipelines.ingestion_pipeline import IngestionExecutor

# Load configuration
config_loader = ConfigLoader("config/pipeline_config.yaml")
config = config_loader.load()

# =============================================================================
# AUDIT TABLES (Bronze Layer)
# =============================================================================

@dlt.table(
    name="audit_batch_runs",
    comment="Audit log for all batch ingestion runs",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def audit_batch_runs():
    """
    Tracks all batch runs with start/end times, record counts, and status.
    """
    schema = StructType([
        StructField("batch_id", StringType(), False),
        StructField("source_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("run_start_time", TimestampType(), False),
        StructField("run_end_time", TimestampType(), True),
        StructField("status", StringType(), False),
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
        StructField("check_type", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_passed", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("failure_percentage", DoubleType(), False),
        StructField("severity", StringType(), False),
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
        StructField("change_type", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("previous_type", StringType(), True),
        StructField("new_type", StringType(), True)
    ])
    
    return spark.createDataFrame([], schema)


# =============================================================================
# DYNAMIC BRONZE TABLE GENERATION
# =============================================================================

def create_bronze_table_definition(source_name: str, table_config: dict):
    """
    Dynamically create a DLT bronze table definition.
    
    Args:
        source_name: Name of the source system
        table_config: Configuration for the specific table
    """
    table_name = table_config['name']
    full_table_name = f"bronze_{source_name}_{table_name}"
    primary_key = table_config.get('primary_key', 'id')
    
    # Define expectations based on config
    expectations = {}
    if table_config.get('primary_key'):
        expectations[f"valid_{primary_key}"] = f"{primary_key} IS NOT NULL"
    
    # Get data quality rules from config
    data_quality = config.get('data_quality', {})
    if data_quality.get('enabled', False):
        for rule in data_quality.get('rules', []):
            rule_name = rule.get('name', 'unnamed_rule')
            expectations[f"dq_{rule_name}"] = rule.get('condition', 'TRUE')
    
    @dlt.table(
        name=full_table_name,
        comment=f"Bronze layer: {source_name}.{table_name}",
        table_properties={
            "quality": "bronze",
            "source": source_name,
            "source_table": table_config.get('source_table', table_name),
            "pipelines.autoOptimize.managed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }
    )
    @dlt.expect_all(expectations)
    def bronze_table():
        """Dynamic bronze table ingestion."""
        executor = IngestionExecutor()
        return executor.ingest_table(source_name, table_config)
    
    # Create quarantine table if data quality is enabled
    if data_quality.get('quarantine_enabled', False):
        @dlt.table(
            name=f"{full_table_name}_quarantine",
            comment=f"Quarantined records from {full_table_name}",
            table_properties={
                "quality": "bronze",
                "pipelines.autoOptimize.managed": "true"
            }
        )
        def quarantine_table():
            """Table for records that failed data quality checks."""
            # This would be populated by a separate flow that captures failed records
            schema = StructType([
                StructField("record_data", StringType(), True),
                StructField("failed_rules", StringType(), True),
                StructField("quarantine_timestamp", TimestampType(), False),
                StructField("batch_id", StringType(), False)
            ])
            return spark.createDataFrame([], schema)
    
    return bronze_table


# =============================================================================
# GENERATE ALL CONFIGURED TABLES
# =============================================================================

# Create bronze tables for all configured sources and tables
for source in config.get('sources', []):
    source_name = source['name']
    source_type = source['type']
    
    for table in source.get('tables', []):
        # Merge source config with table config
        table_config = {**source, **table}
        table_config['source_name'] = source_name
        table_config['type'] = source_type
        
        # Create the bronze table definition
        create_bronze_table_definition(source_name, table_config)
        
        print(f"Created bronze table definition: bronze_{source_name}_{table['name']}")


# =============================================================================
# SILVER LAYER TABLES (Example)
# =============================================================================

@dlt.table(
    name="silver_customers",
    comment="Cleaned and enriched customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect("valid_email", "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'")
def silver_customers():
    """
    Silver layer transformation for customer data.
    Cleans and standardizes data from bronze layer.
    """
    return dlt.read("bronze_crm_system_customers") \
        .select(
            col("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            lower(trim(col("email"))).alias("email"),
            col("phone"),
            col("created_at"),
            col("updated_at"),
            col("_ingestion_timestamp"),
            col("_batch_id")
        ) \
        .dropDuplicates(["customer_id"])


@dlt.table(
    name="silver_orders",
    comment="Cleaned and enriched order data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect("positive_amount", "order_amount > 0")
def silver_orders():
    """
    Silver layer transformation for order data.
    """
    return dlt.read("bronze_crm_system_orders") \
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_date"),
            col("order_amount").cast("decimal(18,2)").alias("order_amount"),
            col("status"),
            col("created_at"),
            col("_ingestion_timestamp"),
            col("_batch_id")
        ) \
        .dropDuplicates(["order_id"])


# =============================================================================
# GOLD LAYER TABLES (Example)
# =============================================================================

@dlt.table(
    name="gold_customer_orders_summary",
    comment="Aggregated customer order metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customer_orders_summary():
    """
    Gold layer: Customer order summary metrics.
    """
    customers_df = dlt.read("silver_customers")
    orders_df = dlt.read("silver_orders")
    
    return customers_df.join(
        orders_df,
        customers_df.customer_id == orders_df.customer_id,
        "left"
    ).groupBy(
        customers_df.customer_id,
        customers_df.first_name,
        customers_df.last_name,
        customers_df.email
    ).agg(
        count("order_id").alias("total_orders"),
        sum("order_amount").alias("total_order_amount"),
        avg("order_amount").alias("avg_order_amount"),
        max("order_date").alias("last_order_date")
    )


@dlt.table(
    name="gold_daily_order_metrics",
    comment="Daily order metrics for reporting",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_daily_order_metrics():
    """
    Gold layer: Daily aggregated order metrics.
    """
    return dlt.read("silver_orders") \
        .groupBy(
            date_trunc("day", col("order_date")).alias("order_day")
        ).agg(
            count("*").alias("total_orders"),
            sum("order_amount").alias("total_revenue"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("order_amount").alias("avg_order_value")
        )
