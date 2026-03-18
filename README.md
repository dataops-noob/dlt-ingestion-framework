# DLT Ingestion Framework

A configuration-driven data ingestion framework built on Delta Live Tables (DLT) that provides end-to-end data pipelines with built-in auditing, data quality checks, and monitoring.

## Features

- **Configuration-Driven**: Define all sources, tables, and ingestion parameters in YAML
- **Multi-Source Support**: JDBC, Kafka, REST APIs, and Cloud Storage (Azure Blob, S3, GCS)
- **Built-in Auditing**: Comprehensive audit logging for all batch runs, data quality checks, and schema evolution
- **Data Quality**: Configurable rules with quarantine support
- **Medallion Architecture**: Bronze, Silver, and Gold layer implementations
- **Incremental & Full Loads**: Support for both incremental and full refresh patterns
- **Schema Evolution**: Automatic tracking of schema changes
- **Monitoring**: Built-in metrics and alerting capabilities

## Project Structure

```
dlt-ingestion-framework/
├── databricks.yml              # Databricks Asset Bundle configuration
├── config/
│   ├── pipeline_config.yaml    # Main configuration file
│   └── dlt_pipeline.json       # DLT pipeline definition
├── resources/
│   ├── dlt_pipeline.yml        # DLT pipeline resource definition
│   └── schemas.yml             # Unity Catalog schema definitions
├── src/
│   ├── pipelines/
│   │   ├── ingestion_pipeline.py   # Core ingestion logic
│   │   └── dlt_definitions.py      # DLT table definitions
│   ├── utils/
│   │   ├── config_loader.py        # Configuration management
│   │   └── connection_manager.py   # Source connectors
│   └── audit/
│       └── audit_logger.py         # Audit logging
├── tests/
│   └── test_ingestion_framework.py # Unit tests
├── scripts/
│   └── deploy.sh                   # Deployment script
├── .github/
│   └── workflows/
│       └── databricks-bundle-cicd.yml  # GitHub Actions CI/CD
└── README.md
```

## Quick Start

### Option 1: Databricks Asset Bundles (Recommended)

Databricks Asset Bundles (DAB) provide a declarative way to define and deploy your DLT pipelines.

#### 1. Install Databricks CLI

```bash
pip install databricks-cli
```

#### 2. Configure Databricks CLI

```bash
databricks configure
# Enter your Databricks host and personal access token
```

#### 3. Initialize Bundle

```bash
cd dlt-ingestion-framework
databricks bundle validate
```

#### 4. Deploy to Dev

```bash
databricks bundle deploy -t dev
```

#### 5. Run Pipeline

```bash
databricks bundle run -t dev run_dlt_ingestion
```

### Option 2: Manual Deployment

#### 1. Configuration

Edit `config/pipeline_config.yaml` to define your sources:

```yaml
global:
  catalog: "data_platform"
  schema_prefix: "bronze"
  checkpoint_root: "/mnt/dlt/checkpoints"
  audit_schema: "audit"

sources:
  - name: "crm_system"
    type: "jdbc"
    connection:
      host: "${CRM_DB_HOST}"
      port: 5432
      database: "crm_prod"
    credentials:
      username: "${CRM_DB_USER}"
      password: "${CRM_DB_PASSWORD}"
    tables:
      - name: "customers"
        source_table: "public.customers"
        primary_key: "customer_id"
        incremental_column: "updated_at"
        load_type: "incremental"
```

### 2. Set Environment Variables

```bash
export CRM_DB_HOST=your-db-host
export CRM_DB_USER=your-username
export CRM_DB_PASSWORD=your-password
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
```

### 3. Deploy

```bash
./scripts/deploy.sh dev
```

### 4. Start Pipeline

Start the pipeline from the Databricks Delta Live Tables UI or via API:

```bash
databricks pipelines start <pipeline-id>
```

## Databricks Asset Bundles (DAB)

This project uses Databricks Asset Bundles for infrastructure-as-code deployment. The bundle configuration is defined in `databricks.yml`.

### Bundle Structure

- **databricks.yml**: Root bundle configuration with variables and targets
- **resources/dlt_pipeline.yml**: DLT pipeline and job definitions
- **resources/schemas.yml**: Unity Catalog schema and permissions

### Bundle Commands

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to specific target
databricks bundle deploy -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod

# Run jobs
databricks bundle run -t dev run_dlt_ingestion
databricks bundle run -t dev data_quality_checks

# Destroy resources
databricks bundle destroy -t dev --auto-approve
```

### CI/CD with GitHub Actions

The project includes a GitHub Actions workflow (`.github/workflows/databricks-bundle-cicd.yml`) that:

1. Validates the bundle on every PR
2. Deploys to dev on pushes to `develop` branch
3. Deploys to staging on pushes to `main` branch
4. Deploys to production after staging approval

Required GitHub Secrets:
- `DATABRICKS_HOST`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Databricks personal access token
- `SLACK_WEBHOOK_URL` (optional): For deployment notifications

## Configuration Reference

### Source Types

#### JDBC
```yaml
type: "jdbc"
connection:
  host: "db.example.com"
  port: 5432
  database: "production"
  driver: "org.postgresql.Driver"
credentials:
  username: "${DB_USER}"
  password: "${DB_PASS}"
```

#### Kafka
```yaml
type: "kafka"
connection:
  bootstrap_servers: "kafka:9092"
  security_protocol: "SASL_SSL"
credentials:
  username: "${KAFKA_USER}"
  password: "${KAFKA_PASS}"
tables:
  - name: "events"
    topic: "user.events"
    load_type: "streaming"
```

#### Cloud Storage (Azure Blob)
```yaml
type: "cloud_storage"
connection:
  storage_account: "${AZURE_STORAGE_ACCOUNT}"
  container: "raw-data"
credentials:
  client_id: "${AZURE_CLIENT_ID}"
  client_secret: "${AZURE_CLIENT_SECRET}"
  tenant_id: "${AZURE_TENANT_ID}"
tables:
  - name: "uploads"
    path: "uploads/csv/"
    format: "csv"
```

### Load Types

- **full**: Complete table refresh on each run
- **incremental**: Only new/changed records based on watermark column
- **streaming**: Continuous ingestion from streaming sources

### Data Quality Rules

```yaml
data_quality:
  enabled: true
  quarantine_enabled: true
  rules:
    - name: "not_null"
      condition: "{col} IS NOT NULL"
      severity: "error"
    - name: "positive_value"
      condition: "{col} > 0"
      severity: "warn"
```

## Audit Tables

The framework automatically creates and maintains the following audit tables:

### audit_batch_runs
Tracks all batch ingestion runs with:
- Batch ID, source, and table names
- Start/end timestamps and duration
- Record counts (read/written/failed)
- Status and error messages
- Watermark values for incremental loads

### audit_data_quality
Tracks data quality check results:
- Rule names and types
- Pass/fail counts and percentages
- Severity levels
- Quarantine status

### audit_schema_evolution
Tracks schema changes over time:
- Previous and new schema definitions
- Change types (ADD_COLUMN, DROP_COLUMN, TYPE_CHANGE)
- Timestamps for each change

### audit_lineage
Tracks data lineage:
- Source to target mappings
- Transformation logic
- Record counts at each stage

## Monitoring

### Pipeline Metrics

Query pipeline metrics:
```sql
SELECT * FROM data_platform.audit.audit_batch_runs
WHERE status = 'FAILED'
ORDER BY run_start_time DESC
```

### Data Quality Summary

```sql
SELECT 
    source_name,
    table_name,
    rule_name,
    AVG(failure_percentage) as avg_failure_rate
FROM data_platform.audit.audit_data_quality
GROUP BY source_name, table_name, rule_name
```

## Development

### Running Tests

```bash
cd dlt-ingestion-framework
python3 -m pytest tests/ -v
```

### Adding New Source Types

1. Create a new connector class in `src/utils/connection_manager.py`
2. Add ingestion logic in `src/pipelines/ingestion_pipeline.py`
3. Update configuration validation in `src/utils/config_loader.py`

## Best Practices

1. **Use Incremental Loads**: For large tables, use incremental loads with appropriate watermark columns
2. **Configure Data Quality**: Enable data quality checks to catch issues early
3. **Monitor Audit Tables**: Regularly check audit tables for failures and trends
4. **Secure Credentials**: Use Databricks Secrets or environment variables for credentials
5. **Test Connections**: Use the connection test feature before deploying
6. **Version Control**: Keep configuration in version control with environment-specific values externalized

## Troubleshooting

### Common Issues

**Connection Failures**
- Verify credentials and network access
- Check firewall rules and security groups
- Test connections using the connection manager

**Schema Evolution Errors**
- Review `audit_schema_evolution` table for recent changes
- Consider enabling schema evolution in Delta table properties

**Data Quality Failures**
- Check `audit_data_quality` for specific rule failures
- Review quarantine tables for failed records

## License

MIT License - See LICENSE file for details
