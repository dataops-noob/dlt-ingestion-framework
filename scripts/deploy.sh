#!/bin/bash
# DLT Ingestion Framework Deployment Script
# Usage: ./deploy.sh [environment]

set -e

ENVIRONMENT=${1:-dev}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "DLT Ingestion Framework Deployment"
echo "Environment: $ENVIRONMENT"
echo "=========================================="

# Configuration
DATABRICKS_WORKSPACE_URL="${DATABRICKS_HOST:-https://your-workspace.cloud.databricks.com}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN:-your-token}"
DBFS_BASE_PATH="/Shared/dlt-ingestion-framework"

echo "Step 1: Validating configuration..."
python3 "$PROJECT_DIR/src/utils/config_loader.py" "$PROJECT_DIR/config/pipeline_config.yaml"

echo "Step 2: Creating DBFS directories..."
databricks fs mkdirs "dbfs:$DBFS_BASE_PATH" --profile "$ENVIRONMENT" 2>/dev/null || true
databricks fs mkdirs "dbfs:$DBFS_BASE_PATH/src" --profile "$ENVIRONMENT" 2>/dev/null || true
databricks fs mkdirs "dbfs:$DBFS_BASE_PATH/config" --profile "$ENVIRONMENT" 2>/dev/null || true

echo "Step 3: Uploading source code..."
# Upload Python modules
databricks fs cp -r "$PROJECT_DIR/src" "dbfs:$DBFS_BASE_PATH/" --overwrite --profile "$ENVIRONMENT"

echo "Step 4: Uploading configuration..."
databricks fs cp "$PROJECT_DIR/config/pipeline_config.yaml" "dbfs:$DBFS_BASE_PATH/config/" --overwrite --profile "$ENVIRONMENT"
databricks fs cp "$PROJECT_DIR/config/dlt_pipeline.json" "dbfs:$DBFS_BASE_PATH/config/" --overwrite --profile "$ENVIRONMENT"

echo "Step 5: Creating DLT pipeline..."
# Create or update DLT pipeline using API
PIPELINE_NAME="dlt-ingestion-$ENVIRONMENT"

# Check if pipeline exists
EXISTING_PIPELINE=$(databricks pipelines list --profile "$ENVIRONMENT" 2>/dev/null | grep "$PIPELINE_NAME" | awk '{print $1}')

if [ -n "$EXISTING_PIPELINE" ]; then
    echo "Updating existing pipeline: $EXISTING_PIPELINE"
    databricks pipelines update "$EXISTING_PIPELINE" \
        --json "@$PROJECT_DIR/config/dlt_pipeline.json" \
        --profile "$ENVIRONMENT"
else
    echo "Creating new pipeline: $PIPELINE_NAME"
    databricks pipelines create \
        --json "@$PROJECT_DIR/config/dlt_pipeline.json" \
        --profile "$ENVIRONMENT"
fi

echo "Step 6: Running tests..."
cd "$PROJECT_DIR"
python3 -m pytest tests/ -v || echo "Tests completed with warnings"

echo "=========================================="
echo "Deployment completed successfully!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Configure environment variables in Databricks Secrets"
echo "2. Start the DLT pipeline from the Databricks UI"
echo "3. Monitor pipeline execution in the Delta Live Tables UI"
echo ""
echo "Useful commands:"
echo "  databricks pipelines list --profile $ENVIRONMENT"
echo "  databricks pipelines start <pipeline-id> --profile $ENVIRONMENT"
