# Silver Layer Deployment Guide

## Prerequisites
1. **Bronze Layer Deployed**: The Bronze layer pipelines and control tables must be fully deployed and operational.
2. **Storage Account**: ADLS Gen2 must be provisioned with `bronze` and `silver` containers.
3. **Databricks Workspace**: Cluster configured with appropriate access to ADLS Gen2.
4. **Azure SQL Database**: `ControlDB` must be accessible from ADF.

## Deployment Steps

### 1. Database Setup
Execute the SQL scripts in the following order against the `ControlDB`:
1. `sql/silver_control_tables/01_create_silver_control_tables.sql`
2. `sql/silver_control_tables/02_populate_silver_config.sql`
3. `sql/silver_stored_procedures/sp_UpdateSilverTableStatus.sql`
4. `sql/silver_stored_procedures/sp_GetSilverTransformationRules.sql`

### 2. Databricks Setup
1. Open Databricks Workspace.
2. Navigate to `/Shared/silver/`.
3. Import the following notebooks:
   - `Silver_Transform_Dynamic.py`
   - `Silver_Data_Quality.py`
   - `Silver_Schema_Evolution.py`

### 3. Azure Data Factory Setup
1. **Linked Services**: Deploy `LS_AzureDataLakeStorage.json`, `LS_AzureDatabricks.json`, and `LS_AzureSQL_Control.json`. Ensure placeholders (e.g., `{{PLACEHOLDER_STORAGE_KEY}}`) are replaced with actual Key Vault references.
2. **Datasets**: Deploy `DS_Delta_Bronze.json`, `DS_Delta_Silver.json`, and `DS_ControlDB_Silver.json`.
3. **Pipelines**: 
   - Deploy `PL_Silver_Transform_Single_Table.json`
   - Deploy `PL_Silver_Orchestrator.json`
   - Deploy `PL_Medallion_Master.json`

### 4. Validation
1. Trigger `PL_Medallion_Master` with `run_bronze = false` and `run_silver = true`.
2. Monitor the pipeline execution in ADF Monitor.
3. Verify data in ADLS Gen2 under `silver/` container.
4. Check `control.silver_execution_log` in Azure SQL for success metrics.