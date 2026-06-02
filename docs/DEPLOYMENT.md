# Deployment Guide: Metadata-Driven CDC Pipeline

## Prerequisites
1. **Azure Resources Provisioned:**
   - Azure SQL Database (`clv-control-db` on `clv-control-sql-server`)
   - Azure Data Lake Storage Gen2 (`clvdatalakegen2`)
   - Azure Databricks Workspace (`adb-clv-databricks-workspace`)
   - Azure Data Factory
2. **Permissions:**
   - ADF Managed Identity needs `Storage Blob Data Contributor` on ADLS Gen2.
   - ADF Managed Identity needs `db_owner` or equivalent on Control DB.
3. **Source Systems:**
   - Change Tracking (CT) or CDC must be enabled on source databases and tables.

## Step-by-Step Deployment

### Phase 1: Database Setup
1. Connect to `clv-control-db` using SSMS or Azure Data Studio.
2. Execute `sql/control_tables/01_create_control_tables.sql` to create the schema and tables.
3. Execute `sql/control_tables/02_populate_control_tables.sql` to insert the 13 CLV entities.
4. Execute the three stored procedure scripts in `sql/stored_procedures/`.

### Phase 2: Databricks Setup
1. Import the notebooks from `databricks/notebooks/` into your Databricks workspace under `/Shared/`.
2. Create an interactive cluster (e.g., 13.3 LTS, Photon enabled).
3. Run `databricks/notebooks/setup/Mount_ADLS.py` (Optional if using direct ABFSS paths, but good for verification).
4. Run `databricks/notebooks/utilities/Validate_Dependencies.py` to ensure pyodbc and connectivity work.

### Phase 3: Azure Data Factory Setup
1. **Linked Services:**
   - Import the 3 JSON files from `adf/linkedService/`.
   - **CRITICAL:** Replace `{{PLACEHOLDER_SQL_USERNAME}}`, `{{PLACEHOLDER_SQL_PASSWORD}}`, `{{PLACEHOLDER_STORAGE_KEY}}`, `{{PLACEHOLDER_DATABRICKS_TOKEN}}`, and `{{PLACEHOLDER_CLUSTER_ID}}` with actual values or Key Vault references.
2. **Datasets:**
   - Import the 3 JSON files from `adf/dataset/`.
3. **Pipelines:**
   - Import `PL_Initial_Load_Single_Table.json`.
   - Import `PL_Incremental_CDC_Single_Table.json`.
   - Import `PL_Bronze_Orchestrator.json`.
   - Import `PL_Medallion_Master.json`.

### Phase 4: Execution & Verification
1. Trigger `PL_Medallion_Master` in ADF.
2. Monitor the pipeline. It should route all 13 tables to `PL_Initial_Load_Single_Table` because `initial_load_completed = 0`.
3. Verify in `clv-control-db`: