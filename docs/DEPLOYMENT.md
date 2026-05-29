# Deployment Guide: Metadata-Driven CDC Pipeline (Bronze Layer)

## Prerequisites
1. **Azure Resources**:
   - Azure SQL Database (Control DB & Source DBs)
   - Azure Data Lake Storage Gen2 (ADLS Gen2)
   - Azure Data Factory (ADF)
   - Azure Databricks Workspace
   - Azure Key Vault (for secrets)
2. **Permissions**:
   - ADF Managed Identity needs `Storage Blob Data Contributor` on ADLS.
   - ADF Managed Identity needs `db_owner` or execute permissions on Control DB.
3. **Source Systems**:
   - Change Tracking (CT) or CDC must be enabled on all source databases and tables.