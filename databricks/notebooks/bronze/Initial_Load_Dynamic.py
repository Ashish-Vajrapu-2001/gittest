# Databricks notebook source
import pyodbc
from pyspark.sql.functions import current_timestamp, lit, col
import json

# COMMAND ----------

# Define widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("sql_server", "")
dbutils.widgets.text("sql_database", "")
dbutils.widgets.text("sql_username", "")
dbutils.widgets.text("sql_password", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

# COMMAND ----------

# Get parameters
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
table_id = dbutils.widgets.get("table_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# Storage authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# Define paths
# CRITICAL: Read from STAGING path (Parquet)
staging_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/staging/{source_system}/{schema_name}/{table_name}/initial/{pipeline_run_id}/"

# CRITICAL: Write to DELTA TABLE path
delta_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/"

print(f"Reading from Staging: {staging_path}")
print(f"Writing to Delta: {delta_path}")

# COMMAND ----------

# Read Parquet from staging
try:
    df = spark.read.format("parquet").load(staging_path)
    records_loaded = df.count()
    print(f"Loaded {records_loaded} records from staging.")
except Exception as e:
    print(f"Failed to read staging data: {str(e)}")
    raise e

# COMMAND ----------

# Extract current sync version from the data (added by SQL query in ADF)
if "_current_sync_version" in df.columns:
    current_sync_version = df.select("_current_sync_version").first()[0]
    # Drop the column as we don't need it in the final table
    df = df.drop("_current_sync_version")
else:
    current_sync_version = 0
    print("Warning: _current_sync_version not found in source data.")

# COMMAND ----------

# Add metadata columns
df_with_metadata = df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_system_code", lit(source_system)) \
    .withColumn("_is_deleted", lit(False)) \
    .withColumn("_cdc_operation", lit("I"))

# COMMAND ----------

# Write to Delta table (Overwrite for initial load)
try:
    df_with_metadata.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(delta_path)
    print(f"Successfully wrote to Delta table at {delta_path}")
except Exception as e:
    print(f"Failed to write Delta table: {str(e)}")
    raise e

# COMMAND ----------

# OPTIMIZE with Z-ORDER for query performance
try:
    spark.sql(f"""
        OPTIMIZE delta.`{delta_path}`
        ZORDER BY ({primary_key_columns}, _load_timestamp)
    """)
    print(f"Optimized Delta table: {delta_path}")
except Exception as e:
    print(f"Warning: Optimization failed: {str(e)}")

# COMMAND ----------

# Update Control Database
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Call stored procedure to update metadata
    # CRITICAL: @MarkInitialLoadComplete = 1
    exec_sp = """
        EXEC control.sp_UpdateTableMetadata 
            @TableId = ?, 
            @Status = ?, 
            @PipelineRunId = ?, 
            @RecordsLoaded = ?, 
            @SyncVersion = ?, 
            @MarkInitialLoadComplete = ?,
            @ErrorMessage = ?
    """
    
    cursor.execute(exec_sp, (
        int(table_id), 
        'SUCCESS', 
        pipeline_run_id, 
        records_loaded, 
        current_sync_version, 
        1, 
        None
    ))
    
    conn.commit()
    print("Successfully updated control database.")
    
except Exception as e:
    print(f"Failed to update control database: {str(e)}")
    if 'conn' in locals():
        conn.rollback()
    raise e
finally:
    if 'conn' in locals():
        conn.close()

# COMMAND ----------

# Clean up staging files (Optional but recommended)
try:
    dbutils.fs.rm(staging_path, recurse=True)
    print(f"Cleaned up staging path: {staging_path}")
except Exception as e:
    print(f"Warning: Failed to clean up staging path: {str(e)}")

# COMMAND ----------

# Return success JSON
result = {
    "status": "SUCCESS",
    "records_loaded": records_loaded,
    "sync_version": current_sync_version,
    "delta_path": delta_path
}
dbutils.notebook.exit(json.dumps(result))