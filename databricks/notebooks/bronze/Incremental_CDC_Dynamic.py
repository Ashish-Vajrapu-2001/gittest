# Databricks notebook source
import pyodbc
from pyspark.sql.functions import current_timestamp, lit, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json

# COMMAND ----------

# Define widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("last_sync_version", "")
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
last_sync_version = dbutils.widgets.get("last_sync_version")
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
staging_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/staging/{source_system}/{schema_name}/{table_name}/incremental/{pipeline_run_id}/"

# CRITICAL: Write to DELTA TABLE path
delta_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/"

print(f"Reading CDC from Staging: {staging_path}")
print(f"Merging to Delta: {delta_path}")

# COMMAND ----------

# Read Parquet from staging
try:
    source_df = spark.read.format("parquet").load(staging_path)
    records_processed = source_df.count()
    print(f"Loaded {records_processed} CDC records from staging.")
    
    if records_processed == 0:
        dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "records_loaded": 0, "message": "No records to process"}))
except Exception as e:
    print(f"Failed to read staging data: {str(e)}")
    raise e

# COMMAND ----------

# Extract new sync version
if "_current_sync_version" in source_df.columns:
    new_sync_version = source_df.select("_current_sync_version").first()[0]
    source_df = source_df.drop("_current_sync_version")
else:
    new_sync_version = last_sync_version
    print("Warning: _current_sync_version not found. Using previous version.")

# COMMAND ----------

# CRITICAL: Pre-MERGE Deduplication
# If a record was updated multiple times in the CDC window, keep only the latest change
pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]

# Window partitioned by PKs, ordered by SYS_CHANGE_VERSION descending
window_spec = Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())

source_deduped = source_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

print(f"Records after deduplication: {source_deduped.count()}")

# COMMAND ----------

# Add metadata columns to source
source_deduped = source_deduped \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_system_code", lit(source_system)) \
    .withColumn("_is_deleted", col("SYS_CHANGE_OPERATION") == 'D') \
    .withColumn("_cdc_operation", col("SYS_CHANGE_OPERATION"))

# COMMAND ----------

# Build dynamic merge condition
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])
print(f"Merge Condition: {merge_condition}")

# Get columns for update/insert mapping
all_columns = source_deduped.columns
# Exclude CDC specific columns from the actual data update if needed, but we map them all
update_columns = [c for c in all_columns if c not in pk_cols and c not in ['SYS_CHANGE_OPERATION', 'SYS_CHANGE_VERSION']]

# COMMAND ----------

# Execute MERGE
try:
    target_table = DeltaTable.forPath(spark, delta_path)
    
    # COMPLETE MERGE implementation
    target_table.alias("target").merge(
        source_deduped.alias("source"),
        merge_condition
    ).whenMatchedDelete(
        condition="source.SYS_CHANGE_OPERATION = 'D'"
    ).whenMatchedUpdate(
        condition="source.SYS_CHANGE_OPERATION IN ('U', 'I')",
        set={column: f"source.{column}" for column in update_columns}
    ).whenNotMatchedInsert(
        condition="source.SYS_CHANGE_OPERATION != 'D'",
        values={column: f"source.{column}" for column in all_columns}
    ).execute()
    
    print("MERGE operation completed successfully.")
except Exception as e:
    print(f"MERGE operation failed: {str(e)}")
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
        records_processed, 
        new_sync_version, 
        0, 
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

# Clean up staging files
try:
    dbutils.fs.rm(staging_path, recurse=True)
    print(f"Cleaned up staging path: {staging_path}")
except Exception as e:
    print(f"Warning: Failed to clean up staging path: {str(e)}")

# COMMAND ----------

# Return success JSON
result = {
    "status": "SUCCESS",
    "records_processed": records_processed,
    "sync_version": new_sync_version,
    "delta_path": delta_path
}
dbutils.notebook.exit(json.dumps(result))