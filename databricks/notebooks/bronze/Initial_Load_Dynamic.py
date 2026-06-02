# Databricks notebook source
import pyodbc
import json
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------
# 1. Define and Get Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("sql_server", "")
dbutils.widgets.text("sql_database", "")
dbutils.widgets.text("sql_username", "")
dbutils.widgets.text("sql_password", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
table_id = dbutils.widgets.get("table_id")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
source_system = dbutils.widgets.get("source_system")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------
# 2. Storage Authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------
# 3. Read from STAGING path (Parquet)
staging_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/staging/{source_system}/{schema_name}/{table_name}/initial/{pipeline_run_id}/"
print(f"Reading from staging path: {staging_path}")

df = spark.read.format("parquet").load(staging_path)
records_loaded = df.count()
print(f"Records read from staging: {records_loaded}")

# COMMAND ----------
# 4. Add Metadata Columns
df_with_metadata = df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumn("_is_deleted", lit(False)) \
    .withColumn("_cdc_operation", lit("I"))

# COMMAND ----------
# 5. Write to DELTA TABLE path
delta_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/"
print(f"Writing to Delta path: {delta_path}")

df_with_metadata.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------
# 6. Get Current Sync Version and Update Control DB
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Get current change tracking version
    cursor.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
    row = cursor.fetchone()
    current_sync_version = row[0] if row and row[0] is not None else 0
    
    # CRITICAL SECTION - Update control table
    update_sql = """
        EXEC [control].[sp_UpdateTableMetadata] 
            @TableId = ?, 
            @Status = 'SUCCESS', 
            @PipelineRunId = ?, 
            @RecordsLoaded = ?, 
            @SyncVersion = ?, 
            @MarkInitialLoadComplete = 1
    """
    cursor.execute(update_sql, (table_id, pipeline_run_id, records_loaded, current_sync_version))
    conn.commit()
    
    # Verification query
    cursor.execute("SELECT initial_load_completed FROM [control].[table_metadata] WHERE table_id = ?", (table_id,))
    verify_row = cursor.fetchone()
    print(f"Verification - initial_load_completed flag is now: {verify_row[0]}")
    
except Exception as e:
    print(f"Error updating control database: {str(e)}")
    raise e
finally:
    if 'conn' in locals():
        conn.close()

# COMMAND ----------
# 7. Clean up staging files (Optional but recommended)
dbutils.fs.rm(staging_path, recurse=True)
print(f"Cleaned up staging path: {staging_path}")

# COMMAND ----------
# 8. Return Result
result = {
    "status": "SUCCESS",
    "records_loaded": records_loaded,
    "sync_version": current_sync_version,
    "delta_path": delta_path
}
dbutils.notebook.exit(json.dumps(result))