# Databricks notebook source
import pyodbc
import json
from pyspark.sql.functions import lit, current_timestamp, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# 1. Define and Get Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("last_sync_version", "")
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
last_sync_version = dbutils.widgets.get("last_sync_version")
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
# 3. Read CDC changes from STAGING path (Parquet)
staging_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/staging/{source_system}/{schema_name}/{table_name}/incremental/{pipeline_run_id}/"
print(f"Reading from staging path: {staging_path}")

cdc_df = spark.read.format("parquet").load(staging_path)
records_processed = cdc_df.count()
print(f"Records read from staging: {records_processed}")

if records_processed == 0:
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "records_processed": 0, "message": "No records to process"}))

# Extract new sync version from the first row
new_sync_version = cdc_df.select("_current_sync_version").first()[0]

# COMMAND ----------
# 4. Add Metadata Columns
source_df = cdc_df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumn("_is_deleted", col("SYS_CHANGE_OPERATION") == 'D') \
    .withColumn("_cdc_operation", col("SYS_CHANGE_OPERATION"))

# COMMAND ----------
# 5. CRITICAL: Pre-MERGE Deduplication
pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]

# Validate PK columns exist
for pk in pk_cols:
    if pk not in source_df.columns:
        raise ValueError(f"Primary key column {pk} not found in source dataframe.")

# Deduplicate source data - keep latest change per key based on SYS_CHANGE_VERSION
window_spec = Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())

source_deduped = source_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

# COMMAND ----------
# 6. Read existing DELTA TABLE for MERGE
delta_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/"
print(f"Target Delta path: {delta_path}")

target_table = DeltaTable.forPath(spark, delta_path)

# COMMAND ----------
# 7. Build merge condition dynamically
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])
print(f"Merge condition: {merge_condition}")

# Prepare columns for update/insert
all_columns = [c for c in source_deduped.columns if c not in ['SYS_CHANGE_OPERATION', 'SYS_CHANGE_VERSION', '_current_sync_version']]
update_columns = [c for c in all_columns if c not in pk_cols]

# COMMAND ----------
# 8. COMPLETE MERGE implementation
target_table.alias("target").merge(
    source_deduped.alias("source"),
    merge_condition
).whenMatchedDelete(
    condition="source.SYS_CHANGE_OPERATION = 'D'"
).whenMatchedUpdate(
    condition="source.SYS_CHANGE_OPERATION IN ('U', 'I')",
    set={column: f"source.{column}" for column in update_columns + ["_pipeline_run_id", "_load_timestamp", "_is_deleted", "_cdc_operation"]}
).whenNotMatchedInsert(
    condition="source.SYS_CHANGE_OPERATION != 'D'",
    values={column: f"source.{column}" for column in all_columns}
).execute()

# COMMAND ----------
# 9. POST-MERGE Optimization (CRITICAL for performance)
print(f"Optimizing Delta table: {delta_path}")
spark.sql(f"""
    OPTIMIZE delta.`{delta_path}`
    ZORDER BY ({primary_key_columns}, _load_timestamp)
""")

# COMMAND ----------
# 10. Update Control DB
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    update_sql = """
        EXEC [control].[sp_UpdateTableMetadata] 
            @TableId = ?, 
            @Status = 'SUCCESS', 
            @PipelineRunId = ?, 
            @RecordsLoaded = ?, 
            @SyncVersion = ?, 
            @MarkInitialLoadComplete = 0
    """
    cursor.execute(update_sql, (table_id, pipeline_run_id, records_processed, new_sync_version))
    conn.commit()
    
except Exception as e:
    print(f"Error updating control database: {str(e)}")
    raise e
finally:
    if 'conn' in locals():
        conn.close()

# COMMAND ----------
# 11. Clean up staging files
dbutils.fs.rm(staging_path, recurse=True)
print(f"Cleaned up staging path: {staging_path}")

# COMMAND ----------
# 12. Return Result
result = {
    "status": "SUCCESS",
    "records_processed": records_processed,
    "new_sync_version": new_sync_version,
    "delta_path": delta_path
}
dbutils.notebook.exit(json.dumps(result))