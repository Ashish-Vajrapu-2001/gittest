# Databricks notebook source
import json

# COMMAND ----------
# 1. Define and get widgets
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("source_bronze_table", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
source_bronze_table = dbutils.widgets.get("source_bronze_table")
target_silver_table = dbutils.widgets.get("target_silver_table")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------
# 2. Storage authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------
# 3. Read schemas
bronze_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{source_bronze_table}/"
silver_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{source_system}/{schema_name}/{target_silver_table}/"

try:
    df_bronze = spark.read.format("delta").load(bronze_path)
    bronze_columns = set(df_bronze.columns)
except Exception as e:
    raise Exception(f"Failed to read Bronze schema: {str(e)}")

try:
    df_silver = spark.read.format("delta").load(silver_path)
    silver_columns = set(df_silver.columns)
except Exception as e:
    print("Silver table does not exist yet. Schema evolution not required.")
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "Silver table not found"}))
    df_silver = None
    silver_columns = set()

# COMMAND ----------
# 4. Detect new columns
if df_silver is not None:
    # Ignore metadata columns in comparison
    metadata_cols = {"_pipeline_run_id", "_silver_load_timestamp", "_source_bronze_table", "_is_deleted", "_is_current"}
    silver_business_cols = silver_columns - metadata_cols
    
    # In a real scenario, we map bronze columns to silver columns via rules.
    # For this evolution check, we look for raw bronze columns that have no mapping.
    new_bronze_cols = bronze_columns - silver_business_cols
    
    if len(new_bronze_cols) > 0:
        print(f"WARNING: Detected new columns in Bronze not mapped to Silver: {new_bronze_cols}")
        # Here you would typically log to an audit table or trigger an alert
        # control.schema_evolution_log
        
    else:
        print("Schemas are in sync. No unmapped columns detected.")

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "new_columns_detected": list(new_bronze_cols) if df_silver is not None else []
}))