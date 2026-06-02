# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import col, expr, current_timestamp, lit

# COMMAND ----------
# 1. Define and get widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("silver_table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("source_bronze_table", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("transformation_rules_json", "[]")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
silver_table_id = dbutils.widgets.get("silver_table_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
source_bronze_table = dbutils.widgets.get("source_bronze_table")
target_silver_table = dbutils.widgets.get("target_silver_table")
transformation_rules_json = dbutils.widgets.get("transformation_rules_json")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

start_time = datetime.utcnow().isoformat()

# COMMAND ----------
# 2. Storage authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------
# 3. Read Bronze Delta table
bronze_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{source_bronze_table}/"
print(f"Reading from Bronze path: {bronze_path}")

try:
    df = spark.read.format("delta").load(bronze_path)
    records_read = df.count()
    print(f"Records read from Bronze: {records_read}")
except Exception as e:
    raise Exception(f"Failed to read Bronze table at {bronze_path}. Error: {str(e)}")

# COMMAND ----------
# 4. Parse and apply transformation rules
rules = json.loads(transformation_rules_json)
df_transformed = df

records_filtered = 0

for rule in rules:
    rule_type = rule.get("rule_type")
    source_col = rule.get("source_column")
    target_col = rule.get("target_column")
    expression = rule.get("transformation_expression")
    
    print(f"Applying rule: {rule.get('rule_name')} | Type: {rule_type}")
    
    if rule_type == "FILTER":
        count_before = df_transformed.count()
        df_transformed = df_transformed.filter(expr(expression))
        count_after = df_transformed.count()
        records_filtered += (count_before - count_after)
        
    elif rule_type == "TRANSFORM":
        df_transformed = df_transformed.withColumn(target_col, expr(expression))
        
    elif rule_type == "RENAME":
        df_transformed = df_transformed.withColumnRenamed(source_col, target_col)
        
    elif rule_type == "CAST":
        df_transformed = df_transformed.withColumn(target_col, col(source_col).cast(expression))
        
    elif rule_type == "DEDUPE":
        key_columns = [c.strip() for c in expression.split(",")]
        df_transformed = df_transformed.dropDuplicates(key_columns)

# COMMAND ----------
# 5. Add standard metadata columns
df_transformed = df_transformed \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .withColumn("_source_bronze_table", lit(source_bronze_table)) \
    .withColumn("_is_deleted", lit(False)) \
    .withColumn("_is_current", lit(True))

# COMMAND ----------
# 6. Write to Silver Delta table
silver_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{source_system}/{schema_name}/{target_silver_table}/"
print(f"Writing to Silver path: {silver_path}")

try:
    # Using overwrite for initial load/full refresh. 
    # For incremental MERGE, this logic would be expanded in a dedicated CDC notebook.
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_path)
        
    records_written = df_transformed.count()
    print(f"Records written to Silver: {records_written}")
except Exception as e:
    raise Exception(f"Failed to write Silver table at {silver_path}. Error: {str(e)}")

# COMMAND ----------
# 7. Return execution metrics to ADF
end_time = datetime.utcnow().isoformat()

output_metrics = {
    "records_read": records_read,
    "records_written": records_written,
    "records_filtered": records_filtered,
    "start_time": start_time,
    "end_time": end_time,
    "status": "SUCCESS"
}

dbutils.notebook.exit(json.dumps(output_metrics))