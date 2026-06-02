# Databricks notebook source
import json
from pyspark.sql.functions import col, expr, lit, current_timestamp

# COMMAND ----------
# 1. Define and get widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
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
# 3. Read Silver Delta table
silver_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{source_system}/{schema_name}/{target_silver_table}/"
print(f"Reading Silver table for DQ checks: {silver_path}")

df = spark.read.format("delta").load(silver_path)
total_records = df.count()

# COMMAND ----------
# 4. Define Hardcoded DQ Rules based on LLD (In production, these can be fetched from control.data_quality_rules)
dq_rules = []

if target_silver_table == "customers":
    dq_rules = [
        {"rule_id": "DQ-N-003", "type": "NULL_CHECK", "expr": "customer_id IS NOT NULL", "severity": "ERROR"},
        {"rule_id": "DQ-C-001", "type": "NULL_CHECK", "expr": "email IS NOT NULL", "severity": "SKIP_ROW"},
        {"rule_id": "DQ-F-001", "type": "FORMAT_CHECK", "expr": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'", "severity": "SKIP_ROW"}
    ]
elif target_silver_table == "orders":
    dq_rules = [
        {"rule_id": "DQ-N-004", "type": "NULL_CHECK", "expr": "order_id IS NOT NULL", "severity": "ERROR"},
        {"rule_id": "DQ-C-003", "type": "NULL_CHECK", "expr": "customer_id IS NOT NULL", "severity": "ERROR"},
        {"rule_id": "DQ-V-001", "type": "RANGE_CHECK", "expr": "total_amount > 0", "severity": "SKIP_ROW"}
    ]
elif target_silver_table == "surveys":
    dq_rules = [
        {"rule_id": "DQ-N-008", "type": "NULL_CHECK", "expr": "survey_id IS NOT NULL", "severity": "ERROR"},
        {"rule_id": "DQ-N-001", "type": "RANGE_CHECK", "expr": "nps_score BETWEEN 0 AND 10 OR nps_score IS NULL", "severity": "WARNING"},
        {"rule_id": "DQ-N-002", "type": "RANGE_CHECK", "expr": "csat_score BETWEEN 1 AND 5 OR csat_score IS NULL", "severity": "WARNING"}
    ]

# COMMAND ----------
# 5. Apply DQ Rules and collect metrics
dq_results = []
df_failed = None

for rule in dq_rules:
    rule_id = rule["rule_id"]
    expression = rule["expr"]
    severity = rule["severity"]
    
    # Find invalid rows (NOT expression)
    invalid_expr = f"NOT ({expression})"
    failed_count = df.filter(expr(invalid_expr)).count()
    
    dq_results.append({
        "rule_id": rule_id,
        "rule_type": rule["type"],
        "failed_count": failed_count,
        "severity": severity
    })
    
    if failed_count > 0 and severity == "ERROR":
        raise Exception(f"CRITICAL DQ FAILURE: Rule {rule_id} failed for {failed_count} records. Pipeline aborted.")

# COMMAND ----------
# 6. Output DQ Summary
print("Data Quality Execution Summary:")
print(json.dumps(dq_results, indent=2))

dbutils.notebook.exit(json.dumps({
    "total_records_evaluated": total_records,
    "dq_results": dq_results
}))