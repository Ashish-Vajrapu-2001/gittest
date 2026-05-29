# Databricks notebook source
import pyodbc
import json

# COMMAND ----------

dbutils.widgets.text("sql_server", "sql-clv-control.database.windows.net")
dbutils.widgets.text("sql_database", "sqldb-clv-control")
dbutils.widgets.text("sql_username", "{{PLACEHOLDER_SQL_USERNAME}}")
dbutils.widgets.text("sql_password", "{{PLACEHOLDER_SQL_PASSWORD}}")
dbutils.widgets.text("storage_account", "adlsclvanalytics")
dbutils.widgets.text("storage_access_key", "{{PLACEHOLDER_STORAGE_KEY}}")

# COMMAND ----------

sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# Storage authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

results = {"passed": 0, "failed": 0, "details": []}

def log_result(test_name, status, message=""):
    print(f"[{status}] {test_name}: {message}")
    results["details"].append({"test": test_name, "status": status, "message": message})
    if status == "PASS":
        results["passed"] += 1
    else:
        results["failed"] += 1

# COMMAND ----------

# 1. Check Spark Version
try:
    spark_version = spark.version
    log_result("Spark Version Check", "PASS", f"Version: {spark_version}")
except Exception as e:
    log_result("Spark Version Check", "FAIL", str(e))

# COMMAND ----------

# 2. Check Storage Access
try:
    test_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/"
    dbutils.fs.ls(test_path)
    log_result("Storage Access", "PASS", f"Successfully accessed {test_path}")
except Exception as e:
    log_result("Storage Access", "FAIL", str(e))

# COMMAND ----------

# 3. Check SQL Server Connectivity & Control Tables
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    log_result("SQL Server Connectivity", "PASS", "Connected successfully")
    
    # Check tables
    tables_to_check = [
        'source_systems', 'table_metadata', 'load_dependencies', 
        'pipeline_execution_log', 'data_quality_rules'
    ]
    
    for table in tables_to_check:
        cursor.execute(f"SELECT COUNT(*) FROM control.{table}")
        count = cursor.fetchone()[0]
        log_result(f"Table Exists: control.{table}", "PASS", f"Row count: {count}")
        
    # Check SPs
    sps_to_check = ['sp_GetCDCChanges', 'sp_UpdateTableMetadata', 'sp_GetTableLoadOrder']
    for sp in sps_to_check:
        cursor.execute(f"SELECT OBJECT_ID('control.{sp}')")
        if cursor.fetchone()[0] is not None:
            log_result(f"SP Exists: control.{sp}", "PASS")
        else:
            log_result(f"SP Exists: control.{sp}", "FAIL", "Stored procedure not found")
            
    conn.close()
except Exception as e:
    log_result("SQL Server Checks", "FAIL", str(e))

# COMMAND ----------

# Summary
print("\n" + "="*50)
print(f"VALIDATION SUMMARY: {results['passed']} Passed, {results['failed']} Failed")
print("="*50)

if results['failed'] > 0:
    raise Exception("Validation failed. Check logs for details.")
    
dbutils.notebook.exit(json.dumps(results))