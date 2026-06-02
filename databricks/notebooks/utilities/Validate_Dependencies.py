# Databricks notebook source
import pyodbc
import sys

# COMMAND ----------
# Define variables
sql_server = "clv-control-sql-server.database.windows.net"
sql_database = "clv-control-db"
sql_username = dbutils.widgets.get("sql_username") if "sql_username" in [w.name for w in dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().values()] else "{{PLACEHOLDER_SQL_USERNAME}}"
sql_password = dbutils.widgets.get("sql_password") if "sql_password" in [w.name for w in dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().values()] else "{{PLACEHOLDER_SQL_PASSWORD}}"
storage_account = "clvdatalakegen2"
storage_access_key = dbutils.widgets.get("storage_access_key") if "storage_access_key" in [w.name for w in dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().values()] else "{{PLACEHOLDER_STORAGE_KEY}}"

# COMMAND ----------
# Storage Authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------
print("--- VALIDATING DEPENDENCIES ---")
passed = 0
failed = 0

def check(name, condition, error_msg=""):
    global passed, failed
    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        print(f"[FAIL] {name} - {error_msg}")
        failed += 1

# 1. Spark Version Check
check("Spark Session Active", spark is not None)

# 2. Delta Lake Availability
try:
    spark.sql("SELECT 1").collect()
    check("Delta Lake Engine", True)
except Exception as e:
    check("Delta Lake Engine", False, str(e))

# 3. Required Python Packages
check("pyodbc installed", 'pyodbc' in sys.modules)

# 4. SQL Server Connectivity & Control Tables
try:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    check("SQL Server Connectivity", True)
    
    # Check tables
    cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('control')")
    table_count = cursor.fetchone()[0]
    check("Control Tables Exist", table_count >= 5, f"Found {table_count} tables, expected 5")
    
    # Check stored procedures
    cursor.execute("SELECT COUNT(*) FROM sys.procedures WHERE schema_id = SCHEMA_ID('control')")
    sp_count = cursor.fetchone()[0]
    check("Stored Procedures Exist", sp_count >= 3, f"Found {sp_count} SPs, expected 3")
    
    conn.close()
except Exception as e:
    check("SQL Server Connectivity", False, str(e))

# 5. Storage Account Access
try:
    test_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/test_dir/"
    dbutils.fs.mkdirs(test_path)
    dbutils.fs.rm(test_path, recurse=True)
    check("ADLS Gen2 Read/Write Access", True)
except Exception as e:
    check("ADLS Gen2 Read/Write Access", False, str(e))

# Summary
print("-" * 30)
print(f"Validation Complete: {passed} Passed, {failed} Failed")
if failed > 0:
    raise Exception("Environment validation failed. Check logs above.")