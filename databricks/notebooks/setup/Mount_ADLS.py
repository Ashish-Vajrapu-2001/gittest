# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Gen2 to Databricks
# MAGIC This notebook mounts the Azure Data Lake Storage Gen2 container to the Databricks workspace.

# COMMAND ----------

# Define widgets for parameters
dbutils.widgets.text("storage_account", "adlsclvanalytics")
dbutils.widgets.text("storage_access_key", "{{PLACEHOLDER_STORAGE_KEY}}")
dbutils.widgets.text("container_name", "datalake")

# COMMAND ----------

# Get parameter values
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

# Storage authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

def mount_adls(storage_account_name, container, access_key):
    mount_point = f"/mnt/{storage_account_name}/{container}"
    
    # Check if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"Directory {mount_point} is already mounted.")
        return mount_point
    
    # Mount using account key
    try:
        dbutils.fs.mount(
            source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
            mount_point = mount_point,
            extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
        )
        print(f"Successfully mounted {mount_point}")
        return mount_point
    except Exception as e:
        print(f"Error mounting {mount_point}: {str(e)}")
        raise e

# COMMAND ----------

# Execute mount
mount_path = mount_adls(storage_account, container_name, storage_access_key)

# COMMAND ----------

# Verify mount
display(dbutils.fs.ls(mount_path))

# COMMAND ----------

# Alternative: Service Principal Mount (Commented out but complete for reference)
"""
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "{{PLACEHOLDER_SP_CLIENT_ID}}",
  "fs.azure.account.oauth2.client.secret": "{{PLACEHOLDER_SP_SECRET}}",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{{PLACEHOLDER_TENANT_ID}}/oauth2/token"
}

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account}/{container_name}",
  extra_configs = configs
)
"""