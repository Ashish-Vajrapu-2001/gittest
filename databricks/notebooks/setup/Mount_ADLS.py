# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Gen2 to Databricks
# MAGIC This notebook mounts the Azure Data Lake Storage Gen2 container to the Databricks workspace.

# COMMAND ----------

# Define variables
storage_account = "clvdatalakegen2"
container_name = "datalake"
mount_point = f"/mnt/{container_name}"

# Get secrets from widgets (or Key Vault in production)
dbutils.widgets.text("storage_access_key", "{{PLACEHOLDER_STORAGE_KEY}}")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# Storage authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

def mount_adls(storage_account_name, container, mount_dir, access_key):
    """Mounts ADLS Gen2 container if not already mounted."""
    
    # Check if already mounted
    if any(mount.mountPoint == mount_dir for mount in dbutils.fs.mounts()):
        print(f"Directory {mount_dir} is already mounted.")
        return True
    
    try:
        print(f"Mounting {container} to {mount_dir}...")
        dbutils.fs.mount(
            source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
            mount_point = mount_dir,
            extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
        )
        print("Mount successful.")
        return True
    except Exception as e:
        print(f"Error mounting ADLS: {str(e)}")
        return False

# COMMAND ----------

# Execute mount
mount_adls(storage_account, container_name, mount_point, storage_access_key)

# COMMAND ----------

# Verify mount
display(dbutils.fs.ls(mount_point))

# COMMAND ----------

# Alternative: Service Principal Mount (Commented out but complete for reference)
"""
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<application-id>",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"
}

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs
)
"""