# Databricks notebook source
# MAGIC %md
# MAGIC #Storage mount using service principal

# COMMAND ----------

dbutils.widgets.text("storage","formula1storagaccount") 
storage_account_name = dbutils.widgets.get("storage") 

dbutils.widgets.text("container","demo") 
container_name = dbutils.widgets.get("container") 
  

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-secret-scope', key = 'formula1-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-secret-scope', key = 'formula1-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-secret-scope', key = 'formula1-client-secret')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Check the mount and status 
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        print("Storage is already mounted")
    else:
        
        # Mount the storage account container
        dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
        print("Mount is successful")
    
    # display(dbutils.fs.mounts())



# COMMAND ----------

#function call
mount_adls(storage_account_name,container_name)

# COMMAND ----------


