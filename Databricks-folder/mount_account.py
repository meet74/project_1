# Databricks notebook source
print("shreehari")

# COMMAND ----------

def mountadls(scope_name,storage_acc_name,container_name):
    
    client_id=dbutils.secrets.get(f"{scope_name}","app-client-id")
    tenent_id=dbutils.secrets.get(f"{scope_name}","app-tenant-id")
    client_secret=dbutils.secrets.get(f"{scope_name}","app-secret-id")

    spark.conf.set("fs.azure.account.auth.type.shrijiformula1adls.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.shrijiformula1adls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.shrijiformula1adls.dfs.core.windows.net", client_id)
    spark.conf.set("fs.azure.account.oauth2.client.secret.shrijiformula1adls.dfs.core.windows.net", client_secret)
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.shrijiformula1adls.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}
    
    dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net",
    mount_point=f"/mnt/{storage_acc_name}/{container_name}",
    extra_configs=configs
    )

    


# COMMAND ----------

mountadls("health-vault","healthstorageaccount","health-project")

# COMMAND ----------


