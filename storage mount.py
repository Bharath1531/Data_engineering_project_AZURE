# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@studentbs1.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# Mount point is created as bronze

dbutils.fs.ls("/mnt/bronze/SalesLT/")

# Mount point is created as silver

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://silver@studentbs1.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs)

# Mount point is created as gold

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://gold@studentbs1.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs)
