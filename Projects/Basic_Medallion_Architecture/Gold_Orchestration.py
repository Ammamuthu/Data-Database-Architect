# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Notebook files need to Orchestration 

# COMMAND ----------

# Please maintain this Order Fact table must be in Last 

Gold_Notebooks = [
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Silver_To_Gold/Silver_To_Gold_Customer_DIM",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Silver_To_Gold/Silver_To_Gold_Product_DIM",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Silver_To_Gold/Silver_To_Gold_Sales_Fact"
]

for i in Gold_Notebooks:
    print ("Starting to Run a Notebook: ", i)
    dbutils.notebook.run(i,timeout_seconds=0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW CATALOGS;
# MAGIC
# MAGIC -- SHOW SCHEMAS IN workspace;
# MAGIC -- SHOW TABLES IN workspace.bronze;
# MAGIC
# MAGIC