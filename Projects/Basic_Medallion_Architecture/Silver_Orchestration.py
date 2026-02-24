# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Files Need to Orchestration  

# COMMAND ----------

# DBTITLE 1,Scheduler_Loop
Notebook_silver = [
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Bronze_to_Silver/Bronze_To_Silver_Catogory_ERP",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Bronze_to_Silver/Bronze_To_Silver_Customer_ERP",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Bronze_to_Silver/Bronze_To_Silver_Location_ERP",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Bronze_to_Silver/Bronze_To_Silver_Product_CRM",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Bronze_to_Silver/Bronze_To_Silver_Sales_CRM",
    "/Workspace/Data_Projects/Basic_Medallion_Architecture/Bronze_to_Silver/Bronze_To_Silver_Customer_CRM"
]

for i in Notebook_silver:
    print("Starting to Run ",i)
    dbutils.notebook.run(i, timeout_seconds=0)

# COMMAND ----------

