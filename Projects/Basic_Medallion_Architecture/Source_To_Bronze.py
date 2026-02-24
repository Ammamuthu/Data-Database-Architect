# Databricks notebook source
# MAGIC %md
# MAGIC # Extract the Data from Source to Bronze layer

# COMMAND ----------

# DBTITLE 1,List_of_source_csv_and_target table_name
List_of_source = [

{
    "path" : "/Volumes/workspace/enterprise_database/source_system/Source_CRM/cust_info.csv" ,
    "Table_Name" : "customer_CRM"
},
{
    "path" : "/Volumes/workspace/enterprise_database/source_system/Source_CRM/prd_info.csv" , 
    "Table_Name" : "Product_CRM"
},
{
    "path" : "/Volumes/workspace/enterprise_database/source_system/Source_CRM/sales_details.csv"  ,
    "Table_Name" : "Sales_CRM"
},

{
    "path" : "/Volumes/workspace/enterprise_database/source_system/Source_ERP/CUST_AZ12.csv"  ,
    "Table_Name" : "CUST_ERP"
},
{
    "path" : "/Volumes/workspace/enterprise_database/source_system/Source_ERP/LOC_A101.csv"  ,
    "Table_Name" : "LOCACTION_ERP"
},
{
    "path" : "/Volumes/workspace/enterprise_database/source_system/Source_ERP/PX_CAT_G1V2.csv"  ,
    "Table_Name" : "CATOGORY_ERP"
}
]

# COMMAND ----------

# DBTITLE 1,loop through above
for i in List_of_source:

    print(i["path"] ,"Completed")

    df = spark.read.option("header","True").option("inferSchema", "true").csv(i["path"])
    df.write.mode("overwrite").format("delta").saveAsTable("workspace.bronze."+i["Table_Name"])

# COMMAND ----------

# DBTITLE 1,Read
# MAGIC %sql
# MAGIC -- SELECT * FROM workspace.bronze.customer_crm;