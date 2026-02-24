# Databricks notebook source
# DBTITLE 1,Gold_Customer_query
query = """
SELECT 
    
    Row_Number() Over (Order By cu_cr.CUSTOMER_ID) Customer_Key,
    cu_cr.CUSTOMER_ID,
    cu_cr.CUSTOMER_PASS,
    CONCAT(cu_cr.First_Name, ' ', cu_cr.Last_Name) AS Full_Name,
     CASE
        WHEN cu_cr.Gender <> 'n/a' THEN cu_cr.Gender
        ELSE COALESCE(cu_er.GENDER, 'n/a')
    END AS Gender,
    lo_er.COUNTRY ,
    cu_cr.MARITAL_STATUS,
    FLOOR(MONTHS_BETWEEN(current_date(), cu_er.BirthDate) / 12) AS Age,
    cu_cr.Acc_Creation_Date
  
    FROM workspace.silver.customer_crm cu_cr

    LEFT JOIN workspace.silver.customer_erp cu_er
    ON CAST(cu_cr.CUSTOMER_ID AS STRING) = CAST(cu_er.CUSTOMER_ID AS STRING)

    LEFT JOIN workspace.silver.location_erp LO_ER
    on cu_cr.CUSTOMER_ID = LO_ER.CUSTOMER_ID

    """

df = spark.sql(query)
display(df)

# COMMAND ----------

df.write.mode("overwrite").format('delta').saveAsTable('workspace.gold.dim_customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dim_customer