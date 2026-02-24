# Databricks notebook source
query = """
Select 
SA_CR.order_number,
G_PRD.prd_key,
G_CUS.customer_id,
SA_CR.order_date,
SA_CR.ship_date,
SA_CR.due_date,
SA_CR.sales_amount,
SA_CR.quantity,
SA_CR.price

from
 workspace.silver.sales_crm SA_CR 

 left join 
 workspace.gold.dim_product G_PRD
 on SA_CR.product_number = G_PRD.prd_key

 left join
 workspace.gold.dim_customer G_CUS
 on SA_CR.customer_id = G_CUS.CUSTOMER_ID

"""


df = spark.sql(query)
df.display()

# COMMAND ----------

df.write.mode("overwrite").format('delta').saveAsTable("workspace.gold.Fact_Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.Fact_Sales limit 20;;