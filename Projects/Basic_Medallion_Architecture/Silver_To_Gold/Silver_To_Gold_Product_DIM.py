# Databricks notebook source
query = """
select 
row_number() over(order by PR_CR.prd_start_dt , PR_CR.prd_id)  AS product_Num,
PR_CR.prd_id,
PR_CR.prd_key,
PR_CR.Prd_name,
PR_CR.prd_size,
PR_CR.prd_start_dt,
PR_CR.CATOGORY_ID,
CA_ER.CATOGORY,
CA_ER.SUB_CATOGORY,
CA_ER.MAINTENANCE

from
workspace.silver.product_crm PR_CR
left join workspace.silver.catogory_erp CA_ER
on PR_CR.CATOGORY_ID =CA_ER.CATOGORY_ID
;

"""

df = spark.sql(query)
df.display()


# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable('workspace.gold.dim_product')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dim_product limit 20;