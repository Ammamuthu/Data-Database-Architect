# Databricks notebook source
# MAGIC %sql
# MAGIC select CNTRY from workspace.bronze.locaction_erp group by CNTRY;

# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true").table('workspace.bronze.locaction_erp')
df.display()

# COMMAND ----------

# DBTITLE 1,Rename a Column
df =  df.withColumnRenamed('CNTRY', 'COUNTRY')\
    .withColumnRenamed('CID', 'CUSTOMER_ID')

df.display()

# COMMAND ----------

# DBTITLE 1,Trim
from pyspark.sql.functions import trim

df = df.withColumn('COUNTRY', trim('COUNTRY'))
df.display()

# COMMAND ----------

# DBTITLE 1,Changing Customer_id text
from pyspark.sql.functions import col,when,substring,length

df = df.withColumn(
    "CUSTOMER_ID",
    when(col("CUSTOMER_ID").startswith("AW"),
           substring(col("CUSTOMER_ID"), 7, length(col("CUSTOMER_ID"))))
     .otherwise(col("CUSTOMER_ID"))
)

     

# COMMAND ----------

# DBTITLE 1,Format_Data
from pyspark.sql.functions import col, when

df = df.withColumn(
    'COUNTRY',  
    when((col('COUNTRY') == '') | col('COUNTRY').isNull(), 'Null')
    .when(col('COUNTRY') == 'DE', 'GERMANY')
    .when(col('COUNTRY').isin('USA', 'US'), 'United States')
    .otherwise(col('COUNTRY'))
)

df.display()

# COMMAND ----------

# DBTITLE 1,save a table
df.write.mode('overwrite').format('delta').saveAsTable('workspace.silver.location_erp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  workspace.silver.location_erp;