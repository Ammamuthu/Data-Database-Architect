# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.bronze.catogory_erp

# COMMAND ----------

# DBTITLE 1,READ_A_DATA
df = spark.read.option('header', 'true').option('inferSchema', 'true').table('workspace.bronze.catogory_erp')

df.display()

# COMMAND ----------

# DBTITLE 1,Rename_Column
df = df.withColumnRenamed('CAT', 'CATOGORY')\
    .withColumnRenamed('SUBCAT', 'SUB_CATOGORY')\
        .withColumnRenamed('MAINTENANCE','MAINTENANCE')\
            .withColumnRenamed('ID', 'CATOGORY_ID')

df.display()

# COMMAND ----------

# DBTITLE 1,Trim
from pyspark.sql.functions import trim


# Here all the column are string so that's why i used 
cols_to_trim = ['CATOGORY_ID', 'CATOGORY', 'SUB_CATOGORY', 'MAINTENANCE']

for i in cols_to_trim:
    df = df.withColumn(i, trim(df[i]))


df.display()

# COMMAND ----------

df.write.mode('overwrite').format('delta').option('header', 'true').saveAsTable('workspace.silver.catogory_erp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.catogory_erp