# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.bronze.cust_erp

# COMMAND ----------

# DBTITLE 1,Read_a_Data
df  = spark.read.option('inferSchema','true').option('header','true').table('workspace.bronze.cust_erp')

# COMMAND ----------

# DBTITLE 1,Change_Column_Name
df = df.withColumnRenamed('CID','CUSTOMER_ID')\
    .withColumnRenamed('BDATE','BIRTHDATE')\
    .withColumnRenamed('GEN','GENDER')

df.display()

# COMMAND ----------

# DBTITLE 1,Drop_Null
df = df.dropna(subset=['CUSTOMER_ID'])


df.display()

# COMMAND ----------

# DBTITLE 1,Trim
from pyspark.sql.functions import trim


df = df.withColumn('GENDER',trim(df.GENDER))\
    .withColumn('CUSTOMER_ID',trim(df.CUSTOMER_ID))


df.display()

# COMMAND ----------

# DBTITLE 1,customer_id cleaning
from pyspark.sql.functions import col, substring, length,when


df = df.withColumn(
    "CUSTOMER_ID",
    when(col("CUSTOMER_ID").startswith("NAS"),
           substring(col("CUSTOMER_ID"), 9, length(col("CUSTOMER_ID"))))
     .otherwise(col("CUSTOMER_ID"))
)

     

# COMMAND ----------

# DBTITLE 1,Gener_column_format
from pyspark.sql.functions import when, initcap, col

df = df.withColumn('GENDER',
    when((col('GENDER').isNull() | (col('GENDER') == '')), 'None')
    .when(col('GENDER') == 'M', 'Male')
    .when(col('GENDER') == 'F', 'Female')
    .when(col('GENDER') == 'T', 'TransGender')
    .when(col('GENDER') == 'O', 'Other')
    .otherwise(initcap(col('GENDER')))
)
df.display()

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.functions import date_format, try_to_date, col
# MAGIC
# MAGIC df = df.withColumn(
# MAGIC     "BIRTHDATE",
# MAGIC     date_format(
# MAGIC         try_to_date(col("BIRTHDATE"), "MM-dd-yyyy"),
# MAGIC         "MM-dd-yyyy"
# MAGIC     )
# MAGIC )
# MAGIC df.display()

# COMMAND ----------

# DBTITLE 1,BDATE_CLEANUP
# Checking Birth Date is greater then or equal to Today Date   and     Birth Date is not null

from pyspark.sql.functions import current_date, when ,col ,length
df.withColumn(
    'BIRTHDATE',
    when(col('BIRTHDATE') > current_date(),'none')
    .when(length(trim(col("BIRTHDATE"))) != 10, "none")
    .otherwise(col('BIRTHDATE'))
    )

df.display()


# COMMAND ----------

# DBTITLE 1,Write_Data_in_silver
df.write.mode("overwrite").format('delta').saveAsTable('workspace.silver.customer_erp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.customer_erp