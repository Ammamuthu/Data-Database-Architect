# Databricks notebook source
# MAGIC %md
# MAGIC ## Transforming a Data from Bronze to Silver Layer
# MAGIC # Product_CRM

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.bronze.product_crm

# COMMAND ----------

# DBTITLE 1,READ_DATA
df = spark.read.option('header', 'true').option('inferSchema', 'true').table('workspace.bronze.product_crm')

df.display()

# COMMAND ----------

# DBTITLE 1,Rename _the_Column
df = df.withColumnRenamed('prd_nm' , 'Prd_name') \
    .withColumnRenamed('prd_line' , 'prd_size')

df.display( )

# COMMAND ----------

# DBTITLE 1,spliting_PRD_KEY
from pyspark.sql.functions import substring, col, length, regexp_replace

# 1. Spliting 0-5 value , and replacing - -> _

# eg : CO-RF-FR-R92B-58 -> CO-RF
# ANS = CO_RF


df = df.withColumn("CATOGORY_ID", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"))

# eg : CO-RF-FR-R92B-58

df = df.withColumn("prd_key", substring(col("prd_key"), 7, length(col("prd_key"))))

df.display()


# COMMAND ----------

# DBTITLE 1,drop_Null_Values
df = df.dropna(subset=['prd_id', 'prd_cost'])

df.display()


# COMMAND ----------

# DBTITLE 1,Trim_a_space's unwanted
from pyspark.sql.functions import trim 

df = df.withColumn("prd_size", trim(df.prd_size))


# COMMAND ----------

# DBTITLE 1,Fill_the_Null_value
df = df.fillna('Free', subset=['prd_size'])

df.display()  


# COMMAND ----------

# DBTITLE 1,Replace_values
from pyspark.sql.functions import when 

df = df.withColumn('prd_size', when(df.prd_size == 'L', 'Large') \
    .when(df.prd_size == 'M', 'Medium') \
       .when(df.prd_size == 'S', 'Small') \
           .when(df.prd_size == 'R', 'Regular Fit') \
                .when(df.prd_size == 'T', 'Tall') \
                    .otherwise(df.prd_size))
    
    
df.display()

# COMMAND ----------

# DBTITLE 1,Save Data as Delta Table
df.write.mode('overwrite').format('Delta').saveAsTable('workspace.silver.product_crm')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.product_crm limit 10 ; 
# MAGIC
# MAGIC -- drop table silver.product_crm;

# COMMAND ----------

df.select("prd_id","CATOGORY_ID","prd_key","prd_name","prd_cost","prd_size","prd_start_dt","prd_end_dt").display()