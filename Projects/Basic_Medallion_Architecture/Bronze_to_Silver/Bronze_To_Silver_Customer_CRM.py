# Databricks notebook source
# MAGIC %md
# MAGIC ## Tranforming a Data while Moving from Bronze to Silver
# MAGIC # Customer_CRM_Table

# COMMAND ----------

# DBTITLE 1,sql- read
# MAGIC %sql
# MAGIC select * from workspace.bronze.customer_crm;

# COMMAND ----------

# DBTITLE 1,Read_a_dataset
df = spark.read.table('workspace.bronze.customer_crm')
# df.display()

df.count()


# COMMAND ----------

# DBTITLE 1,Rename_Coloum
df = df.withColumnRenamed("cst_create_date",'Acc_Creation_Date')\
    .withColumnRenamed("cst_firstname",'First_Name')\
        .withColumnRenamed("cst_lastname",'Last_Name')\
            .withColumnRenamed("cst_marital_status",'Marital_Status')\
                .withColumnRenamed("cst_gndr",'Gender')\
                    .withColumnRenamed("cst_key",'Customer_Pass')\
                        .withColumnRenamed("cst_id",'Customer_ID')

# df.display()

# COMMAND ----------

# DBTITLE 1,Drop_Null_Values
df = df.dropna(subset=['Customer_ID', "Customer_Pass"])

# COMMAND ----------

# DBTITLE 1,Change_Values_in_column level
from pyspark.sql.functions import when

df = df.withColumn("Marital_Status", when(df.Marital_Status == 'M', 'Married')\
    . when(df.Marital_Status == 'S', 'Single') \
         .when(df.Marital_Status == 'D', 'Divorced') \
             .when(df.Marital_Status == 'W', 'Widowed')\
                 .otherwise(df.Marital_Status))


df.display()

# COMMAND ----------

# DBTITLE 1,Trim-Remove empty space
from pyspark.sql.functions import trim

df = df.withColumn("First_Name",trim(df.First_Name))\
    .withColumn("Last_Name",trim(df.Last_Name))

# df.display()   - Trim Remove empty space's in Beginning and End of the Coloumn
       

# COMMAND ----------

# DBTITLE 1,Handle Null values in GENDER
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

# DBTITLE 1,Write in Silver
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.customer_crm")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.customer_crm;