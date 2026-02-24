# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.bronze.sales_crm

# COMMAND ----------

# DBTITLE 1,Read_a Data_from_Bronze
df = spark.read.table('workspace.bronze.sales_crm')
df.display()

# COMMAND ----------

# DBTITLE 1,Trim_unwanted_space's

from pyspark.sql.functions import trim,col
from pyspark.sql.types import StringType


for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

# DBTITLE 1,Change_Data-type
from pyspark.sql.functions import to_date

# df = df.withColumn('sls_order_dt', to_date(df['sls_order_dt'],'yyyyMMdd'))     # -- >Single column 


# For converting multiple column's

from pyspark.sql.functions import col, when, to_date, length

date_columns = ['sls_order_dt','sls_ship_dt','sls_due_dt']

for c in date_columns:
    df = df.withColumn(
        c,
        to_date(
            when(
                (col(c) == 0) |
                (col(c) == '0') |
                (length(col(c).cast("string")) < 8),
                None
            ).otherwise(col(c).cast("string")),
            "yyyyMMdd"
        )
    )


# COMMAND ----------

# DBTITLE 1,Drop_null
df = df.dropna(subset=["sls_ord_num","sls_cust_id","sls_prd_key"])
df.display()

# COMMAND ----------

# DBTITLE 1,Price = sales/quality
from pyspark.sql.functions import col,when
df = (

    df
    .withColumn(
        "sls_price",
        when(
            (col("sls_price").isNull()) | (col("sls_price") <= 0),
            when( col("sls_quantity") != 0, col("sls_sales") / col("sls_quantity")).otherwise(None)
            ).otherwise(col("sls_price"))
    )
)


     

# COMMAND ----------

# DBTITLE 1,Rename_the_columns

RENAME_MAP = {
    "sls_ord_num": "order_number",
    "sls_prd_key": "product_number",
    "sls_cust_id": "customer_id",
    "sls_order_dt": "order_date",
    "sls_ship_dt": "ship_date",
    "sls_due_dt": "due_date",
    "sls_sales": "sales_amount",
    "sls_quantity": "quantity",
    "sls_price": "price"
}
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

df.display()
     

# COMMAND ----------

# DBTITLE 1,Write_in_Delta_Table
df.write.mode("overwrite").format("delta").saveAsTable("workspace.silver.sales_crm") 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver.sales_crm