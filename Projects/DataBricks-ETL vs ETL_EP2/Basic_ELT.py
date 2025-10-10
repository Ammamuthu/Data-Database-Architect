import psycopg2
import pandas as pd
from datetime import datetime


conn = psycopg2.connect(
    dbname="defaultdb",
    user="ashok",
    password="vxV_AjRucUBG1Im398Vejw",
    host="phased-ermine-14990.j77.aws-ap-south-1.cockroachlabs.cloud",
    port=26257,
    sslmode="verify-full",
    sslrootcert=r"C:\Users\AmmamuthuM\AppData\Roaming\postgresql\root.crt"
)


cur = conn.cursor()


df = pd.read_excel(r"C:\Users\AmmamuthuM\OneDrive - ECS Business Solutions PVT LTD\Documents\ECS\Data Arc\Projects\DataBricks-ETL vs ETL_EP2\Cutomer.xlsx")

# Iterate over DataFrame rows and insert into database

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO WW_Customer (transactionid, date, customername, product, quantity, unitprice, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transactionid) DO UPDATE
        SET date = EXCLUDED.date,
            customername = EXCLUDED.customername,
            product = EXCLUDED.product,
            quantity = EXCLUDED.quantity,
            unitprice = EXCLUDED.unitprice,
            country = EXCLUDED.country;
    """, (
        row['TransactionID'],
        row['Date'],
        row['CustomerName'],
        row['Product'],
        row['Quantity'],
        row['UnitPrice'],
        row['Country']
    ))


print ("Exported completed ")
conn.commit()

# with open(r"DataBricks-ETL vs ETL_EP2\ELT_Query.sql", 'r') as f:
#     cur.execute(f.read())

with open(r"DataBricks-ETL vs ETL_EP2\ELT_Query.sql", 'r') as f:
    sql_script = f.read()

# Split SQL statements by semicolon
for statement in sql_script.split(';'):
    statement = statement.strip()
    if statement:
        try:
            cur.execute(statement + ';')
        except Exception as e:
            print(f"\n❌ ERROR in SQL:\n{statement}\n{e}")

conn.commit()

cur.close()
conn.close()