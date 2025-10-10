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

print(df.columns)
# print(df.Country == 'India')

# Filter customers from India with UnitPrice <= 100
india_df = df[(df['Country'] == 'India') & (df['UnitPrice'] <=100) ]

#Filter for USA country 
USA_df = df[df['Country'] == 'USA']

#Multiply Unitprice = Unitprice * quantity
india_df = india_df.copy()
india_df['UnitPrice'] = india_df['UnitPrice'] * india_df['Quantity']

# print(india_df)
# print(USA_df)

for _, row in india_df.iterrows():
    cur.execute("""
        INSERT INTO india_cus (transactionid, date, customername, product, quantity, unitprice, country)
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

for _, row in USA_df.iterrows():
    cur.execute("""
        INSERT INTO usa_cus (transactionid, date, customername, product, quantity, unitprice, country)
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
cur.close()
conn.close()