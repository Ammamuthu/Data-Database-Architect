# From Excel to CockrochDB (cloud)  E T L
import psycopg2
import pandas as pd
from datetime import datetime


conn = psycopg2.connect(
    dbname="defaultdb",
    user="ashok",
    password="arriU_w6sVXAr3NAlkaAzA",
    host="peewee-chimera-13443.j77.aws-ap-south-1.cockroachlabs.cloud",
    port=26257,
    sslmode="verify-full",
    sslrootcert=r"C:\Users\AmmamuthuM\AppData\Roaming\postgresql\root.crt"
)


cur = conn.cursor()

df = pd.read_csv(r"C:\Users\AmmamuthuM\OneDrive - ECS Business Solutions PVT LTD\Documents\ECS\Data Arc\Projects\DataBricks-ETL_EP1\Customer.csv")

# Fill missing last names with placeholder
df["last_name"]=df["last_name"].fillna("unkown")

# Drop rows where critical fields (e.g., first_name, email) are missing
df = df.dropna(subset=["first_name","age"])

# converting to Numeric AGE column
df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(0).astype(int)

# Calculate DOB as (current_year - age)
current_year = datetime.now().year
df["birth_year"] = df["age"].apply(lambda age:current_year - age)


# Iterate over DataFrame rows and insert into database
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO customers (customer_id, first_name, last_name, email, age, country, birth_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE
        SET first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            age = EXCLUDED.age,
            country = EXCLUDED.country,
            birth_year = EXCLUDED.birth_year;
    """, (
        row['customer_id'],
        row['first_name'],
        row['last_name'],
        row['email'],
        row['age'],
        row['country'],
        row['birth_year']
    ))

print ("Exported completed ")
conn.commit()
cur.close()
conn.close()