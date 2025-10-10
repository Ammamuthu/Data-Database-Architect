import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import os

# DB connection
conn = psycopg2.connect(
    dbname="defaultdb",
    user="ashok",
    password="viT29UQCMKkY9VILC7GhFQ",
    host="oilier-eel-16893.j77.aws-ap-south-1.cockroachlabs.cloud",
    port=26257,
    sslmode="verify-full",
    sslrootcert=r"C:\Users\AmmamuthuM\AppData\Roaming\postgresql\root.crt"
)

cur = conn.cursor()

# Load Excel
excel_files = [
    r"C:\Users\AmmamuthuM\OneDrive - ECS Business Solutions PVT LTD\Documents\ECS\Data Arc\Projects\SCD_2_Pipeline\Customer_Data\Day_1.xlsx",
    r"C:\Users\AmmamuthuM\OneDrive - ECS Business Solutions PVT LTD\Documents\ECS\Data Arc\Projects\SCD_2_Pipeline\Customer_Data\Day_2.xlsx",
    r"C:\Users\AmmamuthuM\OneDrive - ECS Business Solutions PVT LTD\Documents\ECS\Data Arc\Projects\SCD_2_Pipeline\Customer_Data\Day_3.xlsx"
]

# --- Process each file ---
for file_path in excel_files:
    print(f"Processing: {os.path.basename(file_path)}")
    df = pd.read_excel(file_path)

# Convert date format
df['Hired_On'] = pd.to_datetime(df['Hired_On'], dayfirst=True)

# Loop through each row
for _, row in df.iterrows():
    cust_id = row['CUSTOMER_ID']
    name = row['NAME']
    city = row['CITY']
    state = row['STATE']
    hired_on = row['Hired_On']
    
    # Step 1: Check for existing active record
    cur.execute("""
        SELECT name, city, state, eff_start_date
        FROM customers
        WHERE customer_id = %s AND current_flag = 'Y'
    """, (cust_id,))
    
    existing = cur.fetchone()

    if existing:
        old_name, old_city, old_state, eff_start_date = existing

        # Check if anything has changed
        if (old_name != name or old_city != city or old_state != state):
            # Step 2: Expire old record
            cur.execute("""
                UPDATE customers
                SET eff_end_date = %s, current_flag = 'N'
                WHERE customer_id = %s AND current_flag = 'Y'
            """, (hired_on - timedelta(days=1), cust_id))

            # Step 3: Insert new version
            cur.execute("""
                INSERT INTO customers (customer_id, name, city, state, eff_start_date, eff_end_date, current_flag)
                VALUES (%s, %s, %s, %s, %s, NULL, 'Y')
            """, (cust_id, name, city, state, hired_on))

    else:
        # Step 4: Insert new customer
        cur.execute("""
            INSERT INTO customers (customer_id, name, city, state, eff_start_date, eff_end_date, current_flag)
            VALUES (%s, %s, %s, %s, %s, NULL, 'Y')
        """, (cust_id, name, city, state, hired_on))

# Commit the changes
conn.commit()
cur.close()
conn.close()

print("SCD Type 2 logic applied successfully.")
