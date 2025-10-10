import psycopg2

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
cur.execute("SELECT now();")
print("CockroachDB Time:", cur.fetchone())




