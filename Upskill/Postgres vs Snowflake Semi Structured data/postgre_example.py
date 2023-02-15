import psycopg2
from psycopg2.extras import Json

# Connect to the database
conn = psycopg2.connect(
    host="paropostrpt.cgltfgjn7lnm.us-west-2.rds.amazonaws.com",
    database="legacy_warehouse",
    user="fivetran",
    password="n3wP455!"
)

# Create a new table with an "id" column and a "data" column
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS adaptiveai.sample_table (
        id integer PRIMARY KEY,
        data jsonb
    )
""")

# Insert two new rows into the table
data1 = {
    "name": "John Doe",
    "age": 30,
    "address": {
        "city": "New York",
        "state": "NY"
    }
}
data2 = {
    "name": "Jane Smith",
    "age": 25,
    "address": {
        "city": "Los Angeles",
        "state": "CA",
        "zip" : "500085"
    }
}
cur.execute("INSERT INTO adaptiveai.sample_table (id, data) VALUES (%s, %s), (%s, %s)", (1, Json(data1), 2, Json(data2)))

# Retrieve all rows from the table
cur.execute("SELECT * FROM adaptiveai.sample_table")
rows = cur.fetchall()
print("All rows in the table after inserts:")
for row in rows:
    print(row)

# Upsert two rows in the table
data3 = {
    "name": "Bob Johnson",
    "age": 45,
    "address": {
        "city": "Chicago",
        "state": "IL"
    }
}
data4 = {
    "name": "Alice Lee",
    "age": 35,
    "address": {
        "city": "San Francisco",
        "state": "CA"
    }
}
cur.execute("""
    INSERT INTO adaptiveai.sample_table (id, data)
    VALUES (%s, %s), (%s, %s)
    ON CONFLICT (id) DO UPDATE
    SET data = EXCLUDED.data
""", (3, Json(data3), 2, Json(data4)))

# Retrieve all rows from the table again
cur.execute("SELECT * FROM adaptiveai.sample_table")
rows = cur.fetchall()
print("All rows in the table after upserts:")
for row in rows:
    print(row)


# Close the cursor and connection
cur.close()
conn.commit()
conn.close()

