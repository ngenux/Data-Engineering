import snowflake.connector

snowflake.connector.paramstyle = 'qmark'

# Set up connection
conn = snowflake.connector.connect(
    user='',
    password='',
    account='',
    role='',
    database='',
    schema=''
)

cursor = conn.cursor()

# create table
cursor.execute('CREATE or REPLACE TABLE dev_dwh.dwh.stage_sample_table (id INT, data VARIANT)')

# insert data
data1 = '{"name": "John Doe", "age": 30, "address": {"city": "New York", "state": "NY"}}'
data2 = '{"name": "Jane Doe", "age": 35, "address": {"city": "San Francisco", "state": "CA"}}'

cursor.execute("""insert into dev_dwh.dwh.stage_sample_table(id, data) select 1, to_variant(?);""", [data1])

# commit changes
conn.commit()

upsert_query = "MERGE INTO dev_dwh.dwh.stage_sample_table s USING (SELECT 1 as id_col, to_variant(?) as data_col) d " \
               "ON s.id = d.id_col WHEN MATCHED THEN UPDATE SET s.data = d.data_col WHEN NOT MATCHED THEN INSERT (id, " \
               "data) VALUES (d.id_col, d.data_col);"
cursor.execute(upsert_query, [data2])

# select data
cursor.execute('SELECT * FROM dev_dwh.dwh.stage_sample_table')
for row in cursor.fetchall():
    print(row)

# close connection
cursor.close()
conn.close()
