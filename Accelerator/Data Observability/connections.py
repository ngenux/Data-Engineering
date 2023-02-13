import psycopg2
import pandas as pd


def get_connection():
    database = 'legacy_warehouse'
    user = 'fivetran'
    password = 'n3wP455!'
    host = 'paropostrpt.cgltfgjn7lnm.us-west-2.rds.amazonaws.com'
    port = '5432'
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    return conn


def extract_data(conn,table_name):
    cur = conn.cursor()
    cur.execute("""select * from adaptiveai.""" + table_name)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    df = pd.DataFrame(rows, columns=colnames)
    return df

def return_tables_within_schema(conn,schema_name):
    cur = conn.cursor()
    cur.execute("""select table_name from information_schema.tables where table_schema = '""" + schema_name + """' 
    and table_name like '%dim%'""")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    df = pd.DataFrame(rows, columns=colnames)
    return df
