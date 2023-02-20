import psycopg2
import pandas as pd


class Connection:

    def __init__(self):
        self.conn = None

    def get_connection(self):
        database = 'legacy_warehouse'
        user = 'fivetran'
        password = 'n3wP455!'
        host = 'paropostrpt.cgltfgjn7lnm.us-west-2.rds.amazonaws.com'
        port = '5432'
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        return self.conn

    def extract_data(self, conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        df = pd.DataFrame(rows, columns=colnames)
        return df


