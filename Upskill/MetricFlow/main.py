import metricflow
import psycopg2

# Connect to your MySQL database
#cnx = mysql.connector.connect(user='your-username', password='your-password', host='your-host', database='your-database')


def get_connection():
    database = 'legacy_warehouse'
    user = 'fivetran'
    password = 'n3wP455!'
    host = 'paropostrpt.cgltfgjn7lnm.us-west-2.rds.amazonaws.com'
    port = '5432'
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    return conn

cnx = get_connection()

# Define a data source for collecting metrics
#source = metricflow.Source(name='legacy_warehouse', source_type='database', config={
#    'host': 'paropostrpt.cgltfgjn7lnm.us-west-2.rds.amazonaws.com',
#    'port': 5432,
#    'username': 'fivetran',
#    'password': 'n3wP455!',
#    'database': 'legacy_warehouse'
#})

# Define a metric to collect
#metric = metricflow.Metric(name='Average IR')

# Collect metrics from the database
cursor = cnx.cursor()
cursor.execute('select avg(ir) from adaptiveai.project_month_dim')
result = cursor.fetchone()[0]



# Send the collected metric to MetricFlow
#metric.send(value=result, source=source)


#metricflow.client.send(
#    name='average_ir',
#    value=result,
#    tags={
#        'status_code': '200',
#        'source': 'My App'
#    }
#)

metrics.send(name='response_time', value=result, tags={'status_code': '200', 'source': 'My App'})