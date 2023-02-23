from pyspark.sql import SparkSession
import pandas as pd
import pymysql
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:3.3.1" pyspark-shell'

# Set up the SparkSession with the appropriate configurations for S3
spark = SparkSession.builder \
    .appName('s3-read') \
    .config('spark.hadoop.fs.s3a.access.key', "****************************") \
    .config('spark.hadoop.fs.s3a.secret.key', "**********************************") \
    .config('spark.hadoop.fs.s3a.endpoint', 's********') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()
conn = pymysql.connect(user='*****', database='spark',
                               password='*******',
                               host="localhost",
                               port=3306)
cursor = conn.cursor()
query = "SELECT * FROM orders1"
query1 = 'SELECT * FROM products1'
# Create a pandas dataframe
pdf = pd.read_sql(query, con=conn)
pdf1 = pd.read_sql(query1, con=conn)
conn.close()
# Convert Pandas dataframe to spark DataFrame
order_df = spark.createDataFrame(pdf)
products_df = spark.createDataFrame(pdf1)
df_join = products_df.join(order_df, order_df.product_id == products_df.product_id, "inner") \
    .drop(order_df.product_id)
df_join.show()

# writing the data
df_join.write.format('csv').option('header','true') \
    .save('s3a://sparketl/pyspark/data.csv',mode='overwrite')
# reading the data
s3_df = spark.read.csv("s3a://sparketl/pyspark/data.csv/",header=True,inferSchema=True)
s3_df.show()
