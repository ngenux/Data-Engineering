from pyspark.sql import *
from pyspark.sql import functions as F
import pyodbc as odbc
import pandas as pd

from azure.storage.blob import BlobServiceClient


db = {'servername': 'DESKTOP-T38U86N', 'database': 'AdventureWorks2019'}

conn = odbc.connect(
    'DRIVER={SQL Server};SERVER=' + db['servername'] + ';DATABASE=' + db['database'] + ';Trusted_Connection=yes')

table_name = 'dbo.products'
# query = "select * from " + table_name
# df = pd.read_sql(query, conn)
# print(df.head(10))

if __name__ == "__main__":
    print("pyspark application started")
    spark = SparkSession \
        .builder \
        .appName("pyspark demo") \
        .master("local[3]") \
        .getOrCreate()

    query = f"SELECT * FROM {table_name}"
    pdf = pd.read_sql(query, conn)
    productDF = spark.createDataFrame(pdf)
    # productDF.show()

    # Replacing null values in product dataframe
    product_cleaned_df = productDF.na.fill({"ProductID": "11", "size": "M", "weight": "100"})
    # product_cleaned_df.show()

    table = 'dbo.SalesOrderDetail'
    query1 = f"SELECT * FROM {table}"
    pdf = pd.read_sql(query1, conn)
    Salesdf = spark.createDataFrame(pdf)
    # Salesdf.show()

    sales_cleaned_df = Salesdf.drop_duplicates()
    # sales_cleaned_df.show()

    df_join = sales_cleaned_df.join(product_cleaned_df, sales_cleaned_df.ProductID == product_cleaned_df.productid,
                                    "leftouter").select(
        sales_cleaned_df.ProductID,
        sales_cleaned_df.LineTotal,
        product_cleaned_df.name,
        product_cleaned_df.color,
        product_cleaned_df.size
    )
    # df_join.show()

    df_join.createTempView("products")
    # spark.sql("select * from products").show()

    df = spark.sql("select ProductID,name,color,sum(LineTotal) as total_sales from products group by ProductID,name,"
                   "color")
    df1 = df.toPandas()
    output = df1.to_csv (index_label="idx", encoding = "utf-8")

    from azure.storage.blob import BlobServiceClient

    connection_string = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client("raw")
    container_client.upload_blob(name="OutFilePy.csv", data=output)


      


