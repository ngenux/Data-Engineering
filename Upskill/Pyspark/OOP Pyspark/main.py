from pyspark.sql import *
from pyspark.sql.functions import *
from classes.spark_basics_class import SparkBasics

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    # Instantiate the object
    spark_obj = SparkBasics(spark, "data/invoice_data_2.csv")

    # Generate aggregate values for the provided column
    spark_obj.generate_agg_values("TotalQuantity")

    # Apply the window aggregation function
    spark_obj.window_aggregate_average(["Country"], "week_of_year", -3, "InvoiceValue")

    # Fill Null Values for the given column with the provided value
    df = spark_obj.fill_null_value("Country", "No Country")
