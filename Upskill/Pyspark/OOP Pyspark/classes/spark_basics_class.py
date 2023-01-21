from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


class SparkBasics:
    def __init__(self, spark, path):
        self.df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)
        print("Instantiated the object")

    def generate_agg_values(self, *args):
        for arg in args:
            print("For the variable : " + arg)
            self.df.select(expr("count(" + arg + ") as count")).show()
            self.df.select(expr("sum(" + arg + ") as sum")).show()
            self.df.select(expr("avg(" + arg + ") as avg")).show()
            self.df.select(expr("count(distinct " + arg + ") as distinct")).show()

    def window_aggregate_average(self, partition_by_columns, order_by_columns, no_of_rows, column_name):
        window_partition = Window.partitionBy(partition_by_columns).orderBy(order_by_columns).rowsBetween(-3, Window.currentRow)
        self.df = self.df.withColumn("rolling_average", avg(column_name).over(window_partition))
        self.df.show(5)

    def fill_null_value(self, col_name, value):
        self.df = self.df.withColumn(col_name, expr("coalesce(" + col_name + ",'" + value + "')"))
        return self.df
