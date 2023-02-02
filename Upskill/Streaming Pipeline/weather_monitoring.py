from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


kafka_topic_name = 'sampletopic1'
kafka_boostrap_server = 'localhost:9092'
spark = SparkSession.builder.master("local[*]"). \
    appName("Spark streaming with kafka"). \
    config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3").\
    config("spark.jars", "C:/Program Files/sqljdbc_11.2/enu/mssql-jdbc-11.2.3.jre8.jar").\
    getOrCreate()


weather_detail_df = spark \
    .readStream \
    .format("kafka") \
    .option("failOnDataLoss", "false") \
    .option("kafka.bootstrap.servers", kafka_boostrap_server) \
    .option("subscribe", kafka_topic_name) \
    .load()


weather_detail_df = weather_detail_df.select(expr("CAST(value as STRING)"))


transaction_detail_schema = StructType([
    StructField("CityName", StringType()),
    StructField("Temperature", DoubleType()),
    StructField("Humidity", IntegerType()),
    StructField("CreationTime", StringType())
])

weather_detail_df_2 = weather_detail_df.withColumn("value", from_json(col("value"), transaction_detail_schema))
weather_detail_df_2 = weather_detail_df_2.select(weather_detail_df_2["value.CityName"],
                                                 weather_detail_df_2["value.Temperature"],
                                                 weather_detail_df_2["value.Humidity"],)
weather_detail_df_2.printSchema()
db_target_properties = {"user": "arvind" , "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


def foreach_batch_function(df, epoch_id):
    df.write.mode("append").jdbc(url='jdbc:sqlserver://localhost:1433;databaseName = master;encrypt=true;trustServerCertificate=true', table="sparkkafka" , properties= db_target_properties)
    pass


query = weather_detail_df_2.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()

query.awaitTermination()


# query = weather_detail_df_2 \
#    .writeStream \
#    .format("csv") \
#    .option("format", "append") \
#    .option("header", "true") \
#    .trigger(processingTime="5 seconds") \
#    .option("path", "output_path/") \
#    .option("checkpointLocation", "checkpoint/") \
#    .outputMode("append") \
#    .start()


# query.awaitTermination()
