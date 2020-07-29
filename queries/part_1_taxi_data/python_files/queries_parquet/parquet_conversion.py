from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, FloatType, LongType
import time

path ="hdfs://master:9000/"


t1 = time.time()
print("************starting time: {}***********".format(t1))

trip_data_schema = StructType([
    StructField("trip_id", StringType()),
    StructField("trip_start_time", StringType()),
    StructField("trip_end_time", StringType()),
    StructField("trip_start_long", FloatType()),
    StructField("trip_start_lat", FloatType()),
    StructField("trip_end_long", FloatType()),
    StructField("trip_end_lat", FloatType()),
    StructField("trip_cost", FloatType())
    ])

trip_vendors_schema = StructType([
    StructField("trip_id", StringType()),
    StructField("vendor_id", StringType())
    ])

sc = SparkContext("local", "My App2")

sql_c = SQLContext(sc)

trip_data = sql_c \
    .read \
    .format("com.databricks.spark.csv") \
    .schema(trip_data_schema) \
    .option("header", "false") \
    .option("mode", "DROPMALFORMED") \
    .load('hdfs://master:9000/small.csv')


trip_vendors = sql_c \
    .read \
    .format("com.databricks.spark.csv") \
    .schema(trip_vendors_schema) \
    .option("header", "false") \
    .option("mode", "DROPMALFORMED") \
    .load('hdfs://master:9000/small_vendors.csv')


