#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, FloatType, LongType
from pyspark.sql.functions import col, hour, asc, udf, dayofmonth, desc,avg
import time

t1 = time.time()


path_trip = "hdfs://master:9000/yellow_tripdata_1m.csv"


# define a scructType schema for the dataframe
Schema = StructType([ StructField("ID", FloatType(), True),
                      StructField("start_datetime", TimestampType(), True),
                      StructField("stop_datetime", TimestampType(), True),
                      StructField("start_long", FloatType(), True),
                      StructField("start_lat", FloatType(), True),
                      StructField("stop_long", FloatType(), True),
                      StructField("stop_lat", FloatType(), True),
                      StructField("cost", FloatType())
                    ])

spark = SparkSession.builder.appName("q1_sql").getOrCreate()

trip_data = spark.read.format("csv").options(header='false').schema(Schema).load(path_trip)


# set filters
trip_data = trip_data.filter(((col("start_long") != col("stop_long")) & (col("start_lat") != col("stop_lat"))) &                              (col("start_datetime") < col("stop_datetime")) & (col("start_lat") >= 40) & (col("start_lat") <= 45) &                             (col("stop_lat") >= 40) & (col("stop_lat") <= 45) & (col("start_long") >= -80) & (col("start_long") <= -71) &(col("stop_long") >= -80) & (col("stop_long") <= -71))

# register table
trip_data.registerTempTable("trip_data")


trip_data = trip_data.withColumn("start_datetime", hour(col("start_datetime")))
#trip_data.show(3)

res = trip_data.groupBy('start_datetime').agg({'start_long': 'mean', 'start_lat':'mean'}).sort('start_datetime')

res.show(24)

t2 = time.time()

total_time = t2-t1

print('\n')
print("total time to run: ", total_time)

