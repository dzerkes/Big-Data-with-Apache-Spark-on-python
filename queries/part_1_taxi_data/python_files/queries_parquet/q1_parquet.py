#!/usr/bin/env python
# coding: utf-8




from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, hour, asc, udf, dayofmonth, desc,avg
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, FloatType, LongType
import math
import time
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


path_trip="hdfs://master:9000/yellow_tripdata_1m.parquet"
path_ven="hdfs://master:9000/yellow_tripvendors_1m.parquet"


t1 = time.time()

####structuring tripdata#####

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

spark = SparkSession.builder.appName("q2_sql").getOrCreate()

trip_data = spark.read.format("csv").schema(trip_data_schema).option("header", "false").load(path_trip)

### filtering trip_data ###
trip_data = trip_data.filter((col("trip_start_lat") >= 40) & (col("trip_start_lat") <= 45) &(col("trip_end_lat") >= 40) & (col("trip_end_lat") <= 45) & (col("trip_start_long") >= -80) & (col("trip_start_long") <= -71) & (col("trip_end_long") >= -80) & (col("trip_end_long") <= -71))


####structuring vendor_data####
trip_vendors_schema = StructType([
    StructField("trip_id", StringType()),
    StructField("vendor_id", StringType())
    ])


trip_vendors = spark.read.format("csv").schema(trip_vendors_schema).option("header", "false").load(path_ven)


##creating useful columns on tripdata
def haversine(long1, lat1, long2, lat2):
    R = 6371
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((long2-long1)/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d

haversine = udf(haversine,DoubleType())

def duration(start, end):
    start_time = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    duration = (end_time - start_time).total_seconds() / 60.0
    return duration

duration = udf(duration, DoubleType())

trip_data = trip_data.withColumn("Distance", (haversine(col("trip_start_long"),col("trip_start_lat"),col("trip_end_long"),col("trip_end_lat"))))
trip_data = trip_data.withColumn("Duration", (duration(col("trip_start_time"),col("trip_end_time"))))

trip_joined = trip_data.join(trip_vendors, "trip_id", "inner")

w = Window.partitionBy('vendor_id')
trip_joined = trip_joined.withColumn('max_distance', F.max('Distance').over(w))
trip_joined.registerTempTable("trip_joined")

spark.sql('select tj.vendor_id as vendor_id, tj.Distance as Distance, tj.Duration as Duration from             trip_joined as tj where tj.max_distance == tj.Distance').show()


t2 = time.time()

total_time = t2-t1
print('total time to run: ', total_time)





