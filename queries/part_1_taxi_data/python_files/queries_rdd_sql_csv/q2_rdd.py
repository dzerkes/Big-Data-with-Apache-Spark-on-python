#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import DateType
import time
import math

t1 = time.time()

spark = SparkSession.builder.appName("Q1").getOrCreate()

sc = spark.sparkContext

path_trip = "hdfs://master:9000/yellow_tripdata_1m.csv"
path_ven = "hdfs://master:9000//yellow_tripvendors_1m.csv"



rdd = sc.textFile("from pyspark.sql import SparkSession")

spark = SparkSession.builder.appName("Q2").getOrCreate()

sc = spark.sparkContext


trip_data = sc.textFile(path_trip)
vendors_data = sc.textFile(path_ven)

def harvesine(long_st, lat_st, long_sp, lat_sp):
    R = 6371
    a = math.sin(.5 *(lat_st - lat_sp))**2 + math.cos(lat_st)*math.cos(lat_sp)*math.sin(.5*(long_st - long_sp))**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R*c
    return d

def filter_data(x):
    row = x.split(",")
    start_time = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
    start_long = float(row[3])
    start_lat = float(row[4])
    end_long = float(row[5])
    end_lat = float(row[6])

    return (start_time < end_time) and ((start_long != end_long) and (start_lat != end_lat)) and (start_lat >= 40) and (start_lat <= 45) & (end_lat >= 40) & (end_lat <= 45) and (start_long >= -80) & (start_long <= -71) & (end_long >= -80) & (end_long <= -71)

def parseData(x):
    x = x.split(",")
    ID = x[0]
    start_datetime = datetime.strptime(x[1], "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S")
    duration = (end_datetime - start_datetime).total_seconds() / 60.0
    distance = harvesine(float(x[3]), float(x[4]), float(x[5]), float(x[6]))
    return ID, (duration, distance)

trip_data = trip_data.filter(filter_data).map(lambda x: (parseData(x)))
#print(trip_data.take(1))

vendors_data = vendors_data.map(lambda x: (x.split(",")[0], (x.split(",")[1])))


res = trip_data.join(vendors_data).map(lambda x: (x[1][1], (x[1][0][0], x[1][0][1]))).reduceByKey(lambda x, y: x if x[1] > y[1] else y).          sortByKey(ascending = True).map(lambda x: (x[0],x[1][1],x[1][0]))



for i in res.collect():
    print(i)

t2 = time.time()

total_time = t2 - t1

print('total time to run: ', total_time)

