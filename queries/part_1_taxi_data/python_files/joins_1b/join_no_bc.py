from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

import time

path = 'hdfs://master:9000/'
trip_data_path = path + "yellow_tripdata_1m.parquet"
trip_vendors_path = path + 'yellow_tripvendors_1m.parquet'

start = time.time()
print("************start: {}***********".format(start))

#sconf = SparkConf()
#sconf.setAppName("MyApp") 
#setting sql join ...
#sconf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
#sc = SparkContext(conf=sconf)
#####
#sql_c = SQLContext(sc)

spark = SparkSession.builder.appName("no_bc").getOrCreate()
sc = spark.sparkContext
sql_c = SQLContext(sc)
sql_c.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")


trip_data = sql_c \
    .read \
    .parquet(trip_data_path)

trip_vendors = sql_c \
    .read \
    .parquet(trip_vendors_path) 



trip_joined = trip_data.join(trip_vendors.limit(50), "trip_id", "inner")
trip_joined.explain()
trip_joined.show()

first = time.time()
print("******time elapsed: {} *********".format(first - start))


