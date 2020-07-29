
#broadcast
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
import time

path = 'hdfs://master:9000/'

spark = SparkSession.builder.appName("bc_join").getOrCreate()
sc = spark.sparkContext
sql_c = SQLContext(sc)

trip_data_path = path + "yellow_tripdata_1m.parquet"
trip_vendors_path = path + 'yellow_tripvendors_1m.parquet'


start = time.time()
print("************start: {}***********".format(start))

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



