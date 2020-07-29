#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import DateType
import time

t1 = time.time()

spark = SparkSession.builder.appName("Q1").getOrCreate()

sc = spark.sparkContext



path_trip = "hdfs://master:9000/yellow_tripdata_1m.csv"



#rdd = sc.textFile("from pyspark.sql import SparkSession")
spark = SparkSession.builder.appName("Q1_rdd").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile(path_trip)

def extract_hour(x):
    return "{:02d}".format(datetime.strptime(x, '%Y-%m-%d %H:%M:%S').hour)

'''filter(logical condition(s)): Return a new RDD containing only the elements that satisfy a predicate.'''

def filter_data(x):
    values = x.split(",")
    start_time = datetime.strptime(values[1], '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(values[2], '%Y-%m-%d %H:%M:%S')
    start_long = float(values[3])
    start_lat = float(values[4])
    end_long = float(values[5])
    end_lat = float(values[6])
    return (start_time < end_time) and ((start_long != end_long) and (start_lat != end_lat)) and (start_lat >= 40) and (start_lat <= 45)          and (end_lat >= 40) and (end_lat <= 45) and (start_long >= -80) and (start_long <= -71) and (end_long >= -80) and (end_long <= -71)

res = rdd.filter(filter_data).map(lambda x: (extract_hour(x.split(',')[1]),(float(x.split(',')[3]), float(x.split(',')[4]),1))).               reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2])).sortByKey(ascending = True).map(lambda x: (x[0],x[1][0]/x[1][2], x[1][1]/x[1][2]))


t2 = time.time()

total_time = t2-t1

print('\n')

for i in res.collect():
        print(i)

print("total time to run: ", total_time)


# ####  Ποια είναι η μέση τιμή του γεωγραφικού μήκους και πλάτους επιβίβασης ανά ώρα έναρξης της διαδρομής;  
#   
# #### Ταξινομείστε το αποτέλεσμα με βάση την ώρα έναρξης σε αύξουσα σειρά και αγνοείστε dirty εγγραφές που προκαλούν προβληματικά αποτελέσματα (π.χ. Γεωγραφικό μήκος / πλάτος με μηδενική τιμή)

# Διαδικασία:  
#   
# - κρατάμε τις τούπλες: (ώρα εκκίνησης, (γεωγραφικό μήκος, γεωγραφικό πλάτος))  
# - κάνουμε reduceByKey έτσι ώστε να αθροίσουμε τα γ.μ. και γ.π. και τις συχνότητες εμφάνισης του κάθε key (ώρα)
# - διαιρούμε τα αθροίσματα με τις συχνότητες εμφάνισης του κάθε key για να εξαχθούν οι Μ.Ο.

# Συνάρτηση filter_data()  
#   
# - χωρίζει κάθε γραμμή επιστρέφοντας τούπλα  
# - σπάει τις ημερομηνίες/ώρες στη μορφή datetime.datetime (Year, Month, Day, Hour, Minute, Second)  
# - κρατάει σε float type τα γ.μ. και γ.π.  
#   
# επιστρέφει μια συνθήκη (predicate) με την οποία θα φιλτραριστεί το rdd που έχουμε
