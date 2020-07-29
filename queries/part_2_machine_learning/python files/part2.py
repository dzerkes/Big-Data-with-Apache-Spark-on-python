from pyspark.sql import SparkSession
from datetime import datetime
import math
import time
import re
import nltk 
from pyspark.sql.functions import lit
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from nltk.corpus import stopwords as sw

stopwords = sw.words('english') + ['xxxx' , 'xxx' ,'' , 'xx' , 'x']

spark = SparkSession.builder.appName("tfidf").getOrCreate()
from pyspark.sql.functions import avg
sc = spark.sparkContext


def filter_data(x):
    values = x.split(",")
    if len(values) == 3:
        date = values[0]
        label = values[1]
        comment = values[2]
        if (len(comment) != 0):
            return (date.startswith("201"))  
        else:
            return False
    else:
        return False
    

def parse_data(x):
    values = x.split(",")
    label = values[1]
    comment = re.sub('[^a-zA-Z]+', ' ', values[2]).lower().strip()
    return label,comment


data = sc.textFile("hdfs://master:9000/customer_complaints.csv").filter(filter_data).map(parse_data)
regs_number = data.count()

lexicon_size = 50
unique_words = data.flatMap(lambda x: x[1].split(" ")).\
                          filter(lambda x: x not in stopwords).\
                          map(lambda x : (x,1)).\
                          reduceByKey(lambda x,y : x+y ).\
                          sortBy(lambda x: x[1],ascending=False).\
                          map(lambda x : x[0]).\
                          take(lexicon_size)

uwords = sc.broadcast(unique_words)

#customer complaints
cc = data.map(lambda x: (x[0],x[1].split(" "))).\
            map(lambda x : (x[0], [y for y in x[1] if y in uwords.value])).\
            filter(lambda x: len(x[1]) !=0).\
            zipWithIndex()
            #((string_label,list_of_words),sentence_index)
    


#Ft,d (we'll calculate the double normalized TF later)
tf = cc.flatMap(lambda x : [((y,x[0][0],x[1]),1) for y in x[0][1]]).\
            reduceByKey(lambda x,y : x + y).\
            map(lambda x : (x[0][0],(x[0][1],x[0][2],x[1])))
            #((word,label,sentence_index),tf) before map
            #(word,(label,sentence_index,tf)) after map -> final form
    

#IDF
idf = cc.flatMap(lambda x: [(y,1) for y in set(x[0][1])]).\
              reduceByKey(lambda x ,y: x+y).\
              sortBy(lambda x: x[1],ascending=False).\
              map(lambda x : (x[0],math.log(regs_number/x[1])))


    
##tfidf rdd
tfidf = tf.join(idf)

res = tfidf.map(lambda x: (uwords.value.index(x[0]),x[1])).\
            map(lambda x: ((x[1][0][1],x[1][0][0]),(x[0],x[1][0][2],x[1][1]))).\
            map(lambda x: (x[0],([x[1][0]],[x[1][1]],[x[1][2]]))).\
            reduceByKey(lambda x,y: (x[0] + y[0],x[1] + y[1],x[2]+y[2])).\
            map(lambda x: (x[0],(x[1][0],[0.5 + 0.5*y/max(x[1][1]) for y in x[1][1]],x[1][2]))).\
            map(lambda x: (x[0][1],lexicon_size,[(x[1][0][i],x[1][1][i]*x[1][2][i]) for i in range(len(x[1][0]))])).\
            map(lambda x: (x[0],x[1],sorted(x[2],key=lambda y:y[0]))).\
            map(lambda x: (x[0],x[1],[y[0] for y in x[2]],[k[1] for k in x[2]]))
            
            

for i in res.take(5):
    print(i,"\n")

##sparsevector
sv = res.map(lambda x :(x[0],SparseVector(lexicon_size,x[2],x[3])))
####dataframe 
cc_df = sv.toDF(["label_f","features"])
stringIndexer = StringIndexer(inputCol="label_f",outputCol = "label")
stringIndexer.setHandleInvalid("skip")
stringIndexerModel = stringIndexer.fit(cc_df)
cc_df = stringIndexerModel.transform(cc_df)

### stratified
fractions = cc_df.select("label").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()

#remove duplicates
cc_df = cc_df.dropDuplicates()

#check time without caching training set and with caching it.
train = cc_df.sampleBy("label", fractions=fractions, seed=10).cache() # remove and add cache

test = cc_df.subtract(train)

#print test, train rows and rows for each category on both train/test set
print("Rows of full dataset",cc_df.count())
print("Rows of training set",train.count()) # train rows
print("Rows of test set",test.count()) # test rows

print("Rows of each category for full dataset")
cc_df.groupby("label").count().show()

print("Rows of each category for training dataset")
train.groupby("label").count().show()

print("Rows of each category for test dataset")
test.groupby("label").count().show()
### MLP training and accuracy on test set

#specify layers
layers = [lexicon_size,40,30,20,18] # 1st layer is lexicon size and last #of classes

# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)
#####Start of training#######
start = time.time()
model = trainer.fit(train)
end = time.time()
#####End of training#######
result = model.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")


print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
print("Training was done in time: ", end-start)
