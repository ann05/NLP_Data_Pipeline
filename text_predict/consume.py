
from pyspark.sql.functions import udf
#from nltk import ngrams
import re
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
#    Spark
from elasticsearch import Elasticsearch, helpers
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession
#    json parsing
import json,os

sc = SparkContext(appName="PythonSparkStreaming")
sc.setLogLevel("WARN")
spark = SparkSession.builder.appName("PythonSparkStreaming").master("local").getOrCreate()
ssc = StreamingContext(sc, 1)
topics="twitter_stream"

brokers = "ec2-ip.compute-1.amazonaws.com:9092,ec2-ip.compute-1.amazonaws.com:9092"
#,'ec2-ip.compute-1.amazonaws.com','ec2-ip.compute-1.amazonaws.com','ec2-ip.compute-1.amazonaws.com']

#----------------Elasticsearch------------------------------------
es_access_key = os.getenv('ES_ACCESS_KEY_ID', 'default')
es_secret_access_key = os.getenv('ES_SECRET_ACCESS_KEY', 'default')
master_internal_ip = "ec2-ip.compute-1.amazonaws.com"


try:
    es = Elasticsearch(
        [master_internal_ip],
        http_auth=('elastic', 'changeme'),
        port=9200,
        sniff_on_start=True
    )
    #logging.debug("Elasticsearch Connected")
    print 'connected'
except Exception as ex:
    #logging.debug("Error:", ex)
    print 'error'

try:
    if not es.indices.exists(index="initial"):
	es.indices.create(index = "tweet", body={"mappings":          \
                                                {"na":                \
                                                {"properties":        \
                                                {"key":            \
                                                {"type": "keyword" }, \
                                                "value":          \
                                                {"type": "keyword"},  \
                                                #"date":              \
                                                           #{"type": "double"}
						}}}})
    print ('created')
except:
    print 'error'


#------------------------------------------------------

kafkaStream = KafkaUtils.createDirectStream(ssc, [topics],{"metadata.broker.list":brokers}  )

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint() 


temp=parsed.map(lambda tweet: tweet.encode('utf8'))




temp = temp.map(lambda x: re.sub("[\<].*?[\>]", "", x))
temp=temp.map(lambda x: re.sub("[\@].*?[' ']","",x))
temp =temp.map(lambda x: re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', x))
temp=temp.map(lambda x: re.sub('[^A-Za-z ]+', '', x))
temp=temp.map(lambda x: x.replace('RT',''))

twogram = temp.map(lambda x :x.split(" "))





def ngrams(x):
   for i in range(0, len(x)-1):
	x[i]= (x[i],x[i+1])
   return x

three=twogram.map(lambda x:ngrams(x))
   

data = []

def to_json(x):
    data_json=[]
    for i in x:
	    try:
	        p={'_index':'tweet','_type':'na','key':i[0],'value':i[1]}
	        data_json.append(p)
		#data.append(p)
	    except:
		{}
    return data_json
t1=three.map(lambda x:to_json(x))
"""
"""
def data_json(x):
    t=x.collect()

    print '-------------------------------'

    try:
        helpers.bulk(es,t)
    except:
	print 'cant index'


t2=t1.flatMap(lambda x :x)
t2.foreachRDD(data_json)




 
ssc.start()  
ssc.awaitTermination()

