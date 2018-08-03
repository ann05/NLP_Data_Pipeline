from pyspark.sql.functions import udf
import re
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
brokers = "broker1_ip,broker2_ip,broker3_ip,broker4_ip"

words=["fuck you","fuck","fucker","faggot","son of a bitch","nigger","nigga","twat","wanker","pussy","ass","bitch","cock sucker","dick","asshole","cunt"]


#----------------Elasticsearch------------------------------------
es_access_key = os.getenv('ES_ACCESS_KEY_ID', 'default')
es_secret_access_key = os.getenv('ES_SECRET_ACCESS_KEY', 'default')
master_internal_ip = "master_ip"

#-----------------conncting to elasticsearch--------------------
try:
    es = Elasticsearch(
        [master_internal_ip],
        http_auth=('user', 'password'),
        port=9200,
        sniff_on_start=True
    )

    print 'connected'
except Exception as ex:
    print 'error'

#--------------------creating/ accessing the index----------------------------

try:
    if not es.indices.exists(index="tweet_abuse"):
	es.indices.create(index = "tweet_abuse", body={"mappings":          \
                                                {"na":                \
                                                {"properties":        \
                                                {"text":            \
                                                {"type": "text" }, \
                                                "id":          \
                                                {"type": "text"},  \
                                                "flag":              \
                                                {"type": "integer"}
						}}}})
    print ('created')
except Exception as ex:
    print ex


#------------------------------------------------------Stream Processing-------------------


kafkaStream = KafkaUtils.createDirectStream(ssc, [topics],{"metadata.broker.list":brokers}  )

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint() 

dstream=parsed.map(lambda x : (x['id'],x['text'].encode('utf8')))



def  test(x):
    try:
        t=0
	t2=x[1].split()
        for a in words:
            if a in t2:
                x={'_index':'tweet_abuse','_type':'na','flag':'1','id':x[0],'text':x[1]}
                print a
                t=1
                continue
        if t!=1:
            x={'_index':'tweet_abuse','_type':'na','flag':'0','id':x[0],'text':x[1]}
    except:
        print  "error"
    return(x)
dstream3=dstream.map(lambda x:test(x))
dstream3.pprint()



def data_json(x):
    t=x.collect()
    print t
    try:
	helpers.bulk(es,t)
    except Exception as ex:
	print ex
dstream3.foreachRDD(data_json)


ssc.start()  
ssc.awaitTermination()

