# WordHub

A real time pipeline for NLP


Abuse Detection:
  Detect users and tweets that may be potentialy abusive, in order to prevent other users from getting offended.
  
Text Prediction:
  Given a word, the top words that follow the given word based on the usage of all the twitter users are given as an output. This is done by finding word-cooccurence on the tweets.


PIPELINE:

![alt text](https://github.com/ann05/NLP_Data_Pipeline/blob/master/pipeline.PNG)




# 1. Data:

The data is streamed from twitter Api using python and also from a 150 GB twitter dump on Aws S3.

# 2. Data Ingestion:

Data is ingested into  Apache Kafka, to a topic called Twitter_stream and Spark Sreaming reads the data from the topics. The data is produced and sent to Kafka using the Kafka_producer.py script. I have different scripts for data from Twitter API and S3.

# 3.Stream Processing:

The data is processed using Spark Streaming.
For abuse detection, the text and the user id are obtained, the text is checked for any phrase/ words that were deeemed abusive intially. If the text contains the phrases/words it is flagged as abusive.
For term prediction, the text is filtered and any emojis, links, etc are filtered out and the words are processed as Bi-grams. The formed tuples are converted to json and inserted into elasticsearch.

# 4.Database:
Elasticsearch is used as the database, the index for abuse detection is:
index = "tweet_abuse", body={"mappings":          \
                                                {"na":                \
                                                {"properties":        \
                                                {"text":            \
                                                {"type": "text" }, \
                                                "id":          \
                                                {"type": "text"},  \
                                                "flag":              \
                                                {"type": "integer"}}}}}
The tweets are indexed in a way where they can be queried for user, text or flagged content.

# 5.Front End:
Kibana is used as the front end for Elasticsearch, it can be downloaded and configured to map the elasticsearch indexes. Data can be queried using Kibana.
A list of tweets, users and flags are presented using Kibana for Abuse Detection
A word cloud of top words that follow the given word are depicted as a word cloud for text prediction.
            
            




