import re
import urllib
from kafka import KafkaProducer, KafkaClient
import sys
import json
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
from twitter import *

class Producer(object):

    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self, source_symbol):
        topic ='twitter_stream'
        


#Variables that contains the user credentials to access Twitter API
    
        consumer_key="consumer_key"
        consumer_secret="consumer_secret"
        access_token="access_token"
        access_token_secret="a_token_secret"
        count=0
        

        auth = OAuth(access_token,access_token_secret,consumer_key,consumer_secret)


        stream = TwitterStream(domain='userstream.twitter.com', auth = auth, secure = True)

       
        tweet_iter = stream.statuses.sample()
        for iter in tweet_iter:
            count = count +1 
            print(iter)
            try:
                self.producer.send(topic, json.dumps(iter['text']))
            except:
                print count
        
     



if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
