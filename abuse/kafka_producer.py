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
    
        consumer_key="dIhjRcszok2R3LyK0VgYSded8"
        consumer_secret="IxujimHa6JJcWS8mzfC57i9GbgQSc7qst49Fvqu0aglyuykecQ"
        access_token="2493349752-ao5qcIqhAga9jdk2N6RJTxeRIuQXaFwRk9RUhRb"
        access_token_secret="T0sYiVKJQsYRShQopO1Kvqwbr7Bx0Ky3DXs8kdRuxEUTZ"
        count=0
        

        auth = OAuth(access_token,access_token_secret,consumer_key,consumer_secret)


        stream = TwitterStream(domain='userstream.twitter.com', auth = auth, secure = True)


        search_term = "fuck, ass, bitch, Hp, Lenovo, Samsung, Apple, Razor,Dell"

        tweet_iter = stream.statuses.filter(track = search_term, language = 'en')
        #tweet_iter = stream.statuses.sample()
        for iter in tweet_iter:
            count = count +1 
            #print(iter)
            #l=json.loads(iter)
            try:
                p={"text":iter['text'],"id":iter['id']}
                #print p
                self.producer.send(topic,json.dumps(p))
                #self.producer.send(topic, json.dumps(iter))
            except:
                print count
        
     


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
