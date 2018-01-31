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
        
        opener = urllib.URLopener()
        myurl = "https://s3-us-west-2.amazonaws.com/timo-twitter-data/2016-02-08-11-57_tweets.txt"
        myfile = opener.open(myurl)

        for line in myfile:
            try:
                d = json.loads(line)
                print d
            except: 
                continue


            self.producer.send(topic, json.dumps(d))

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
            
            
    
