import time
import json
from json import dumps
from kafka import KafkaProducer
from time import sleep
import requests as req
import pandas as pd


brokers='localhost:9092'
topic='tweets_covid'
sleep_time=1




producer = KafkaProducer(bootstrap_servers=[brokers],value_serializer=lambda x: dumps(x).encode('utf-8'),batch_size=10)


while(True):
    print("Getting new data...")
    tweets=pd.read_csv("tweets_covid.csv")
    #for tex in tweets.to_dict(orient='records'):
    tex=tweets.to_dict(orient='records')
    producer.send(topic, tex)
    producer.flush(30)
    #time.sleep(sleep_time)
    break
