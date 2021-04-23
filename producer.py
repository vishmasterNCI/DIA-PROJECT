import time
import json
from json import dumps
from kafka import KafkaProducer
from time import sleep
import requests as req
import pandas as pd

import pydoop.hdfs as hd
#hdfs://localhost:9000/
with hd.open("DIA-PROJECT/tweets.csv") as f:
    print(f)
tweets =  pd.read_csv("/home/hduser/DIA-PROJECT/tweets.csv",names = ['polarity','id','date','query','user','text'],encoding='ISO-8859-1')
brokers='localhost:9092'
topic='tweets_covid'
sleep_time=3000




producer = KafkaProducer(bootstrap_servers=[brokers],value_serializer=lambda x: dumps(x).encode('utf-8'),batch_size=10)
#tweets=pd.read_csv("tweets_covid.csv")
def chunks(L, n):
    for i in range(0, len(L), n):
        yield L[i:i+n]

text=tweets[["text"]].to_dict(orient='records')

#text=chunks(text,1000)
while(True):
    print("Getting new data...")
    #tweets=pd.read_csv("tweets_covid.csv")
    for tex in text:
        #tex=tweets[["text"]].to_dict(orient='records')
        print(tex)
        producer.send(topic,tex)
        producer.flush()
    #producer.close()
    #time.sleep(sleep_time)
    #break
