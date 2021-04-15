import sys
 
from pyspark import SparkContext, SparkConf
import pandas as pd
import map_reduce
from map_reduce import map_reduce


import time
import json
from json import dumps
from kafka import KafkaConsumer
from time import sleep
import requests as req
import ast
import json
import seaborn as sns
import matplotlib.pyplot as plt


if __name__ == "__main__":
    brokers='localhost:9092'
    topic='tweets_covid'
    offset='earliest'

    consumer = KafkaConsumer(bootstrap_servers=brokers)
    consumer.subscribe([topic])
    sleep_time=10
    message_list=[]
    sc = SparkContext.getOrCreate()
    data_dict={"rows":None,"time_for_loading":None,"time_for_hashtag":None,"time_for_users":None,"time_for_cleaning":None,"time_for_sentiment_prediction":None}
    while(True):    
        for count,message in enumerate(consumer):

            
            message_list.append(json.loads(message.value))
            t1=time.time()
            df=pd.DataFrame().from_dict(message_list)
            data_dict["time_for_loading"]=time.time()-t1
            data_dict["rows"]=len(df)
            
            sc = SparkContext.getOrCreate()
            #rdd = sc.parallelize(tweets)
            m1=map_reduce.map_reduce(sc)
            t2=time.time()
            hashtags=m1.hashtag_count(df.text)
            data_dict["time_for_hashtag"]=time.time()-t2
            t3=time.time()
            mentioned_users=m1.mentioned_user_count(df.text)
            data_dict["time_for_users"]=time.time()-t3
            print("*"*20+"Top 5 hashtags by frequency"+"*"*20)
            print(hashtags.collect()[0:5])
            print("\n")
            print("*"*20+"Top 5 mentioned users hashtags by frequency"+"*"*20)
            print(mentioned_users.collect()[0:5])
            print("\n")
            t4=time.time()
            cleaned_text=m1.RemoveOverallNoise(df.text)
            b=m1.clean(cleaned_text)
            data_dict["time_for_cleaning"]=time.time()-t4
            t5=time.time()
            c=m1.sentiment_score(b)
            d=m1.sentiment_label(c)
            data_dict["time_for_sentiment_prediction"]=time.time()-t5
            
            
            t6=time.time()
            e=m1.calculate_top_words_sentiment(b,d)
            data_dict["time_for_words_by_sentiment"]=time.time()-t6
            f=m1.calculate_sentiment_total(d)
            
            

            print("*"*20+"Top 5 frequent words by Neutral sentiment"+"*"*20)
            print(e.filter(lambda x: x[0][1]=='Neutral').take(5))
            print("\n")
            print("*"*20+"Top 5 frequent words by Positive sentiment"+"*"*20)
            print(e.filter(lambda x: x[0][1]=='Positive').take(5))
            print("\n")
            print("*"*20+"Top 5 frequent words by Negative sentiment"+"*"*20)
            print(e.filter(lambda x: x[0][1]=='Negative').take(5))
            print("\n")
            print("*"*20+"Total tweets by sentiment"+"*"*20)
            print(f.collect())
            print("\n\n\n")
            df=pd.DataFrame(data_dict,index=[count])
            df.to_csv("map-reduce_timings.csv",mode='a',header=False,index=False)
            