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
#import seaborn as sns
#import matplotlib.pyplot as plt

if __name__ == "__main__":
    brokers='localhost:9092'
    topic='tweets_covid'
    offset='latest'
    consumer = KafkaConsumer(bootstrap_servers=brokers)
    consumer.subscribe([topic])
    sleep_time=10
    message_list=[]
    sc = SparkContext.getOrCreate()
    data_dict={"time_for_loading":None,"time_for_hashtag":None,"time_for_users":None,"time_for_cleaning":None,"time_for_sentiment_prediction":None}
    i=0
    final_time=time.time()
    if int(sys.argv[1])==500:
        batch=0.5
    elif int(sys.argv[1])==1000:
        batch=1
    elif int(sys.argv)==2000:
        batch=2 

    while(True):
        try:
            i=i+1
            message_list=[]
            #records = consumer.poll(60 * 1000)
            for count,message in enumerate(consumer):
                #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                #                          message.offset, message.key,
                #                          message.value))
                
                message_list.append(json.loads(message.value))
                if count==int(sys.argv[1]):
                    break
            
            
            #for tp, consumer_records in records.items():
            #print(records.items())
            
            #for consumer_record in consumer_records:
            #message_list.append()#[json.loads(consumer_records.value) for tp,consumer_records in records.items()]
            #print(message_list)
            #message_list.append(json.loads(message.value))
            t1=time.time()
            df=pd.DataFrame().from_dict(message_list)
            #print(df)
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
            df=pd.DataFrame(data_dict,index=[i])
            print(df)
            df.to_csv("map-reduce-timings.csv",mode='a',header=False,index=False)
            print(i)
            if i==10//batch:
                break
        except Exception as e:
            print(e)
            break
        #consumer.poll() 
        #consumer.seek_to_end()
  
        #break
    print("total time for map-reduce script {}".format(time.time()-final_time))
    df=pd.read_csv("map-reduce-timings.csv",names=["time_for_loading","time_for_hashtag","time_for_users","time_for_cleaning","time_for_sentiment_prediction","rows","time_for_words_by_sentiment"])
    df.to_csv("map-reduce-timings.csv",index=False)
