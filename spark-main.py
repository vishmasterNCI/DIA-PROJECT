from spark_pipeline import spark_pipeline,dataCleaning,sentimentAnalysis
import time
import json
from json import dumps
from kafka import KafkaConsumer
from time import sleep
import requests as req
import ast
import json
from pyspark import SparkContext
#import seaborn as sns
import sys
sys.setrecursionlimit(10**6)
import pandas as p
from pyspark.sql import SQLContext
import pandas as pd



if __name__=="__main__":

    brokers='localhost:9092'
    topic='tweets_covid'
    sleep_time=1
    offset='earliest'
    print(sys.argv)
    consumer = KafkaConsumer(bootstrap_servers=brokers)
    consumer.subscribe([topic])
    sleep_time=10
    message_list=[]
    data_dict={"rows":None,"time_for_loading":None,"time_for_hashtag":None,"time_for_users":None,"time_for_cleaning":None,"time_for_sentiment_prediction":None}
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    final_time=time.time()
    i=0
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


            #for count,message in enumerate(consumer):
            #message_list.append(json.loads(message.value))
            t1=time.time()
            df = sqlContext.createDataFrame(message_list)
            data_dict["time_for_loading"]=time.time()-t1
            data_dict["rows"]=df.count()
            s1=spark_pipeline.keyProcessIndicators(df)
            #     p1.location_cleaner()
            t2=time.time()
            hashtags=s1.get_hashtag()
            data_dict["time_for_hashtag"]=time.time()-t2
            print("*"*20+"Top 5 hashtags by frequency"+"*"*20)
            hashtags.show(5)
            print("\n")
            t3=time.time()
            users=s1.get_users()
            data_dict["time_for_users"]=time.time()-t3
            print("*"*20+"Top 5 mentioned users hashtags by frequency"+"*"*20)
            users.show(5)
            print("\n")



                #=p1.get_kpi_tweets()
            t4=time.time()
            s2=dataCleaning.dataCleaning(df)
            s2.preprocessing()
            data_dict["time_for_cleaning"]=time.time()-t4
            t5=time.time()
            s3=sentimentAnalysis.sentimentAnalysis(s2._df.select('words'))
            s3.text_classification()
            data_dict["time_for_sentiment_prediction"]=time.time()-t5
            print("*"*20+"Total tweets by sentiment"+"*"*20)
            s3._df.select('Sentiment').groupby("Sentiment").count().sort('count', ascending=False).show()
            print("\n")


            df=pd.DataFrame(data_dict,index=[i])
            print(df)
            df.to_csv("sparkdf-timings-{}.csv".format(batch),mode='a',header=False,index=False)
            print(i)
            if i==10//batch:
                break
         except Exception as e:
                        print(e)
                        break
    print("total time for sparkdf script {}".format(time.time()-final_time))
    df=pd.read_csv("sparkdf-timings.csv",names=["rows","time_for_loading","time_for_hashtag","time_for_users","time_for_cleaning","time_for_sentiment_prediction"])
    df.to_csv("sparkdf-timings-{}.csv".format(batch),index=False)
#             h=sns.barplot(x=list(hashtags.index),y='Other_hash',data=hashtags,label='Count')# only 1 column is passed ie x or y
#             h.set_xticklabels(rotation=90,labels = list(hashtags.index))
#             h.set(ylabel = 'Count')
#             plt.title("top_related_hashtags")
#             h.legend()
#             plt.show()
