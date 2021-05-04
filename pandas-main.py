from pandas_pipeline import pandas_pipeline,dataCleaning,sentimentAnalysis
import time
import json
from json import dumps
from kafka import KafkaConsumer
from time import sleep
import requests as req
import ast
import json
#import seaborn as sns
import sys
sys.setrecursionlimit(10**6)
import pandas as pd







if __name__=="__main__":

    brokers='localhost:9092'
    topic='tweets_covid'
    sleep_time=1
    offset='earliest'

    consumer = KafkaConsumer(bootstrap_servers=brokers)
    consumer.subscribe([topic])
    sleep_time=10
    message_list=[]
    time_for_loading=[]
    total_rows=[]
    time_for_hashtag=[]
    time_for_users=[]
    time_for_sentiment=[]
    time_for_cleaning=[]

    if int(sys.argv[1])==500:
        batch=0.5
    elif int(sys.argv[1])==10000:
        batch=10
    elif int(sys.argv[1])==20000:
        batch=20
    data_dict={"rows":None,"time_for_loading":None,"time_for_hashtag":None,"time_for_users":None,"time_for_cleaning":None,"time_for_sentiment":None}
    i=0
    final_time=time.time()
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

       #     for count,message in enumerate(consumer):
           # message_list.append(json.loads(message.value))
            t1=time.time()
            df=pd.DataFrame().from_dict(message_list)
            data_dict["time_for_loading"]=time.time()-t1
            data_dict["rows"]=len(df)

            p1=pandas_pipeline.keyProcessIndicators(df)
            #     p1.location_cleaner()
            t2=time.time()
            hashtags=p1.get_kpi_hashtags()
            hashtags=hashtags[0:7]
            data_dict["time_for_hashtag"]=time.time()-t2
            print("*"*20+"Top 5 hashtags by frequency"+"*"*20)
            print(hashtags)
            print("\n")
            t3=time.time()
            users=p1.get_kpi_users()
            print("*"*20+"Top 5 mentioned users hashtags by frequency"+"*"*20)
            users=users[0:7]
            data_dict["time_for_users"]=time.time()-t3
            print(users)
            print("\n")


                #=p1.get_kpi_tweets()
            t4=time.time()
            p2=dataCleaning.dataCleaning(df.copy())
            p2.preprocess_tweet()
            data_dict["time_for_cleaning"]=time.time()-t4

            t5=time.time()
            p3=sentimentAnalysis.sentimentAnalysis(p2._df)
            p3.main()

            print("*"*20+"Total tweets by sentiment"+"*"*20)
            print(p3._df['Sentiment'].value_counts())
            data_dict["time_for_sentiment"]=time.time()-t5
            print("\n")
            print(data_dict)
            df=pd.DataFrame(data_dict,index=[count])
            print(df)
            print(df.columns)
            df.to_csv("pandas-timings.csv",mode='a',header=False,index=False)
            print(i)
            if i==10//batch:
                break
         except:
            break
    print("total time for pandas script {}".format(time.time()-final_time))
    df=pd.read_csv("pandas-timings.csv",names=["time_for_loading","time_for_hashtag","time_for_users","time_for_cleaning","time_for_sentiment_prediction"])
    df.to_csv("pandas-timings.csv",index=False)


#             h=sns.barplot(x=list(hashtags.index),y='Other_hash',data=hashtags,label='Count')# only 1 column is passed ie x or y
#             h.set_xticklabels(rotation=90,labels = list(hashtags.index))
#             h.set(ylabel = 'Count')
#             plt.title("top_related_hashtags")
#             h.legend()
#             plt.show()
