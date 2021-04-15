import re
import tweepy
from tweepy import OAuthHandler
import pandas as pd
import numpy as np
import natsort
from collections import OrderedDict

import nltk
from nltk import sent_tokenize, word_tokenize, pos_tag
#import pysentiment as ps
import numpy as np
import datetime
import itertools



class keyProcessIndicators():
    def __init__(self,df):
        self._df=df
        self._hash_list=[]
        self._user_list=[]
        
        
    def location_cleaner(self):
        self._df['Location']=self._df['Location'].apply(lambda x: "Not specified" if x=='' else x.lower())
        return self._df


    def get_kpi_hashtags(self):
        hash_kpi_dict_list=[]
        self._df['hash']=np.zeros(len(self._df))
        self._df['hash']=self._df.text.apply(lambda x:["no hashtag mentioned"] if not (re.findall(r"(#\w*)",x)) else re.findall(r"([#]\w*)",x))
        df_explode=self._df
        df_explode=df_explode.explode('hash', axis=1)
        cols=list(self._df.columns)
        cols.remove('hash')
        self._df = df_explode.melt(id_vars=cols,value_name='hash')
        self._df.drop('variable',inplace=True,axis=1)
        top_related_hashtags=self._df['hash'].value_counts()
        top_related_hashtags=top_related_hashtags.to_frame()
        #top_5_related_hashtags=OrderedDict(natsort.natsorted(top_5_related_hashtags.items()))
        return top_related_hashtags

    def get_kpi_users(self):
        
        self._df['user']=np.nan
        self._df['user']=self._df.text.apply(lambda x:["no user mentioned"] if not (re.findall(r"[@]\w*",x)) else re.findall(r"[@]\w*",x))
        df_explode=self._df
        df_explode=df_explode.explode('user', axis=1)
        cols=list(self._df.columns)
        cols.remove('user')
        self._df = df_explode.melt(id_vars=cols,value_name='user')
        self._df.drop('variable',inplace=True,axis=1)
        top_related_users=self._df['user'].value_counts()
        #top_5_related_users=top_5_related_users[0:7].to_frame()
        return top_related_users

