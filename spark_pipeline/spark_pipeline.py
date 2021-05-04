import warnings
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, StringType
import re

class keyProcessIndicators():
    def __init__(self,df):
        self._df=df
        self._hash_df=None
        self._users_df=None

    def location_cleaner(self):
        location_udf=udf(lambda text:"Not specified" if text=='' else text.lower(),StringType())
        self._df=self._df.withColumn("Location", hashtag_udf('text'))
        return self._df

    def get_hashtag(self):
        hashtag_udf=udf(lambda text:re.findall(r"([#]\w+)",text),StringType())
        flatten1 = udf(lambda arr: str(arr).replace("[", "").replace("]", "").replace(" ","").split(","),ArrayType(StringType()))
        strip=udf(lambda s:s.strip(),StringType())
        self._hash_df=self._df.withColumn("hashtags", hashtag_udf('text'))
        self._hash_df=self._hash_df.withColumn("hashtags", F.explode(flatten1(('hashtags')))).groupby("hashtags").count().sort('count', ascending=False)
        return self._hash_df

    def get_users(self):
        users_udf=udf(lambda text:re.findall(r"([@]\w+)",text),StringType())
        flatten1 = udf(lambda arr: str(arr).replace("[", "").replace("]", "").replace(" ","").split(","),ArrayType(StringType()))
        strip=udf(lambda s:s.strip(),StringType())
        self._users_df=self._df.withColumn("mentioned_users", users_udf('text'))
        self._users_df=self._users_df.withColumn("mentioned_users", F.explode(flatten1(('mentioned_users')))).groupby("mentioned_users").count().sort('count', ascending=False)
        return self._users_df
