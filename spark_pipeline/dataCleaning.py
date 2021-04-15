import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import pandas as pd
import numpy as np
import natsort
from collections import OrderedDict
import pandas_explode
import nltk
from nltk import sent_tokenize, word_tokenize, pos_tag
#import pysentiment as ps
import numpy as np
import datetime
import itertools
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
nltk.download('stopwords')
#!python -m spacy download en
from nltk.corpus import stopwords
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from wordcloud import WordCloud
from nltk.tokenize import sent_tokenize, word_tokenize
import matplotlib.pyplot as plt
import spacy
from nltk.stem import WordNetLemmatizer, SnowballStemmer,PorterStemmer
from nltk.corpus import sentiwordnet as swn, wordnet
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('omw')
nltk.download('vader_lexicon')
#!python -m spacy download en
from nltk.corpus import stopwords
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from wordcloud import WordCloud
from nltk.tokenize import sent_tokenize, word_tokenize
import matplotlib.pyplot as plt
import spacy
import gensim
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer,PorterStemmer
from nltk.stem.porter import *
import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover

class dataCleaning():
    def __init__(self,df):
        self._df=df
        self._lemmatizer = WordNetLemmatizer()
        self._tag_dict = {"J": wordnet.ADJ,
                        "N": wordnet.NOUN,
                        "V": wordnet.VERB,
                        "R": wordnet.ADV}
        self._stop_words = set(stopwords.words("english"))
        self._stop_words.union(STOPWORDS)
        #stop_words.union(nlp.Defaults.stop_words)
        self._stop_words.remove('not')
        
    def noise_removal(self,lines):
        words = lines.withColumn('text', F.regexp_replace('text', r'[@]\w*', ''))
        words = words.withColumn('text', F.regexp_replace('text', '[#]\w*', ''))
        words = words.withColumn('text', F.regexp_replace('text', '\s*\d+\s*', ''))
        words = words.withColumn('text', F.regexp_replace('text', '\W+', ''))
        words = lines.withColumn('text', F.regexp_replace('text', r'^[^a-zA-Z]', ''))
        words = words.withColumn('text', F.regexp_replace('text', '@\w+', ''))
        words = words.withColumn('text', F.regexp_replace('text', '\b(\w+)( \1\b)+', ''))
        words = words.withColumn('text', F.regexp_replace('text', '\s[a-z]\s|\s[A-Z]\s', ''))
        words = words.withColumn('text', F.regexp_replace('text', '(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', ''))
        return words
    
    
    def get_wordnet_pos(self,word):
        """Map POS tag to first character lemmatize() accepts"""
        tag = nltk.pos_tag([word])[0][1][0].upper()
        return tag_dict.get(tag,wordnet.NOUN)


    def TokenizeText(self,text):
        return [w for s in sent_tokenize(text) for w in word_tokenize(s)]

    def RemoveStopwords(self,words_list):
        tokens = [w for w in words_list if w not in stop_words]
        return tokens
    
    def Stemming(text):
        tokens = [stemmer.stem(w) for w in text]
        return ' '.join(tokens)
    
    def lemmatize_text(text):
        return lemmatizer.lemmatize(text, get_wordnet_pos(text))


    
    def get_wordnet_pos(word):
        """Map POS tag to first character lemmatize() accepts"""
        tag = nltk.pos_tag([word])[0][1][0].upper()
        return tag_dict.get(tag,wordnet.NOUN)
        

    def Stemming(self,text):
        tokens = [stemmer.stem(w) for w in self.TokenizeText(text) if w not in stop_words]
        return ' '.join(tokens)

    def preprocessing(self):
        # polarity detection
        strip=udf(lambda s:s.strip(),StringType())
        self._df=self.noise_removal(self._df)
        self._df=self._df.withColumn("words",strip("text"))
        tokenizer = Tokenizer(inputCol="text", outputCol="words_tokens")
        self._df = tokenizer.transform(self._df)
        
        stopwords_remove = StopWordsRemover(inputCol="words_tokens", outputCol="filtered")
        self._df = stopwords_remove.transform(self._df)


        #RemoveStopwords_udf=udf(self.RemoveStopwords,ArrayType(StringType()))    
        Stemming_udf=udf(self.Stemming,StringType())
        
        stemmer = SnowballStemmer(language='english')
        stemmer_udf = udf(lambda tokens: [stemmer.stem(token) for token in tokens], ArrayType(StringType()))
        #Lemmatize_udf=udf(self.lemmatize_stringtext,StringType())
        join_udf=udf(lambda x :" ".join(x),StringType())
        #tokenized=tokenized.withColumn("words",RemoveStopwords_udf("words"))
        self._df=self._df.withColumn("words",stemmer_udf("filtered"))
        self._df=self._df.withColumn("words",join_udf("words"))
        #tokenized=tokenized.withColumn("words",Lemmatize_udf("words"))


        #self._df = self._df.withColumn("text", stop_words_udf("text"))
        #self._df = self._df.withColumn("text", stemmer_udf("text"))
        
        return self._df