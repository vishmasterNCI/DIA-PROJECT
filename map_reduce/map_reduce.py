import sys
from pyspark import SparkContext, SparkConf
import pandas as pd
import re
from nltk import sent_tokenize, word_tokenize, pos_tag
from nltk.corpus import stopwords
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#from gensim.parsing.preprocessing import STOPWORDS
import nltk
nltk.download('stopwords')

from nltk.stem import WordNetLemmatizer, SnowballStemmer,PorterStemmer
stemmer=SnowballStemmer(language='english')
from pyspark.sql import functions as F





class map_reduce():
    def __init__(self,sc):
        self._sc=sc


    def hashtag_count(self,sentences):
        words=self._sc.parallelize(sentences).flatMap(lambda line: re.findall(r"(#\w+)",line))#.split(" "))
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b).sortBy(lambda a:-a[1])
        return wordCounts

    def mentioned_user_count(self,sentences):
        words=self._sc.parallelize(sentences).flatMap(lambda line: re.findall(r"(@\w+)",line))
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b).sortBy(lambda a:-a[1])
        return wordCounts

    def RemoveOverallNoise(self,text):
        text=text.map(lambda line: re.sub(r'[@]\w*'," ",line))
        text=text.map(lambda line: re.sub(r'[#]\w*'," ",line))
        text=text.map(lambda line: re.sub(r'\s*\d+\s'," ",line))
        text=text.map(lambda line: re.sub(r'\W+'," ",line))
        text=text.map(lambda line: re.sub(r'^[^a-zA-Z]'," ",line))
        text=text.map(lambda line: re.sub(r'\b(\w+)( \1\b)+'," ",line))
        text=text.map(lambda line: re.sub(r'\s[a-z]\s|\s[A-Z]\s'," ",line))
        text=text.map(lambda line: re.sub(r'[@]\w*'," ",line))
        text=text.map(lambda line: re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",line))
        text=text.map(lambda line: re.sub(r"won\'t", "will not", line))
        text=text.map(lambda line: re.sub(r"can\'t", "can not", line))
        text=text.map(lambda line: re.sub(r"n\'t", " not", line))
        text=text.map(lambda line: re.sub(r"\'re", " are", line))
        text=text.map(lambda line: re.sub(r"\'s", " is", line))
        text=text.map(lambda line: re.sub(r"\'d", " would", line))
        text=text.map(lambda line: re.sub(r"\'ll", " will", line))
        text=text.map(lambda line: re.sub(r"\'t", " not", line))
        text=text.map(lambda line: re.sub(r"\'ve", " have", line))
        text=text.map(lambda line: re.sub(r"\'m", " am", line))
        return text

    def sentiment_score(self,text):
        textblob=text.map(lambda line:TextBlob(line).sentiment.polarity)
        vader=text.map(lambda line:SentimentIntensityAnalyzer().polarity_scores(line)['compound'])
        zipped=textblob.zip(vader)
        zipped=zipped.map(lambda x:(x[0]+x[1])/2)
        return zipped



    def sentiment_label(self,score):
        def get_sentiment_label(Sentiment_Score):
            if  Sentiment_Score ==0.0:
                return 'Neutral'
            elif Sentiment_Score > 0.0:
                return 'Positive'
            elif Sentiment_Score < 0.0:
                return 'Negative'


        sentiment=score.map(lambda val:get_sentiment_label(val))
        return sentiment

    def TokenizeText(self,text):
        text=self._sc.parallelize(text).map(lambda line: line.split())
        return text #[w for s in sent_tokenize(text) for w in word_tokenize(s)]




    def clean(self,text):
        stop_words = set(stopwords.words("english"))
        #stop_words.union(STOPWORDS)
        #stop_words.union(nlp.Defaults.stop_words)
        stop_words.remove('not')
        words_list=self._sc.parallelize(text).map(lambda line:line.split())#
        words_list=words_list.map(lambda lines: list(set(lines).difference(stop_words)))
        words_list=words_list.filter(lambda lines: all(stemmer.stem(words) for words in lines ))
        words_list=words_list.map(lambda lines:" ".join(lines))
        return words_list

    def calculate_sentiment_total(self,text):
        words=text.map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b).sortBy(lambda a:-a[1])
        return words

    def calculate_top_words_sentiment(self,data,d):
        words = data.map(lambda word: word.split())#
        zipped=words.zip(d)
        zippedCounts=zipped.flatMap(lambda x: [(( i, x[1]),1) for i in x[0]]).reduceByKey(lambda a,b:a+b).sortBy(lambda a:-a[1])
        return zippedCounts
