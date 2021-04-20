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
#from wordcloud import WordCloud
from nltk.tokenize import sent_tokenize, word_tokenize
#import matplotlib.pyplot as plt
#import spacy
from nltk.stem import WordNetLemmatizer, SnowballStemmer,PorterStemmer
from nltk.corpus import sentiwordnet as swn, wordnet
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

from nltk.stem import WordNetLemmatizer, SnowballStemmer,PorterStemmer
from nltk.stem.porter import *
import numpy as np


class dataCleaning():
    def __init__(self,df):
        self._df=df
        self._lemmatizer = WordNetLemmatizer()
        self._tag_dict = {"J": wordnet.ADJ,
                        "N": wordnet.NOUN,
                        "V": wordnet.VERB,
                        "R": wordnet.ADV}
        self._stop_words = set(stopwords.words("english"))
        #self._stop_words.union(STOPWORDS)
        #stop_words.union(nlp.Defaults.stop_words)
        self._stop_words.remove('not')

    def stemSentence(self,sentence):
        token_words=word_tokenize(sentence)
        stem_sentence=[]
        for word in token_words:
            stem_sentence.append(ps.stem(word))
            stem_sentence.append(" ")
        return "".join(stem_sentence)

    def get_wordnet_pos(self,word):
        """Map POS tag to first character lemmatize() accepts"""
        tag = nltk.pos_tag([word])[0][1][0].upper()
        return self._tag_dict.get(tag,wordnet.NOUN)


    def lemmatize_text(self,sentence):
        return " ".join([self._lemmatizer.lemmatize(w, self.get_wordnet_pos(w)) for w in nltk.word_tokenize(sentence)])

    def TokenizeText(self,text):
                return [w for s in sent_tokenize(text) for w in word_tokenize(s)]

    def RemoveStopwords(self,text):
                tokens = [w for w in self.TokenizeText(text) if w not in self._stop_words]
                return ' '.join(tokens)
    def RemoveOverallNoise(self,text):
        text = re.sub(r'[@]\w*'," ",                                   text ) ##Authors
        text = re.sub(r'[#]\w*'," " ,                            text ) ##hash tags
        #text = re.sub('\s*\d+\s*',' ',                                 text ) ##Numbers
        text = re.sub('\W+',' ',text ) ##special char
        text = re.sub(r'^[^a-zA-Z]',r' ',text ) ##non words
        text = re.sub(r'\b(\w+)( \1\b)+', r'\1',text ) ##repetitive words
        text = re.sub(r'\s[a-z]\s|\s[A-Z]\s', " ",text ) ##Single word

        #' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",x).split())
        return text

    def preprocess_tweet(self):

        self._df.Location=self._df.Location.apply(lambda x: self.RemoveOverallNoise(x.lower()))
        self._df.text = self._df.text.apply(lambda x: self.RemoveOverallNoise(x.lower()))
        self._df.text = self._df.text.apply(lambda x: self.RemoveStopwords(x.lower()))
        self._df.text = self._df.text.apply(lambda x: self.lemmatize_text(x))
        # file = file.drop_duplicates(subset=['text'], keep="first")
        #df.to_csv('Cleaned_Tweet.csv',index = False,encoding='utf-8')

        return self._df
