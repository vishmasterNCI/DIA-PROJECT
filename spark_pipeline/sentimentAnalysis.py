#import counter
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.decomposition import NMF, LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer

from textblob import TextBlob
import pandas_explode
pandas_explode.patch()

from nltk.corpus import sentiwordnet as swn, wordnet
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover





class sentimentAnalysis():
    def __init__(self,df):
        self._df=df


    def textblob_polarity_detection(self,text):
        return TextBlob(text).sentiment.polarity

    def vader_polarity_detection(self,text):
        vs = self._vader.polarity_scores(text)
        return vs['compound']

    def sentiment_score(self,vs,ts):
        return (vs+ts)/2

    def get_sentiment_label(self,Sentiment_Score):
            if  Sentiment_Score ==0.0:
                return 'Neutral'
            elif Sentiment_Score > 0.0:
                return 'Positive'
            elif Sentiment_Score < 0.0:
                return 'Negative'

    def text_classification(self):
        def textblob_polarity_detection(text):
            return TextBlob(text).sentiment.polarity

        def vader_polarity_detection(text):
            vs = SentimentIntensityAnalyzer().polarity_scores(text)
            return vs['compound']
        def get_sentiment_label(Sentiment_Score):
            if  Sentiment_Score ==0.0:
                return 'Neutral'
            elif Sentiment_Score > 0.0:
                return 'Positive'
            elif Sentiment_Score < 0.0:
                return 'Negative'
        def sentiment_score(vs,ts):
            return (vs+ts)/2
        # polarity detection
        textblob_polarity_detection_udf = udf(textblob_polarity_detection, FloatType())
        vader_polarity_detection_udf = udf(vader_polarity_detection, FloatType())
        sentiment_score_udf=udf(sentiment_score, FloatType())
        get_sentiment_label_udf=udf(get_sentiment_label,StringType())
        self._df = self._df.withColumn("textblob_polarity", textblob_polarity_detection_udf("words"))
        self._df = self._df.withColumn("vader_polarity", vader_polarity_detection_udf("words"))
        self._df = self._df.withColumn("sentiment_score", sentiment_score_udf("vader_polarity","textblob_polarity"))
        self._df = self._df.withColumn("Sentiment", get_sentiment_label_udf("sentiment_score"))
        return self._df
