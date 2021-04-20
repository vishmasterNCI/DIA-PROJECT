#import counter
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.decomposition import NMF, LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from textblob import TextBlob
import pandas_explode
pandas_explode.patch()
import seaborn as sns
from nltk.corpus import sentiwordnet as swn, wordnet





class sentimentAnalysis():
    def __init__(self,df):
        self._df=df
        self._vader = SentimentIntensityAnalyzer()

    def vader_sentiment_score(self,sentence):
        sentiment_dict = self._vader.polarity_scores(sentence)
        #print (sentiment_dict)
        return sentiment_dict['compound']
    def textblob_sentiment_score(self,sentence):
        # Create a SentimentIntensityAnalyzer object.
        textblob_analyzer = TextBlob(sentence)
        textblob_score =  round(textblob_analyzer.sentiment.polarity,2)
        #print (textblob_analyzer.sentiment.polarity)
        return textblob_score

    def get_sentiment_score(self,tweets):
        tweets['Vader_Sentiment_Score']=tweets.text.apply(lambda x:self.vader_sentiment_score(x))
        tweets['Textblob_Sentiment_Score']=tweets.text.apply(lambda x:self.textblob_sentiment_score(x))
        tweets['Sentiment_Score']=(tweets['Vader_Sentiment_Score']+tweets['Textblob_Sentiment_Score'])/2#tweets.text.apply(lambda x:(vader_sentiment_score(x)+textblob_sentiment_score(x))/2)
        return tweets

    def get_sentiment_label(self,Sentiment_Score):
        if  Sentiment_Score ==0.0:
            return 'Neutral'
        elif Sentiment_Score > 0.0:
            return 'Positive'
        elif Sentiment_Score < 0.0:
            return 'Negative'

    def get_sentiment(self,tweets):
        tweets['Sentiment']=tweets['Sentiment_Score'].apply(lambda x:self.get_sentiment_label(x))
        return tweets

    def main(self):
        self._df=self.get_sentiment_score(self._df)
        self._df=self.get_sentiment(self._df)
