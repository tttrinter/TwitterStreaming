# from Streaming.Topic import Topic
# from Streaming.Model import Model
# from Streaming.Streamer import TwitterStream, getTweepyAuth
# from Streaming.ProcessTweets import run_topic_models, save_classified_tweets # classify_tweets, tweet_text_from_file,
from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import SnowballStemmer
# from time import sleep
# from Streaming.Streamer import stream_to_S3
from Streaming.ProcessTweets import process_s3_files
import pickle
import pandas as pd


# # Testing with graduation data
# grad_words = [
#     'graduated',
#     'finished school',
#     'graduate',
#     'graduation',
#     'graduating',
#     '#graduation',
#     '#graduate',
#     'just graduated' ,
#     'class of december 2017',
#     'class of 2018'
# ]
#
# grad_words_exclude = [
#     'electoral college',
#     'high school',
#     'trump',
#     'kindergarten',
#     'elementary school',
#     'middle school',
#     'graduation rate',
#     'graduate seminar',
#     'son graduate',
#     'daughter graduate'
# ]
#
# model_path = 'D:/OneDrive/Documents/Ars Quanta/Clients/Current/Thrivent/Twitter/Life Events/Graduation/Models/'
#
# dt_model = Model(name='dt',
#                  type='text',
#                  description='Decision tree model for graduation',
#                  filename='dt.sav',
#                  vectorizer='vectorizer.sav',
#                  model_path=model_path)
#
# logit_model = Model(name='logit',
#                  type='text',
#                  description='Logistic regression model for graduation',
#                  filename='logit.sav',
#                  vectorizer='vectorizer.sav',
#                  model_path=model_path)
#
# new_topic = Topic(name='Graduation',
#                   filters=grad_words,
#                   exclusions=grad_words_exclude,
#                   models_list=[dt_model, logit_model])

# new_topic = Topic(topic_id=9)
# new_topic.readTopic()

# outfile = "TestGradTweets.json"
# new_stream = TwitterStream(name="Graduation", topic=new_topic, outfile=outfile)
# new_stream.startStream(tweet_count=100)


# # Test Processing Tweet File
# infile = 'D:/OneDrive/Documents/Ars Quanta/Clients/Current/Thrivent/Twitter/Life Events/Graduation/Raw Data/graduation_20170331-195520.json'
# infile = 'D:/OneDrive/Documents/Data Insights/Clients/Thrivent/TwitterStreaming/Streaming/TestGradTweets.json'

# # tweet_df = tweet_text_from_file(infile)
# model = 'D:/OneDrive/Documents/Ars Quanta/Clients/Current/Thrivent/Twitter/Life Events/Graduation/Models/dt.sav'
# vectorizer = 'D:/OneDrive/Documents/Ars Quanta/Clients/Current/Thrivent/Twitter/Life Events/Graduation/Models/vectorizer.sav'
#
# Prepare the vectorizer - this is what extracts the vocabulary terms from the tweets
# creating the custom, stemmed count vectorizer
# english_stemmer = SnowballStemmer('english')
#
# class StemmedCountVectorizer(CountVectorizer):
#     def build_analyzer(self):
#         analyzer = super(StemmedCountVectorizer, self).build_analyzer()
#         return lambda doc: ([english_stemmer.stem(w) for w in analyzer(doc)])
#
# # Test running multiple models on a tweet file:
# tweet_df = run_topic_models(infile=infile, topic= new_topic)
# save_classified_tweets(infile=infile, tweet_df=tweet_df, topic=new_topic, threshold=0.5)

# tweet_df.to_excel("classified_grad_tweets.xlsx")
# stream_to_S3(topic_id=9,s3bucket='aqtwitter',s3path='tweets/Life Events/Graduation', tweet_count=100000)

# process_s3_files(topic_id=1 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Graduation/', threshold=0.5)
# process_s3_files(topic_id=2 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Birth/', threshold=0.5)

