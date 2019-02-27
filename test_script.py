from TwitterFunctions import find_long_state, get_name
from TwitterRDS.RDSQueries import upsert_usernames
# from subprocess import Popen
# import os
# import multiprocessing as mp
from Streaming.Streamer import run_topic_continuous
from Streaming.ProcessTweets import process_s3_files
import pickle
import pandas as pd
from Streaming.Topic import Topic
from Streaming.ProcessTweets import run_topic_models
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import SnowballStemmer
from scipy.sparse import lil_matrix
from Streaming.ProcessTweets import get_upstream_tweets, process_upstream_tweets

# found_state = find_long_state("born and raised in Las Vegas, Nevada")
# print(found_state)

# names = get_name("Tommy Trinter")
# upsert_usernames(713437986118090753, names)
# print(names)

# # Testing starting a new process
# inputs = {'topic_id': 6,
#           's3_bucket': 'di-thrivent',
#           's3_path': 'twitter/Life Events/Moving/' ,
#            'tweet_count': 100}
#
# call_line = 'python start_stream.py {} "{}" "{}" {}'.format(
#     inputs['topic_id'],
#     inputs['s3_bucket'],
#     inputs['s3_path'],
#     inputs['tweet_count']
# )
#
# my_env = os.environ.copy()
# # DETACHED_PROCESS = 0x00000008
# try:
#     # Popen(call_line, shell=True, creationflags=DETACHED_PROCESS, env=my_env)
#     Popen(call_line, shell=True, env=my_env)
#
# except Exception as e:
#     print(e)

# Test Processing Upstream Tweets
# data_path = 'C:\\Users\\tttri\\OneDrive\\Documents\\Data Insights\\Clients\\Thrivent\\Twitter\\Life Events\\Condolences\\'
# data_file = 'tweets.pkl'

# new_topic = Topic(name='Death')

# new_topic = Topic(topic_id=5)
# new_topic.readTopic()

# Set up vectorizers
# Set up Snowball Stemmer - otherwise it won't be found when needed
english_stemmer = SnowballStemmer('english')

class StemmedCountVectorizer(CountVectorizer):
    def build_analyzer(self):
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        return lambda doc: ([english_stemmer.stem(w) for w in analyzer(doc)])

# From the DS Lore Link
class MeanEmbeddingVectorizer(object):
    def __init__(self, word2vec):
        self.word2vec = word2vec
        # if a text is empty we should return a vector of zeros
        # with the same dimensionality as all the other vectors
        self.dim = len(list(word2vec.values())[0])

    def fit(self, X, y):
        return self

    def transform1(self, X):
        return np.array(np.mean([self.word2vec[w] if w in self.word2vec else np.zeros(self.dim)
                                 for w in X],
                                axis=0))

    def transform(self, sentences):
        return lil_matrix([self.transform1(X) for X in sentences])


# json_file = 'C:\\Users\\tttri\\OneDrive\\Documents\\Data Insights\\Clients\\Thrivent\\Twitter\\Life Events\\Congratulations\\Congratulations_20190206162605.json'
# congrat_topics=[1,2,3,4,5,6]
# process_upstream_tweets(tweet_file = json_file, topic_list=congrat_topics, con=None)
process_s3_files(topic_id=1,
                 s3bucket= 'di-thrivent',
                 s3prefix= 'twitter/Life Events/Graduation/',
                 con=None)
# classed_tweets = run_topic_models(infile=data_path+data_file, topic=new_topic)

#           's3_path': 'twitter/Life Events/Moving/' ,