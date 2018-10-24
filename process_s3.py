import logging
from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import SnowballStemmer
from Streaming import process_s3_files
from scipy.sparse import lil_matrix
from TwitterRDS.RDSQueries import get_topics_toprocess
import numpy as np
import sys

# Set up log
logging.basicConfig(filename='process_s3.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

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

# Gather inputs from command line
# Optional command line topic to run
if len(sys.argv) == 2:
    single_topic = sys.argv[1]
else:
  single_topic = None

# Get topics to run
topics = get_topics_toprocess()
if single_topic:
    topics = topics.loc[topics.tp_name == single_topic]

for index, row in topics.iterrows():
    tp_id = row['tp_id']
    topic_name = row['tp_name']
    s3folder = 'twitter/Life Events/{}/'.format(topic_name)
    thresh = row['tp_threshold']

    msg = "Processing {} files.".format(topic_name)
    logging.info(msg)
    print(msg)
    process_s3_files(topic_id=tp_id, s3bucket='di-thrivent', s3prefix=s3folder, threshold=thresh)

# msg = "Processing graduation files."
# logging.info(msg)
# print(msg)
# process_s3_files(topic_id=1 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Graduation/', threshold=0.7)
#
# msg = "Processing birth files."
# logging.info(msg)
# print(msg)
# process_s3_files(topic_id=2 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Birth/', threshold=0.7)
#
# msg = "Processing moving files."
# logging.info(msg)
# print(msg)
# process_s3_files(topic_id=6, s3bucket='di-thrivent', s3prefix='twitter/Life Events/Moving/', threshold=0.5)
#
# msg = "Processing job files."
# logging.info(msg)
# print(msg)
# process_s3_files(topic_id=5, s3bucket='di-thrivent', s3prefix='twitter/Life Events/Job/', threshold=0.7)
#
# msg = "Finished processing S3 Files."
# logging.info(msg)
# print(msg)

