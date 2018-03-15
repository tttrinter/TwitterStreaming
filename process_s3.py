import logging
from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import SnowballStemmer
from Streaming import process_s3_files

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
logging.info("Processing graduation files.")
process_s3_files(topic_id=1 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Graduation/', threshold=0.7)

logging.info("Processing birth files.")
process_s3_files(topic_id=2 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Birth/', threshold=0.7)

