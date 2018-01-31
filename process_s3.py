from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import SnowballStemmer
from Streaming import process_s3_files

# Set up Snowball Stemmer - otherwise it won't be found when needed
english_stemmer = SnowballStemmer('english')

class StemmedCountVectorizer(CountVectorizer):
    def build_analyzer(self):
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        return lambda doc: ([english_stemmer.stem(w) for w in analyzer(doc)])

process_s3_files(topic_id=1 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Graduation/', threshold=0.5)
process_s3_files(topic_id=2 ,s3bucket='di-thrivent', s3prefix='twitter/Life Events/Birth/', threshold=0.5)

