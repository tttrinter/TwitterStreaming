import logging
from Manager import notify
from Streaming.Streamer import run_topic_continuous

# Set up log
logging.basicConfig(filename='birth_stream.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)
try:
    run_topic_continuous(topic_id=6,
                     s3_bucket='di-thrivent',
                     s3_path='twitter/Life Events/Moving/',
                     tweet_count=1000,
                     auth_name = 'john')
except:
    notify.notify('Birth stream failed in module birth_stream.py')

