import logging
from Streaming.Streamer import run_topic_continuous

# Set up log
logging.basicConfig(filename='grad_stream.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

run_topic_continuous(topic_id=1,
                     s3_bucket='di-thrivent',
                     s3_path='twitter/Life Events/Graduation/',
                     tweet_count=1000)

