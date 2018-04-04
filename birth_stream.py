import logging
from Manager import notify
from Streaming.Streamer import run_topic_continuous

# Set up log
logging.basicConfig(filename='birth_stream.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)
try:
    logging.info("Starting via birth_stream.py")
    run_topic_continuous(topic_id=2,
                     s3_bucket='di-thrivent',
                     s3_path='twitter/Life Events/Birth/',
                     tweet_count=2,
                     auth_name = 'jake')
except Exception as e:
    logging.error(e)
    notify.notify('Birth stream failed in module birth_stream.py')

