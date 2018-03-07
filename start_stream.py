""" This script is a generic version for running streams.
Args: these have to be separated by spaces. Strings are in double quotes.
    1: topic Id - must be one of the defined topics in our Postgres database
    2: s3_bucket - the name of the bucket. This assumes the path
    3: s3_path - path to the right folder for this stream
    4: tweet_count - number of tweets to include in each file before cutting and re-starting - usually 1000
    5: auth_name - one of the accounts we have set up: tom, john, ben, ben2, jake, allison
"""

import logging
import sys
from Manager import notify
from Streaming.Streamer import run_topic_continuous

# Gather inputs from command line
topic_id = sys.argv[1]
s3_bucket = sys.argv[2]
s3_path = sys.argv[3]
tweet_count = sys.argv[4]
auth_name = sys.argv[5]
path_parts = s3_path.split("/")
topic_name = path_parts[len(path_parts)-2].lower()
log_name = "{}_stream.log".format(topic_name)

# Check inputs
print("topic_id: {}".format(topic_id))
print("s3_bucket: {}".format(s3_bucket))
print("s3_path: {}".format(s3_path))
print("tweet_count: {}".format(tweet_count))
print("auth_name: {}".format(auth_name))
print("log_name: {}".format(log_name))

# # Set up log
logging.basicConfig(filename=log_name,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)
try:
    run_topic_continuous(topic_id=topic_id,
                     s3_bucket=s3_bucket,
                     s3_path=s3_path,
                     tweet_count=tweet_count,
                     auth_name = auth_name)
except:
    notify.notify('{} stream failed in module start_stream.py'.format(topic_name))

