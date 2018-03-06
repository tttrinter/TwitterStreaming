"""
Created in December of 2017

This script is intended for use as a scheduled task or run from command line
to instantiate a Twitter stream for a topic as defined in the database.
The stream will collect tweets that contain the filter terms and save them as raw JSON
data to S3.

@author: tom trinter
"""


import boto3
import os
from time import gmtime, strftime
from Streaming.Topic import Topic
from Streaming.Streamer import TwitterStream
import sys

def run_topic_continuous(topic_id: int, s3_bucket: str, s3_path: str, tweet_count=1000):
    """
    1. Create the topic and stream
    2. Collects tweet_count tweets and save to a local file
    3. Save tweet file to S3 when finished
    4. Delete the local file
    """

    # 1. Create the topic and stream
    run_topic = Topic(topic_id=topic_id)
    run_topic.readTopic()
    if run_topic.name is None:
        print("Topic not found")
        quit()

    # 2. Start Twitter stream running
    print("Starting {} stream for {} tweets.".format(run_topic.name, tweet_count))
    iteration = 0
    boto3.setup_default_session(profile_name='di')
    s3 = boto3.resource('s3')
    save_bucket = s3_bucket

    while True:
        iteration += 1
        outfile = run_topic.name + "_" + strftime("%Y%m%d%H%M%S", gmtime()) + ".json"
        run_stream = TwitterStream(name=run_topic.name, topic=run_topic, outfile=outfile)
        run_stream.startStream(tweet_count=tweet_count, async=True)

        #TODO:  Do exclusions outside of stream

        # 3. Save file to S3
        print("Saving {} to {}.".format(outfile, s3_path))
        key = s3_path + outfile
        try:
            s3.meta.client.upload_file(outfile, save_bucket, key)
        except Exception as e:
            print(e)

        # 4. Delete the temporary file
        os.remove(outfile)

# this is here to enable running from the command line. NOT CURRENTLY WORKING
# TODO: fix this!
if __name__ == "__main__":
    print("in Main")
    topic_id = int(sys.argv[1])
    s3_bucket = sys.argv[2]
    s3_path = sys.argv[3]
    tweet_count = sys.argv[4]
    print('Running: Topic id: {}, s3 bucket: {}, s3 path: {}, tweet count: {}'.format(topic_id, s3_bucket, s3_path, tweet_count))

    # run_topic_continuous(topic_id, s3_bucket,s3_path,tweet_count)
