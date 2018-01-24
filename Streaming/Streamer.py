"""
Created in December of 2017, using a lot of Ben's code from much earlier in the project

This module manages the creation of Twitter streams and the resulting output files.

@author: tom trinter, ben wood (most of it)
"""

import tweepy
from .Topic import Topic
from TwitterFunctions import Twitter_config as config
from time import sleep, gmtime, strftime
import json
import boto3


def getTweepyAuth():
    consumer_key = config.auth['consumer_key']
    consumer_secret = config.auth['consumer_secret']
    access_token = config.auth['access_key']
    access_token_secret = config.auth['access_secret']\

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    return auth


class FileOutListener(tweepy.StreamListener):
    """
    This is the listener, responsible for receiving data
    """

    def __init__(self, outfile, limit=100, filter=[], exclusions=[]):
        # Note this intentionally does not process each tweet object inline, just dumps to file.
        # Streams of tweets can be detected at several per second
        self._outfile = outfile
        self.result_count = 0
        self.filter = filter
        self.exclusions = exclusions
        self.limit = limit

    def on_status(self, status):
        """Defines what happens when a Tweet passes through the filterset."""

        # Twitter returns data in JSON format - we need to decode it first
        data = status._json
        decoded = json.loads(data)
        self.result_count += 1

        # Reject non-english tweets since the models are all fit with English text
        if decoded['user']['lang'] != 'en':
            return

        # Reject if any of the exclusion terms are found
        filter_count = len([x for x in self.filter if x in decoded['text']])
        exclusion_count = len([x for x in self.exclusions if x in decoded['text']])
        if (filter_count > 0 and exclusion_count == 0):
            self.result_count += 1

            # try: print('@%s: %s' % (decoded['user']['screen_name'], decoded['text'].encode('ascii', 'ignore')))
            # except: pass

            # Write the tweet to the output file
            with open(self._outfile,'a') as tf:
                tf.write(data.rstrip('\n'))

    def on_data(self, data):
        """Does the same thing as the on_status. Not exactly sure why they are separate events."""

        # Twitter returns data in JSON format - we need to decode it first
        # data = status._json
        decoded = json.loads(data)

        # Reject if not english.
        if decoded['user']['lang'] != 'en':
            return

        # Reject if any of the exclusion terms are found
        filter_count = len([x for x in self.filter if x in decoded['text']])
        exclusion_count = len([x for x in self.exclusions if x in decoded['text']])
        if (filter_count > 0 and exclusion_count == 0):
            self.result_count += 1

        # Also, we convert UTF-8 to ASCII ignoring all bad characters sent by users
        # print(decoded)
        #     try:
        #    print('@%s: %s' % (decoded['user']['screen_name'], decoded['text'].encode('ascii', 'ignore')))
        #     except:
        #         pass

        # Write the tweet to the output file
            with open(self._outfile, 'a') as tf:
                tf.write(data.rstrip('\n'))

        return

    def on_error(self, status):
        print(status)
        if status == 420:
            return False


class TwitterStream(object):
    """
    Static class to run a Twitter stream

    Args:
        Topic: the topic contains the filter set used to filter the stream
        dest: this is the destination for the data - currently only writing to file
        future could change this to select file, database, or other output destination
    """
    def __init__(self, name: str, topic: Topic, outfile: str):
        self.name = name
        self.topic = topic
        self.outfile = outfile

    def startStream(self, run_time=None, tweet_count=None, async=False):
        if len(self.topic.filters) < 1:
            raise Exception("Topic missing filter terms.")
        else:
            filters = self.topic.filters

    #override tweepy.StreamListener to add logic to on_status
        myStreamListener = FileOutListener(outfile=self.outfile,
                                           filter=self.topic.filters,
                                           exclusions=self.topic.exclusions,
                                           limit=tweet_count)
        myStream = tweepy.Stream(auth=getTweepyAuth(), listener=myStreamListener)
        myStream.filter(languages=["en"], track=filters, async=async)
        if run_time is not None:
            sleep(run_time)
            myStream.disconnect()
            return

        if tweet_count is not None:
            while myStreamListener.result_count < tweet_count:
                pass
            else:
                myStream.disconnect()
        return


def stream_to_S3(topic_id: int, s3bucket: str, s3path: str, tweet_count=1000, save=True):
    """
    Starts a Twitter stream running and saves the tweets into files on S3

    Args:
        topic_id: int, unique identifier for a topic defined in the database
        s3bucket: str, where the resulting tweet file gets saved
        s3path: str, directory and file name for where to put the file in S3 (excluding bucket)

    Returns:
        none
    """

    # Create the topic and stream
    run_topic = Topic(topic_id=topic_id)
    run_topic.readTopic()
    s3 = boto3.resource('s3')

    # 2. Start Twitter stream running
    # print("Starting {} stream for {} tweets.".format(run_topic.name, tweet_count))
    while True:
        outfile = run_topic.name + "_" + strftime("%Y%m%d%H%M%S", gmtime()) + ".json"
        print(outfile)
        run_stream = TwitterStream(name=run_topic.name, topic=run_topic, outfile=outfile)
        run_stream.startStream(tweet_count=tweet_count, async=True)

        # Save file to S3
        if save:
            key = s3path + "/" + outfile
            s3.meta.client.upload_file(outfile, s3bucket, key)
