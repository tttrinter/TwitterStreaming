""" This module manages the creation of Twitter streams and the
resulting output files.
"""
from tweepy import Stream,StreamListener,OAuthHandler
from .Topic import Topic
from time import sleep, gmtime, strftime
import json
import boto3
import logging
import os
import io
from datetime import datetime
from dateutil import relativedelta
from Manager import notify

def getTweepyAuth(auth_name):
    with open('Streaming/twitter_user_config.json') as cfg:
        config = json.load(cfg)
    auth_configs = config['auth'][auth_name]
    logging.info('Using twitter user: {0}'.format(auth_name))
    try:
        auth = OAuthHandler(auth_configs['consumer_key'], auth_configs['consumer_secret'])
        auth.set_access_token(auth_configs['access_key'], auth_configs['access_secret'])
        return auth
    except Exception as e:
        logging.exception(e)

# This is the listener, responsible for receiving data
class FileOutListener(StreamListener):
    def __init__(self, filter=[], exclusions=[]):
        # Note this intentionally does not process each tweet object inline, just dumps to file.
        # Streams of tweets can be detected at several per second
        self.result_count = 0
        self.filter = filter
        self.exclusions = exclusions
        self.output = io.StringIO()

    def get_tweets(self):
        return self.output.getvalue()

    def on_error(self, status_code):
        if status_code == 420:
            logging.exception('Enhance Your Calm')
            notify.notify('Enhance Your Calm')
            sleep(300)
            #returning False in on_data disconnects the stream
        elif status_code == 429:
            logging.exception('Rate Limited')
            notify.notify('Rate Limited')
            return False
        else:
            logging.exception(status_code)
            notify.notify('Stream Failed in module FileOutListener with status_code({0})'.format(status_code))
            return False

    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        # data = status._json
        self.last_update_time = datetime.now()
        try:
            if data is not None:
                decoded = json.loads(data)
                tweet_text = decoded['text'].lower()
            else:
                return

            if 'user' not in decoded:
                return
            elif decoded['user']['lang'] != 'en':
                return

        except Exception as e:
            logging.exception(e)
            return

        filter_count = len([x for x in self.filter if x in tweet_text])
        # Only save if there are none of the exclusion terms
        if len(self.exclusions) > 0:
            exclusion_count = len([x for x in self.exclusions if x in tweet_text])
        else:
            exclusion_count = 0
        if (filter_count > 0 and exclusion_count == 0):
            self.result_count += 1
            self.output.write(data.rstrip('\n'))
        return

    def on_status(self, status):
        # Twitter returns data in JSON format - we need to decode it first
        try:
            data = status._json
            return self.on_data(data)
        except Exception as e:
            logging.exception(e)
            return

class TwitterStream(object):
    """
    Static class to run a Twitter stream

    Args:
        Topic: the topic contains the filter set used to filter the stream
        dest: this is the destination for the data - currently only writing to file
        future could change this to select file, database, or other output destination
    """
    def __init__(self, name: str, topic: Topic, auth_name: str):
        self.name = name
        self.topic = topic
        self.tweet_count = 10
        self.auth_name = auth_name
        self.myStreamListener = FileOutListener()

    def startStream(self, run_time=None, tweet_count=None, async=False):

        # Set up time variables - used to cutoff the stream when run_time is not none
        # Also used to keep track of time since last update - to cut off dead streams
        start_time = datetime.now()
        self.tweet_count = tweet_count
        time_since_udate = 0
        if len(self.topic.filters) < 1:
            raise Exception("Topic missing filter terms.")
        else:
            filters = self.topic.filters

        auth = getTweepyAuth(auth_name=self.auth_name)
        self.myStreamListener = FileOutListener(filter=self.topic.filters,
                                                exclusions=self.topic.exclusions
                                                )
        myStream = Stream(auth=auth, listener=self.myStreamListener)
        try:
            myStream.filter(languages=["en"], track=filters, async=async)
        except AttributeError:
            pass
        except Exception as e:
            logging.exception(e)
            # Doobie Break - if we get the Twitter chill-out error, stop trying for 5 minutes
            if e == '420':
                msg = "doobie break"
                logging.info(msg)
                print(msg)
                sleep(300)

            # Bail out and save the file - change the tweet_count goal to equal the current value
            tweet_count = self.myStreamListener.result_count
            notify.notify('Stream failed in module:  TwitterStream')
            raise

    # Check Exit Criteria
        # Run Time
        if run_time is not None:
            cur_run_time = (datetime.now()-start_time).total_seconds()
            if cur_run_time >= run_time:
                myStream.disconnect()
            return

        # Tweet Count
        if tweet_count is not None:
            while self.myStreamListener.result_count < tweet_count:
                pass
            else:
                self.myStream.disconnect()
        return

def connect_s3():
    boto3.setup_default_session(profile_name='di')
    s3 = boto3.resource('s3')
    return s3

def run_topic_continuous(topic_id: int, s3_bucket: str, s3_path: str, tweet_count=1000, auth_name='tom'):
    """
    1. Create the topic and stream
    2. Collects tweet_count tweets and save to a local file
    3. Save tweet file to S3 when finished
    4. Delete the local file
    """
    start = datetime.now()
    notify_us = True
    # 1. Create the topic and stream
    run_topic = Topic(topic_id=topic_id)
    run_topic.readTopic()
    if run_topic.name is None:
        print("Topic not found")
        quit()

    # 2. Start Twitter stream running
    print("Starting {0} stream for {1} tweets using auth({2}).".format(run_topic.name, tweet_count,auth_name))
    iteration = 0

    while True:
        notified = True
        if datetime.now().hour % 8 != 0:
            notified = False
        if iteration == 0 and notify_us:
            notify.notify('Starting stream {}'.format(run_topic.name))
        iteration += 1
        run_stream = TwitterStream(name=run_topic.name, topic=run_topic, auth_name=auth_name)

        try:
            run_stream.startStream(tweet_count=tweet_count, async=True)
        except AttributeError:
            pass

        # 3. Save tweets to S3
        s3 = connect_s3()
        outfilename = run_topic.name + "_" + strftime("%Y%m%d%H%M%S", gmtime()) + ".json"
        key = s3_path + outfilename
        object = s3.Object('di-thrivent', key)
        tweets = run_stream.myStreamListener.output.getvalue()
        object.put(Body=tweets)
        #run_stream.myStreamListener.output.close()

        duration = relativedelta.relativedelta(datetime.now(), start)
        msg = (strftime("%Y-%m-%d %H:%M:%S",gmtime()) +
              ": total_files({0}): duration({1} hours): Saving {2} to {3}."
              .format(iteration,duration.hours,outfilename,s3_path)
        )
        print(msg)
        logging.info(msg)

        if datetime.now().hour % 8 == 0 and notify_us and not notified:
            notify.notify('Still streaming {0}'.format(run_topic.name))


