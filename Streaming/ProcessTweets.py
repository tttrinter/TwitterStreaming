"""
Created in December of 2017

This module contains the methods and functions needed to process the tweet files saved to S3
from the Streamer process. These files should be Topic specific, so they are matched with the
right filters and models for processing.

@author: tom trinter
"""

from Entweeties import Tweet, TwitterUser
from TwitterRDS.RDSEntweeties import merge_tweet
from  TwitterRDS import RDSQueries as q
from TwitterFunctions.TwitterProcessing import clean_text_col
from .Topic import Topic

import json
import pandas as pd
import pickle
import boto3
import logging

from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import SnowballStemmer
# import gensim

# import numpy as np


def get_processed_key(file_key: str, dest):
    """ Takes an S3 file key and splits it by "/" for use in building
        save and delete steps as the files are processed.

        Args:
            file_key: string, this is the full file name from S3 for a saved file
            dest: where the file is going - "Processed", or "Failed"

        Returns:
            List of terms that make up the S3 path, dropping the bucket name.
        """

    path_pieces = file_key.split('/')
    path_pieces.insert(len(path_pieces) - 2, dest)
    processed_key = ''
    for piece in path_pieces:
        processed_key += "/" + piece
    #drop the leading /
    return processed_key[1:]


def tweet_text_from_file(infile: str, startline=0, endline=9000000, filters=[], exclusions=[]):
# def tweet_text_from_file(infile: str, startline=0, endline=9000000, exclusions=[]):
    """ Reads in a json tweet file and extracts the tweet ids and text.

        Args:
            infile: str, directory and file name for the .json tweet file
            startline: int, optional start line in the file. Process will skip forward to this line. Default = 0.
            endline: int, optional end line in the file. Process will stop when it gets to this line. Default 9MM.
            filters: list of filter terms - from the topic; since Tweepy isn't filtering everything, reapply here
            exclusions: list of exclusion terms - also from the topic

        Returns:
            pandas DataFrame with two columns: tweet_id, text
        """
    tweets = []
    tweet_keys = ['id', 'text']
    i = 0
    with open(infile, encoding='UTF-8') as f:
        for line in f:
            i += 1
            if i > startline:
                tweet_dict = json.loads(line)
                try:
                    tweet_text = tweet_dict['text']
                    # Reject if the filter terms aren't found
                    filter_count = len([x for x in filters if x in tweet_text])
                    exclusion_count = len([x for x in exclusions if x in tweet_text])
                    if exclusion_count == 0 and filter_count > 0:
                    # if exclusion_count == 0:
                        tweets.append({k: tweet_dict.get(k, None) for k in tweet_keys})
                except Exception as e:
                    logging.exception(e)
                    break
            if i == endline:
                break
    f.close()

    if len(tweets) > 0:
        tweet_df = pd.DataFrame.from_dict(tweets)
        tweet_df.columns = ['tweet_id', 'text']
        tweet_df.drop_duplicates(subset='text', keep='first',inplace=True)

    # Clean the text column, removing hyper links, punctuation, and non ascii chars.
        tweet_df = clean_text_col(tweet_df, 'text', 'clean_text', False )
        tweet_df = clean_text_col(tweet_df, 'text', 'clean_text_list', True )
    else:
        tweet_df = None

    return tweet_df


def classify_tweets(tweet_df: pd.DataFrame, pickled_model: str, pickled_vectorizer: str, model_var: str, model_type: str):
    """
    Processes a dataframe of tweets (resulting from passing an S3 tweet file through tweet_text_from_file)
    and classifies each tweet with the models passed in. Adds the probability of "success" to the data frame.

    Args:
        tweet_df: pandas DataFrame with a 'text' column for classification
        pickled_model: str, path and file name for the pickled model
        pickled_vectorizer: str, path and file name for the feature vectorizer
        model_var: str, this is the column that will be added to the data frame with the resulting model scores
        process_topic: a Topic object that contains definitions for models

    Returns:
        tweet_df: the incoming data frame is appended with a column named after the model_var that contains the
        probabilities from the classification model. This is for binary classifiers only at this point.
    """

    # Load the model
    model = pickle.load(open(pickled_model, 'rb'))
    vectorizer = pickle.load(open(pickled_vectorizer, 'rb'))

    if model_type == 'w2v':
        data_features = vectorizer.transform(tweet_df['clean_text_list'])
    else:
        data_features = vectorizer.transform(tweet_df['clean_text'])

    try:
        predictions = model.predict_proba(data_features)
        tweet_df[model_var] = [i[1] for i in predictions]
    except:
        try:
            predictions = model.predict(data_features)
            tweet_df[model_var] = predictions
        except Exception as e:
            logging.exception(e)
            pass
    return tweet_df


def run_topic_models(infile: str, topic: Topic, startline=0, endline=9000000):
    """ Runs all topic models on the input file and returns classifications

    Args:
        infile: str, directory and file name for the .json tweet file
        topic: Topic, this is where the models are defined
        startinle: int, optional - what line to start on in the file
        endline: int, optional - what line to end on in the file

    Returns:
        tweet_df: dataframe with all of the tweets and the probabilities for each classification model.
        Each model appends a column to the tweet_df named after the model name.
    """

    tweet_df = tweet_text_from_file(infile, filters=topic.filters, exclusions=topic.exclusions)
    # tweet_df = tweet_text_from_file(infile, exclusions=topic.exclusions)
    if tweet_df is not None:
        for model in topic.models_list:
            msg = "Running {}: {} model.".format(topic.name, model.name)
            logging.info(msg)
            print(msg)
            tweet_df = classify_tweets(tweet_df=tweet_df,
                                       pickled_model=model.model_path+model.filename,
                                       pickled_vectorizer=model.model_path+model.vectorizer,
                                       model_var=model.name,
                                       model_type=model.type)
            # except Exception as e:
            #     logging.exception(e)
            #     raise.Exception
            #     return
        return tweet_df


def save_classified_tweets(infile: str, tweet_df: pd.DataFrame, topic: Topic, threshold=0.5, con=None):
    """ With classified tweets in the tweet_df, write the tweet and users to the database connection

    Args:
        infile: str, directory and file name for the .json tweet file
        tweet_df: DataFrame, the resulting DataFrame from run_topic_models
        class_cols: list of strings, the columns in tweet_df containing the classification probabilities
        threshold: float, the probability cutoff for a positive classification. Default=0.5.
        con: database connection -the destination for writing the file.

    Returns:
        None
    """

    # Reduce the tweet_df to only those tweets that meet or exceed the threshold
    for i in range(0, len(topic.models_list)):
        if i == 0:
            filter = "(tweet_df['{}']>={})".format(topic.models_list[i].name, threshold)
        else:
            filter += " | (tweet_df['{}']>={})".format(topic.models_list[i].name, threshold)

    scores_df = tweet_df[eval(filter)]
    save_tweets = list(scores_df['tweet_id'])

    with open(infile, encoding='UTF-8') as f:
        # Store the list of unsaved tweets so they can be excluded from the score upload
        unsaved_tweets = []
        for line in f:
            tweet_dict = json.loads(line)
            try:
                tweet_id = tweet_dict['id']
                if tweet_id in save_tweets:
                    new_tweet = Tweet.from_dict(tweet_dict)
                    merge_tweet(new_tweet, con=con)

            except Exception as e:
                logging.exception('tweet_id: {} in {} failed to load.'.format(tweet_id, infile))
                logging.exception('Tweet load error: {}'.format(e))
                # add the unsaved tweet to the list so the scores can also be excluded from saving
                unsaved_tweets.append(tweet_id)
                pass

        # Save the scores
        for model in topic.models_list:
            # The column order is important - they get renamed to match the DB in the RDSQueries
            try:
                save_scores = scores_df[['tweet_id', model.name]]
                save_scores = save_scores[~save_scores.tweet_id.isin(unsaved_tweets)]
                save_scores['model_id'] = model.model_id
                q.save_scores(save_scores=save_scores, con=con)
            except Exception as e:
                logging.exception("Failed saving scores: {}.".format(infile))
                logging.exception("Score save error: {}".format(e))
                pass
    f.close()


def move_s3_file(s3bucket, file_key, dest):
   """Move an S3 file to a new location and delete from the current.

       Args:
        s3: s3 connection
        s3bucket: str, s3 bucket name
        file_key: str, name of the file to move
        proc_key: str, name of file after moving
        dest: str, destination folder in S3

    Returns:
        None
    """
   proc_key = get_processed_key(file_key, dest)
   print("Copying to: {}".format(proc_key))
   copy_source = {
       'Bucket': s3bucket,
       'Key': file_key
   }

   boto3.setup_default_session(profile_name='di')
   s3 = boto3.client('s3')

   try:
       response = s3.copy(copy_source, s3bucket, proc_key)
       print("Deleting: {}".format(file_key))
       client = boto3.client('s3')
       response = s3.delete_object(
           Bucket=s3bucket,
           Key=file_key)
   except Exception as e:
       logging.error(e)

   # Delete File



def process_s3_files(topic_id:int ,s3bucket: str, s3prefix: str, threshold=0.5, con=None):
    """ Takes an S3 path and topic and looks for .json files in the S3 path and processes them

    Args:
        s3bucket: str, S3 bucket containing the files to process
        s3prefix: str, file prefix that will identify the files in the S3 bucket
        topic: Topic, contains the information on models to process the tweets
        threshold: float, the probability cutoff for a positive classification. Default=0.5.
        con: database connection -the destination for writing the file.

    Returns:
        None
    """

    # Get database connection:
    if con is None:
        con = q.RDSconfig.get_connection('postgres')

    # Create the topic
    run_topic = Topic(topic_id=topic_id )
    run_topic.readTopic(db=con)

    # Check for files
    files = []
    boto3.setup_default_session(profile_name='di')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3bucket)

    try:
        for o in bucket.objects.filter(Prefix=s3prefix):
            if (o.key[-5:]=='.json' and o.key.find("Processed") == -1):
                files.append(o.key)
    except Exception as e:
        logging.exception(e)
        print(e)

    # Process each file
    msg = '{} {} files to process.'.format(run_topic.name, len(files))
    print(msg)
    logging.info(msg)
    for file_key in files:
        # Download file
        temp = 'temp_file.json'
        s3.meta.client.download_file(s3bucket, file_key, temp)
        try:
            tweet_df = run_topic_models(infile=temp, topic=run_topic)
            if tweet_df is not None:
                save_classified_tweets(infile=temp, tweet_df=tweet_df, topic=run_topic, threshold=threshold, con=con)
            # if successful - copy the file to the "processed" folder and delete from the original folder
                move_s3_file(s3bucket, file_key, 'Processed')
            else:
                msg='No tweets found in {}.'.format(file_key)
                logging.info(msg)
                move_s3_file(s3bucket, file_key, 'Processed')

        except Exception as e:
            logging.error(e)
            move_s3_file(s3bucket, file_key, 'Failed')
            continue
