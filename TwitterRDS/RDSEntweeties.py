"""
Created in December 2017

This file contains all of the connections between "Entweeties" objects and a relational database

There is code for SQLite and Postgres databases. Initial development was with SQLite. However, since
we've moved over to Postgres, SQLite development has stopped, so some of that code may no longer work.

@author: tom trinter
"""

from . import RDSQueries as q
from . import RDSconfig
from TwitterFunctions import parse_it
from Entweeties import TwitterUser, Tweet

def get_user_data(u: TwitterUser):
    """Pulls data FROM RDS for a single user object and updates that user's properties

    Args:
        u: an Entweety User object that has at a minimum the twitter_id populated

    Returns: a dictionary of user object values
    """
    twitter_id = u.id
    user_data = q.get_single_user(twitter_id)

    #Get the list of defined properties
    user_attr = list(vars(u).keys())

    #Look for each property in the data, set the property if found
    for attr in user_attr:
        if attr in user_data.keys():
            setattr(u, attr, user_data[attr] )

    return u

def format_sql_value(key, value):
    """wraps the parse_it function based on data type"""
    attribute_list = TwitterUser.user_attributes
    key_type = attribute_list[key]
    if key_type == "string":
        formatted_value = "'{}'".format(parse_it(value))
    else:
        formatted_value = value
    return formatted_value


def merge_user(user: TwitterUser, con=None):
    """MERGES data TO RDS for a single user object (create if missing, update if existing - based in id)
    Args:
        user: an Entweety User object that has at a minimum the twitter_id populated. This method
    will not get any new data for the user. It will only merge whatever data is passed into the RDS.
        con: an RDS connection. This can be used to send data to a different instance of RDS.
    Default will send the data into the production RDS instance

    Returns:
        user_id
    """

    if con is None:
        con = RDSconfig.get_connection()

    prop_dict = vars(user)
    exclude_vars = ['id']  # these are attributes that are excluded from the update
    user_id = user.id

    # IF we name the database columns consistent with the object values we can set values
    # in a loop, rather than explicitly setting each one! Assuming the properties will be
    # prefixed with us_ in the database.

    # Check if user exists:
    try:
        user_id = q.check_for_user(user_id , con)
    except Exception as e:
        print(e)

    if user_id > 0:
        # UPDATE
        q.update_user(user.id, user.name, user.screen_name,user.location,
                      user.followers_count, user.friends_count, user.favourites_count, user.description,
                      user.geo_enabled, user.lang,user.statuses_count, user.time_zone, user.created_at, user.verified,
                      user.utc_offset, user.contributors_enabled, user.listed_count, user.protected, user.url, con=con)

    else:
        # INSERT
        q.insert_user(user.id, user.name, user.screen_name,user.location,
                      user.followers_count, user.friends_count, user.favourites_count, user.description,
                      user.geo_enabled, user.lang,user.statuses_count, user.time_zone, user.created_at, user.verified,
                      user.utc_offset, user.contributors_enabled, user.listed_count, user.protected, user.url,con=con)


def merge_tweet(tweet: Tweet, con=None):
    """MERGES data TO RDS for a tweet (create if missing, update if existing - based in id)
    Args:
        tweet: an Entweety Tweet object that has at a minimum the id, text and user (shell with id minimum)
                populated. This method will not call the Twitter API - only save data that is present in the
                Tweet object.
        con: an open RDS connection. This can be used to send data to a different instance of RDS. Default will
        send the data into the production RDS instance

    Returns:
        None

    The Tweet object from Twitter has a lot of data besides the Tweet itself in it. This method will do all of the
    following, if the data is present in the Tweet object that is sent in:

    1. Load/Merge the User object
    2. Load the Tweet object and create a "TWEET" relationship between the Tweet and the User
    3. Not Implemented: Load/Merge the "in-reply-to" User, if present and create "REPLY" relationship to the in-reply-to user
    4. Not Implemented: Load/Merge any users in the entities['user_mentions'] list and Create "MENTIONS" relationships
    """

    if con is None:
        con = RDSconfig.get_connection('sqllite')

    # 1. Load/Merge the User Object
    merge_user(tweet.user, con)

    #2. Check if the Tweet already exists
    tweet_id = q.check_for_tweet(tweet.id, con=con)
    if tweet_id > 0:
        return tweet_id
    else:
        # Insert Tweet
        try:
            tweet_id = q.insert_tweet(tweet.id, tweet.created_at,tweet.text,tweet.user.id,
                                      tweet.favorite_count, tweet.favorited, tweet.in_reply_to_status_id, tweet.in_reply_to_user_id,
                                      tweet.lang, tweet.place, tweet.retweet_count, tweet.retweeted, con=con)
            return tweet_id
        except Exception as e:
            print(e)
            return -1

