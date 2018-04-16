# encoding: utf-8

"""
Created over the course of the project, mostly in the Spring of 2016.
This has all of the different calls to the Tweepy API to pull and save data from Twitter.

@author: tom trinter
"""

import tweepy
import sys
from pandas import to_numeric
from pandas.io.json import json_normalize
from pandas import read_sql
from TwitterFunctions.TwitterProcessing import parse_it
from Entweeties import TwitterUser
from math import ceil

# this is a file that contains the twitter API authentication keys.
import TwitterFunctions.Twitter_config as Config

# In[2]:

consumer_key = Config.auth['consumer_key']
consumer_secret = Config.auth['consumer_secret']
access_token = Config.auth['access_key']
access_token_secret = Config.auth['access_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser(), wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Some column definitions for use in multiple functions:
user_columns = [
    'user.id',
    'user.id_str',
    'user.name',
    'user.screen_name',
    'user.location',
    'user.followers_count',
    'user.friends_count',
    'user.description',
    'user.favourites_count',
    'user.geo_enabled',
    'user.lang',
    'user.statuses_count',
    'user.time_zone',
    'user.created_at',
    'user.verified',
    'user.utc_offset',
    'user.contributors_enabled',
    'user.listed_count',
    'user.protected',
    'user.url']

user_column_names = [
    'id',
    'id_str',
    'name',
    'screen_name',
    'location',
    'followers_count',
    'friends_count',
    'description',
    'favourites_count',
    'geo_enabled',
    'lang',
    'statuses_count',
    'time_zone',
    'created_at',
    'verified',
    'utc_offset',
    'contributors_enabled',
    'listed_count',
    'protected',
    'url']

tweet_columns = [
    'id',
    'user.id',
    'source',
    'text',
    'created_at']


def get_twitter_ids(api_response):
    """
    Takes the response from an API call and cycles through all pages of the result (cursor)
    and combines all the ids into one list. If, in the process of going through the cursor the 
    rate limit is hit, then this function should trip the 15 minute wait.

    Args:
        api_response: this is the response that is returned from the api call.

    Returns:
        a list of ids. Returns an empty list if there were no ids returned from the API call
    """
    twitter_ids = []

    last_page = False
    pages = api_response.pages()
    try:
        page = next(pages)
        twitter_ids = page['ids']
        if page['next_cursor'] == 0:
            last_page = True

    except:
        print("Unknown error")
        last_page = True
        pass

    while not last_page:
        try:
            page = next(pages)
            twitter_ids.extend(page['ids'])
            if page['next_cursor'] == 0:
                last_page = True
        except tweepy.TweepError as e:
            print("Tweepy error:" + e.message[0]['code'])
            if page['next_cursor'] == 0:
                last_page = True
            pass
    return twitter_ids


def get_followers(userid, paged=False, existing_list=[], complete_ratio=0.999):
    """
    Calls the GET followers/ids API function (https://dev.twitter.com/rest/reference/get/followers/ids)

    Args:
        userid: Twitter user-id for the user who's followers you want to pull
        paged: optional - determines whether to get all or just the first page - changes the call
        existing_list: list of existing twitter followers' ids. If provided, the function will only return new users
        not in the existing_list
        complete_ratio: ratio from zero to 1 used as a cutoff for the search for new followers. Calculated as the
        the fraction of followers found that we already recognized divided by the total number new followers found.
        Twitter users should be returned in reverse order from when they began following, so a search for new followers
         should eventually get to the point where we already have the full list.

    Returns: a list of ids of all users following the user = userid.
    Returns an empty list if there were no ids returned from the API call
    """
    #     test_rate_limit(api,'followers')

    if complete_ratio is None:
        complete_ratio = 0.999

    if paged:
        twitter_ids = []
        cursor = -1

        while cursor != 0:
            try:
                followers = api.followers_ids(user_id=userid, cursor=cursor, stringify_ids=True)
                follower_list = [int(i) for i in followers[0]['ids']]
                new_followers = set(follower_list) - set(existing_list)
                if cursor == -1:
                    twitter_ids = list(new_followers)
                else:
                    twitter_ids.extend(new_followers)

                # According to the Twitter API, followers are returned in reverse order from their following
                # Therefore if any of the followers found are already in our existing_list, then we've found
                # all new users.
                ratio = len(new_followers) / len(follower_list)
                if ratio > complete_ratio:
                    cursor = 0  # exit while loop
                else:
                    # Get the next_cursor value
                    cursor = followers[1][1]

            except:
                print("Tweepy error")
                cursor = 0
                pass

        return twitter_ids

    else:
        # Not using the cursor here, since due to rate limits, we'll pull friends of less than 5000 followers anyway
        # Since we're looking for most commonly followed ids, we should get that from a smaller list of followers
        followers = api.followers_ids(user_id=userid, stringify_ids=True)
        follower_ids = followers['ids']

    return follower_ids


def get_followers_by_handle(userhandle, paged=False):
    """     Calls the GET followers/ids API function (https://dev.twitter.com/rest/reference/get/followers/ids)

    Args:
    userhandle: Twitter user-id for the user who's followers you want to pull
    paged: determines whether to get all or just the first page - changes the call

    Returns:
    a list of ids of all users following the user = userid.
    Returns an empty list if there were no ids returned from the API call
    """

    #     test_rate_limit(api,'followers')
    if paged:
        followers = tweepy.Cursor(api.followers_ids, screen_name=userhandle, stringify_ids=True)
        follower_ids = get_twitter_ids(followers)

    else:

        # Not using the cursor here, since due to rate limits, we'll pull friends of less than 5000 followers anyway
        # Since we're looking for most commonly followed ids, we should get that from a smaller list of followers
        followers = api.followers_ids(screen_name=userhandle, stringify_ids=True)
        follower_ids = followers['ids']

    return follower_ids


def get_friends(userid, paged=False, existing_list=[], complete_ratio=1):
    """    Calls the GET friends/ids API function (https://dev.twitter.com/rest/reference/get/friends/ids)

    Args:
    userid: Twitter user-id for the user who's followers you want to pull
    paged: determines whether to get all or just the first page - changes the call
    existing_list: list of existing twitter friends' ids. If provided, the function will only return new users not in the
    existing_list
    complete_ratio: ratio from zero to 1 used as a cutoff for the search for new followers. Calculated as the
    the fraction of followers found that we already recognized divided by the total number new followers found. Twitter
    users should be returned in reverse order from when they began following, so a search for new followers should
    eventually get to the point where we already have the full list.

    Returns:
    a list of ids of all users that user = userid is following.
    Returns an empty list if there were no ids returned from the API call
    """

    if paged:
        twitter_ids = []
        cursor = -1

        while cursor != 0:
            try:
                friends = api.friends_ids(user_id=userid, cursor=cursor, stringify_ids=True)
                friends_list = to_numeric(friends[0]['ids'])
                new_friends = set(friends_list) - set(existing_list)
                if cursor == -1:
                    twitter_ids = friends[0]['ids']
                else:
                    twitter_ids.extend(friends[0]['ids'])

                ratio = len(new_friends) / len(friends_list)
                if ratio > complete_ratio:
                    cursor = 0  # exit while loop
                else:
                    # Get the next_cursor value
                    cursor = friends[1][1]
            except:
                print("Tweepy error")
                if cursor == -1:
                    # if it craps out on the first try it's probably a permission error; return empty list
                    twitter_ids = []
                    cursor = 0
                else:
                    pass

        return twitter_ids

    else:
        try:
            friends = api.friends_ids(user_id=userid, stringify_ids=True)
            friend_ids = friends['ids']

        except:
            print("Tweepy error")
            friend_ids = []

    return friend_ids


def get_users(user_list, byhandle=False):
    """    Calls the GET users/lookup API function
    (https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-lookup)

    Args:
        user_list: comma delimited list of userids or screen_names. NOTE - API limit is 100 users. If the user_list
    is longer than 100, this function will pass it in blocks of 100.
        byhandle: boolean - if True, then the list should be screen_names, otherwise user_ids

    Returns:
    a dataframe with the key data that we are interested in for each user.
    """

    list_length = len(user_list)
    iterations = ceil(list_length/100)

    for i in range(iterations):
        sublist = user_list[i*100:min((i+1)*100, list_length)]

        if byhandle:
            users_json = api.lookup_users(screen_names=sublist)

        else:
            try:
                users_json = api.lookup_users(sublist)
            # except TweepError as e:
            #     return "TweepError:" + e.message[0]['code']
            except:
                return "Unexpected error:", sys.exc_info()[0]

        temp_df = json_normalize(users_json)
        temp_df = temp_df[user_column_names]
        if i == 0:
            users_df = temp_df
        else:
            users_df = users_df.merge(temp_df, how="outer")

    # parse descriptions to clear out bad data
    users_df['description'] = users_df['description'].apply(lambda x: parse_it(x))

    # set the index on the dataframe to equal the userid
    users_df.set_index('id', inplace=True)
    return users_df


def get_tweets_historic(q, max_tweet_id):
    """ Calls the GET search/tweets API function (https://dev.twitter.com/rest/public/search)

    Args:
        q: a delimited list of search terms with search terms in "" and separated by OR
        max_tweet_id: for a cursor, this says what is the max tweet id, or starting point in the list
        a dataframe with the key user data of users tweeted

    Returns:
        a dataframe with the specifics of the tweets and the associated user ids
         new max_tweet_id
        status: success or error
    """

    try:
        tweets = api.search(q, count=100, max_id=max_tweet_id)
        tweet_df = json_normalize(tweets['statuses'])
        new_max_tweet_id = tweet_df['id'].min()
        status = "success"

        user_df = tweet_df[user_columns]
        user_df.columns = user_column_names

        tweet_detail_df = tweet_df[tweet_columns]
        tweet_detail_df.rename(columns={'id': 'tweet_id', 'user.id': 'user_id'}, inplace=True)

    except:
        # this will cause the loop to stop! intentional to stop when it gets to the end of the 7 day period
        new_max_tweet_id = max_tweet_id
        status = "Other Error"
        user_df = None
        tweet_detail_df = None

    return user_df, tweet_detail_df, new_max_tweet_id, status


def get_db_followers(leader_id, conn):
    """Checks the database for existing followers associated with the leader_id user

    Args:
        leader_id: int, this is the twitter user id of interest
        conn: connection to a database with matching the Twitter SQLite database structure

    Returns:
        list of twitter IDs of users that are following the leader id
    """
    sql = 'SELECT tf_follower_id FROM user_followers WHERE tf_user_id={};'.format(leader_id)
    existing_ids = read_sql(sql, conn)

    return existing_ids['tf_follower_id'].tolist()


def get_db_friends(follower_id, conn):
    """Checks the database for existing friends associated with the leader_id user

    Args:
        follower_id: int, this is the twitter user id of interest
        conn: connection to a database with matching the Twitter SQLite database structure

    Returns:
        list of twitter IDs of twitter users that the follower_id is following
    """
    sql = 'SELECT tf_user_id FROM user_followers WHERE tf_follower_id={};'.format(follower_id)
    existing_ids = read_sql(sql, conn)

    return existing_ids['tf_user_id'].tolist()


def get_tweets_by_id(tweet_id_list):
    """ Calls the Twitter API to retrieve id's by a list of tweet ids

    Args:
        tweet_id_list:

    Returns:
         tweet_detail_df: a pandas dataframe with the details from the tweet
         user_df: a panas dataframe with the user data
    """


    tweets = api.statuses_lookup(id_=tweet_id_list, include_entities=False)
    tweet_df = json_normalize(tweets)

    user_df = tweet_df[user_columns]
    user_df.columns = user_column_names
    user_df = user_df.drop_duplicates()

    tweet_detail_df = tweet_df[tweet_columns]
    tweet_detail_df.rename(columns={'id': 'tweet_id', 'user.id': 'user_id'}, inplace=True)

    return tweet_detail_df, user_df


def hydrate_user(user: TwitterUser):
    """ Calls the Twitter API to set properties of a single user object

    Calls the GET users/lookup API function (https://dev.twitter.com/rest/reference/get/users/lookup)

    Args:
        user: Entweeties user object

    Returns:
        The hydrated user
    """

    id_list = [user.id]
    try:
        user_dict = api.lookup_users(id_list)[0]
    except TweepError as e:
        return "TweepError:" + e.message[0]['code']

    prop_dict = vars(user)

    for key in prop_dict:
        if key in user_dict:
            setattr(user,key,user_dict[key])

    return user


def hydrate_users(user_list: list):
    """
    Calls the GET users/lookup API function (https://dev.twitter.com/rest/reference/get/users/lookup)

    Args:
        user_list: a list of Entweeties user objects seeking hydration

    Returns:
        Returns the hydrated user list for all users found.
        Sets hydrated = 1 for users found.
        Sets hydrated = -1 for users not found
        Hydrated = 0 is reserved for user nodes that are not hydrated
    """
    id_list = []
    for user in user_list:
        if isinstance(user, TwitterUser):
            id_list.append(user.id)

    try:
        user_data = get_users(id_list)
    except Exception as e:
        return "TweepError:" + e.message[0]['code']

    # The user_data returned is a pandas dataframe.
    # The dataframe is indexed by the user id, so we can use that to match
    ids_found = user_data.index.tolist()
    for user in user_list:
        if user.id in ids_found:
            data = user_data.loc[user.id]
            for key in vars(user).keys():
                if key in data.keys():
                    setattr(user, key, data[key])
                setattr(user, 'hydrated',1)
        else:
            # set the "hydrated" attribute to -1 to indicate a failure to hydrate
            setattr(user, 'hydrated', -1)

    return user_list