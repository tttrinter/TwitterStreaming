"""
Created in December 2017

This file contains all of the SQL queries used with RDS - SQLite or Postgres

There is code for SQLite and Postgres databases. Initial development was with SQLite. However, since
we've moved over to Postgres, SQLite development has stopped, so some of that code may no longer work.

@author: tom trinter
"""

# from datetime import datetime
import pandas as pd
import os  # used to capture computer name and pid for process mangement
from TwitterFunctions import get_followers, parse_it
from . import RDSconfig
from datetime import datetime

# source='sqllite'
source = 'postgres'

def check_string(instring:str):
    if instring is None:
        return None
    else:
        return parse_it(instring)

def fix_none(SQL: str):
    # Replaces None and Null in SQL statements so they don't error out
    SQL = SQL.replace("None", "NULL")
    SQL = SQL.replace("'NULL'", "NULL")
    SQL = SQL.replace("nan", "NULL")
    return SQL


############ GENERAL ############
def getidentity(cursor, table, id_col):
    """ Gets the most recent identity value from the database in the session for a recent insert

    Args:
        cursor: database cursor - should be part of an open connection
        table: the table for which you want the latest identity value
        id_col: what column in the database is the primary key

    Returns:
        integer - latest id for the table.
    """

    #TODO: could this be an issue with multiple users or processes? Seems possible, but low risk.

    if source =='sqllite':
        get_index = cursor.execute("SELECT last_insert_rowid();")
        return get_index.lastrowid
    elif source == 'postgres':
        cursor.execute("SELECT currval(pg_get_serial_sequence('{}', '{}')) as new_id;".format(table, id_col))
        new_id = cursor.fetchone()[0]
        return new_id
    else:
        return None


############ TWEETS ############
def check_for_tweet(tweet_id, con=None):
    """ Checks to see if a tweet has already been loaded to avoid duplicates.

    Args:
        tweet_id: bigint
        con: database connection; function will use default connection if none.

    Returns:
        tweet_id if found, -1 if not found.
    """
    if con is None:
        con = RDSconfig.get_connection()

    SQL = "SELECT tweet_id FROM tweets WHERE tweet_id={}".format(tweet_id)
    try:
        tweet = pd.read_sql(SQL, con=con)
    except Exception as e:
        return e

    if len(tweet) > 0:
        return tweet_id
    else:
        return -1


def insert_tweet(tweet_id, created_at, text, user_id,
                 favorite_count=None, favorited=None, in_reply_to_status=None,
                 in_reply_to_user_id=None, lang=None, place=None, retweet_count=None, retweeted=None, con=None):
    """
    Inserts a Tweet into the database. All params below are directly from Twitter, except for the connection
    :param tweet_id:
    :param created_at:
    :param text:
    :param user_id:
    :param favorite_count:
    :param favorited:
    :param in_reply_to_status:
    :param in_reply_to_user_id:
    :param lang:
    :param place: Nulling this out for now. Need to decide how to store this in the future.
    :param retweet_count:
    :param retweeted:
    :param con: database connection. Uses default if none.
    :return: tweet_id after insertion; -1 if failed.
    """

    if con is None:
        con = RDSconfig.get_connection()
    cur = con.cursor()

    SQL = """INSERT INTO tweets (
            tweet_id, id_str, created_at, text, user_id, 
            favorite_count, favorited, in_reply_to_status_id,
            in_reply_to_user_id, lang, place, retweet_count, retweeted) 
            VALUES ({}, '{}', '{}', '{}', {},
            {}, '{}', {},
            {}, '{}', '{}', {}, '{}');""".format(tweet_id, str(tweet_id), created_at, check_string(text), user_id,
                                          favorite_count, int(favorited), in_reply_to_status,
                                          in_reply_to_user_id, lang, None, retweet_count, int(retweeted))

    #Todo: figure out what do do with place - currently not saving.

    SQL = fix_none(SQL)
    try:
        cur.execute(SQL)
        con.commit()
        return tweet_id
    except Exception as e:
        print(e)
        return -1


def save_scores(save_scores: pd.DataFrame, con=None):
    """
    Saves probabilities from scored tweets and their associated topic models to the tweet_scodes table in the database.
    :param save_scores: dataframe with the tweet ids and scores
    :param con: database connection. uses default if None.
    :return: None
    """
    if con is None:
        con = RDSconfig.get_connection()

    # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')

    if len(save_scores) > 0:
        # Rename columns to match DB col names
        save_scores.columns = ['ts_tweet_id', 'ts_score', 'ts_md_id']
        engine = RDSconfig.get_sqlalchemy_engine(source)
        save_scores.to_sql('tweet_scores', con=engine, if_exists='append', index=False)

    return


############ USERS ############
def check_for_user(user_id, con=None):
    """
    Checks the database for a user_id to avoid duplicates
    :param user_id: Twitter user id
    :param con: database connection. Uses default if none.
    :return: user_id, or -1 if not found.
    """
    if con is None:
        con = RDSconfig.get_connection()
    SQL = 'SELECT "id" FROM users WHERE "id"={}'.format(user_id)
    try:
        user = pd.read_sql(SQL, con=con)
    except Exception as e:
        return e

    if len(user) > 0:
        return user_id
    else:
        return -1


def check_for_usernames(user_id, con=None):
    """
    Checks the database for a record in the user_names table to avoid duplicates
    :param user_id: Twitter user id
    :param con: database connection. Uses default if none.
    :return: user_id, or -1 if not found.
    """
    if con is None:
        con = RDSconfig.get_connection()
    SQL = 'SELECT "un_user_id" FROM user_names WHERE "un_user_id"={}'.format(user_id)
    try:
        user = pd.read_sql(SQL, con=con)
    except Exception as e:
        return e

    if len(user) > 0:
        return user_id
    else:
        return -1


def insert_user(id, name, screen_name,
                location=None, followers_count=None, friends_count=None, favourites_count=None,
                description=None, geo_enabled=None, lang=None, statuses_count=None,
                time_zone=None, created_at=None, verified=None, utc_offset=None,
                contributors_enabled=None, listed_count=None, protected=None, url=None, con=None):
    """
    Inserts a user into the users table in the database. All parameters are directly from Twitter except for con.
    :param id:
    :param name:
    :param screen_name:
    :param location:
    :param followers_count:
    :param friends_count:
    :param favourites_count:
    :param description:
    :param geo_enabled:
    :param lang:
    :param statuses_count:
    :param time_zone:
    :param created_at:
    :param verified:
    :param utc_offset:
    :param contributors_enabled:
    :param listed_count:
    :param protected:
    :param url:
    :param con:
    :return: user_id after successful insert, or -1 if failed.
    """

    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """ INSERT INTO users (
    id, id_str, name, screen_name,
    location, followers_count, friends_count, favourites_count,
    description, geo_enabled, lang, statuses_count,
    time_zone, created_at, verified, utc_offset,
    contributors_enabled, listed_count, protected, url) 
    VALUES ({},'{}','{}','{}',
            '{}',{},{},{},
            '{}','{}','{}',{},
            '{}','{}','{}',{},
            '{}',{},'{}','{}');""".format(
        id, str(id), check_string(name), check_string(screen_name),
        check_string(location), followers_count, friends_count, favourites_count,
        check_string(description), int(geo_enabled), lang, statuses_count,
        time_zone, created_at, int(verified), utc_offset,
        int(contributors_enabled), listed_count, int(protected), check_string(url))

    SQL = fix_none(SQL)

    try:
        cur.execute(SQL)
        con.commit()
        return id

    except Exception as e:
        print(e)
        return -1


def update_user(id, name, screen_name,
                location=None, followers_count=None, friends_count=None, favourites_count=None,
                description=None, geo_enabled=None, lang=None, statuses_count=None,
                time_zone=None, created_at=None, verified=None, utc_offset=None,
                contributors_enabled=None, listed_count=None, protected=None, url=None, con=None):
    """
    Updates a user in the database. All parameters are directly from Twitter except for con;
    :param id:
    :param name:
    :param screen_name:
    :param location:
    :param followers_count:
    :param friends_count:
    :param favourites_count:
    :param description:
    :param geo_enabled:
    :param lang:
    :param statuses_count:
    :param time_zone:
    :param created_at:
    :param verified:
    :param utc_offset:
    :param contributors_enabled:
    :param listed_count:
    :param protected:
    :param url:
    :param con: database connection; uses default if None.
    :return: none
    """

    if con is None:
        con = RDSconfig.get_connection()
    cur = con.cursor()

    SQL = """UPDATE users 
            SET name = '{}', screen_name = '{}',
                location = '{}', followers_count={}, friends_count={}, favourites_count={},
                description='{}', geo_enabled='{}', lang='{}', statuses_count={},
                time_zone='{}', created_at='{}', verified='{}', utc_offset={},
                contributors_enabled='{}', listed_count={}, protected='{}', url='{}' 
                WHERE "id"={}""".format(check_string(name), check_string(screen_name),
                check_string(location), followers_count, friends_count, favourites_count,
                check_string(description), int(geo_enabled), lang, statuses_count,
                time_zone, created_at, int(verified), utc_offset,
                int(contributors_enabled), listed_count, int(protected), url, id)

    SQL = fix_none(SQL)

    cur.execute(SQL)
    con.commit()


def update_follower_hist(leaderId, record_count, con=None):
    """ Inserts a row into the follower_update_hist table with the leader id and datetime followers were updated
    Args:
        leaderId: int - twitter id for the account whose followers were updated
        record_count: number of records added
        con: database connection; uses default if none

    """
    if con is None:
        con = RDSconfig.get_connection()

    cur = con.cursor()

    update_time = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    SQL = "INSERT INTO follower_update_hist (fu_twitter_id, fu_records_added, fu_update_date) VALUES ("
    SQL = SQL + str(leaderId) +"," + str(record_count)+ ", '" + update_time + "')"

    cur.execute(SQL)
    con.commit()


def get_prior_user_list(leader_id, con=None):
    """Queries the database for all known followers of the leader_id. This is to avoid duplicates.

    Args:
        leader_id: twitter id for the account of interest
        con: database connection
    Returns:
        list of twitter ids for all known followers of the leader_id
    """
    if con is None:
        con = RDSconfig.get_connection()

    SQL = 'SELECT tf_follower_id FROM user_followers WHERE tf_user_id={};'.format(leader_id)
    existing_ids = pd.read_sql(SQL, con)

    return existing_ids['tf_follower_id'].tolist()


############ TOPICS ############
def insert_topic(topic_name: str, filters=[], exclusions=[], topic_description: str=None, threshold: float=0.5, sub_topics=[], con=None):
    """ Inserts a topic into the topics table in the database
    Args:
    :param topic_name: Name of the topic or life event
    :param filters: comma separated list of terms for the Twitter filter
    :param exclusions: comma separated list of exclusion terms
    :param topic_description: short description of the topic
    :param threshold: topic threshold for classification
    :param sub_topics: list of subtopic ids (integers) to run
    :param con: database connection

    Returns:
    topic id for the newly added topic. -1 if failed.
    """
    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    # TODO: need to add code to avoid duplicates

    SQL = """ INSERT INTO topics (tp_name, tp_filters, tp_exclusions, tp_description, tp_child_topics, tp_threshold, tp_create_dt)
    VALUES ("{}","{}","{}","{}","{}","{}","{}");""".format(
        topic_name.replace("'"," "),
        str(filters),
        str(exclusions),
        topic_description,
        threshold,
        sub_topics,
        str(datetime.now()))

    SQL = fix_none(SQL)
    try:
        cur.execute(SQL)
        con.commit()
        return getidentity(cur, 'topics', 'tp_id')

    except Exception as e:
        print(e)
        return -1


def read_topic(topic_id: int, con=None):
    """ Reads topic data from the database and returns a dictionary with topic parameters

    :param topic_id: unique identifier for the topic
    :param con: database connection
    :return: dictionary with all of the parameters for the topic
    """
    if con is None:
        con = RDSconfig.get_connection()
    SQL = "SELECT * FROM topics WHERE tp_id={}".format(topic_id)
    try:
        topic_df = pd.read_sql(SQL, con=con)
    except Exception as e:
        return e

    if len(topic_df) > 0:
        tr = topic_df.iloc[0]
        topic_dict = {"name": tr['tp_name'],
                      "filters": tr['tp_filters'],
                      "exclusions": tr['tp_exclusions'],
                      "description": tr['tp_description'],
                      "sub_topics": tr['tp_child_topics'],
                      "threshold": tr['tp_threshold']
        }
        return topic_dict
    else:
        return None


def read_topic_models(topic_id:int, con=None):
    """
    Finds all model data for a given topic and returns a dataframe with the model details
    Args:
        :param topic_id: topic unique identifier
        :param con: database connection
    Returns:
        dataframe with model parameters
    """
    if con is None:
        con = RDSconfig.get_connection()
    SQL = "SELECT * FROM models WHERE md_tp_id={}".format(topic_id)
    try:
        models_df = pd.read_sql(SQL, con=con)
    except Exception as e:
        return e

    if len(models_df) > 0:
        models_df.columns = ['model_id',
                             'topic_id',
                             'name',
                             'type',
                             'description',
                             'model_path',
                             'filename',
                             'vectorizer' ]
        return models_df
    else:
        return None


############ MODELS ############
def insert_model(name:str,
                 topic_id: int,
                 filename: str,
                 type: str=None,
                 description=None,
                 vectorizer:str=None,
                 model_path:str=None,
                 con=None):
    """
    Inserts model details into the database
    Args:
    :param name: short model name
    :param topic_id: link to topic unique identifier
    :param filename: string .sav filename for a pickled python model
    :param type: text description of model type. Usually "text" for Tweet models
    :param description: short description of the model
    :param vectorizer: string .sav filename for the vectorizer used together with the model
    :param model_path: string - location of the file and vectorizer; should be the location on the EC2 machine; need
    to override if running locally.
    :param con: database connection.
    :return: new model id or, -1 if failed
    """


    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """ INSERT INTO models (md_name, md_type, md_tp_id, md_description, md_filename, md_vect_file, md_path)
    VALUES ("{}","{}",{},"{}","{}","{}", "{}");""".format(
        name, type, topic_id, description, filename, vectorizer, model_path)

    SQL = fix_none(SQL)
    try:
        cur.execute(SQL)
        con.commit()
        return getidentity(cur, 'models', 'md_id')

    except Exception as e:
        print(e)
        return -1


def read_model(model_id:int, con=None):
    """
    Reads model details from the database and returns a series with the details for a single model
    :param model_id: model unique identifier
    :param con: database connection
    :return: series with model details.
    """
    if con is None:
        con = RDSconfig.get_connection()
    SQL = "SELECT * FROM models WHERE md_id={}".format(model_id)
    try:
        model_df = pd.read_sql(SQL, con=con)
    except Exception as e:
        return e

    if len(model_df) > 0:
        return model_df.iloc[0]
    else:
        return None


def get_leader_indicator_ids(con=None):
    """
    Gets the full list of leader id's for which we want to track followers. Currently
    only gets Christian and College leaders. Could be extended to more in the future.

    :param con: database connection
    :return: list of twitter ids. As currently stands, list is about 2000.
    """
    if con is None:
        con = RDSconfig.get_connection()

    SQL = """SELECT li_user_id 
    FROM leader_indicators 
    INNER JOIN users ON li_user_id=users.id
    WHERE li_indicator_id in (1,2)
    ORDER BY li_indicator_id """

    try:
        id_df = pd.read_sql(SQL, con)
    except Exception as e:
        return e

    if len(id_df) > 0:
        return list(id_df['li_user_id'])
    else:
        return None


def get_dehydrated_followers(minid=0, limit=10000, con=None):
    """
    Finds follower_ids from the user_followers table that do not have user records in the users table
    :param minid: minimum twitter id - used to skip ahead in the list if necessary.
    :param limit: int, limit of how many followers to pull
    :param con: database connection
    :return: list of twitter_ids needing user records
    """
    if con is None:
        con = RDSconfig.get_connection()

    SQL = """SELECT DISTINCT tf_follower_id, users.id 
    FROM user_followers 
    LEFT OUTER JOIN users ON tf_follower_id = users.id
    WHERE users.id is NULL
    AND tf_follower_id > {:d}
    ORDER BY tf_follower_id
    LIMIT {:d}""".format(minid, limit)

    try:
        id_df = pd.read_sql(SQL, con)
    except Exception as e:
        return e

    if len(id_df) > 0:
        return list(id_df['tf_follower_id'])
    else:
        return None


def upsert_usernames(user_id, names, con=None):

    if con is None:
        con = RDSconfig.get_connection()
    cur = con.cursor()
    uid = check_for_usernames(user_id, con)

    if uid > 0:
        # update record
        SQL = """ UPDATE user_names
        SET un_first_name = '{}', 
        un_last_name = '{}',
        un_first_match = {},
        un_last_match = {}
        WHERE un_user_id = {}""".format(names['first'],
                                        names['last'],
                                        names['first_match'],
                                        names['last_match'],
                                        user_id)
    else:
        SQL = """INSERT INTO user_names (
        un_first_name, 
        un_last_name,
        un_first_match,
        un_last_match, 
        un_user_id)
        VALUES ('{}','{}', {}, {}, {})""".format(names['first'],
                                        names['last'],
                                        names['first_match'],
                                        names['last_match'],
                                        user_id)

    cur.execute(SQL)
    con.commit()


def insert_stream_log(topic_id:int,
                      tweet_count: int,
                      api_acct: str,
                      con=None):
    """
    Inserts record of a stream start and the the account used
    Args:
        :param topic_id: topic id from the topics table
        :param tweet_count: number of tweets to gather before shutting down the stream
        :param api_acct: name of the twitter api account used for the stream
        :param con - database connection; default None, connection will be created
        :return: hist_id id or, -1 if failed
    """

    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """ INSERT INTO stream_run_hist(
                rh_tp_topic_id, 
                rh_start_dt,
                rh_tweet_count,  
                rh_api_acct_id,
                rh_computer_name, 
                rh_pid, 
                rh_status)
            VALUES ({},'{}',{},{},'{}',{}, 'running');""".format(
        topic_id,
        datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        tweet_count,
        api_acct,
        os.environ['COMPUTERNAME'],
        os.getpid())

    try:
        cur.execute(SQL)
        con.commit()
        return getidentity(cur, 'stream_run_hist', 'rh_run_hist_id')

    except Exception as e:
        print(e)
        return -1


def finish_stream_log(log_id:int, con=None):
    """
    Updates a stream log record with the finish time
    Args:
        :param log_id: stream log id from stream_run_hist
    """

    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """ UPDATE stream_run_hist 
            SET rh_end_dt = '{}', 
            rh_status = 'finished'
            WHERE rh_run_hist_id = {};""".format(
        datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        log_id)

    try:
        cur.execute(SQL)
        con.commit()

    except Exception as e:
        print(e)
        return -1


def dead_stream_log(pid:int, comp: str, con=None):
    """
    Updates a stream log record with the finish time
    Args:
        :param pid: process id
        :param comp: computer name
    """

    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """ UPDATE stream_run_hist 
            SET rh_end_dt = '{}', 
            rh_status = 'killed'
            WHERE rh_pid = {} 
            AND rh_computer_name = '{}'
            AND rh_status='running';""".format(
        datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        pid,
        comp)

    try:
        cur.execute(SQL)
        con.commit()

    except Exception as e:
        print(e)
        return -1


def dead_stream_log_bytopic(tp_id:int, comp: str, con=None):
    """
    Updates a stream log record with the finish time
    Args:
        :param tp_id: topic id
        :param comp: computer name
    """

    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """UPDATE stream_run_hist 
            SET rh_end_dt = '{}', 
            rh_status = 'killed'
            WHERE rh_tp_topic_id = {} 
            AND rh_computer_name = '{}'
            AND rh_status='running';""".format(
        datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        tp_id,
        comp)

    try:
        cur.execute(SQL)
        con.commit()

    except Exception as e:
        print(e)
        return -1


def get_next_api_acct(con=None):
    """
    :param con: database connection; one will be created if not provided
    :return: the account from the list with the oldest start date in the stream_run_hist table, or first alphabetically
    """
    if con is None:
        con = RDSconfig.get_connection()

    SQL = """SELECT api_acct_id, tw_act_name, coalesce(max(rh_start_dt),'2000-01-01') last_run
            FROM api_accts
            LEFT OUTER JOIN stream_run_hist ON rh_api_acct_id=api_acct_id
            WHERE tw_status=true
            GROUP BY api_acct_id, tw_act_name
            ORDER BY last_run, tw_act_name
            LIMIT 1"""

    try:
        api_df = pd.read_sql(SQL, con)
    except Exception as e:
        return e

    if len(api_df) > 0:
        return api_df.iloc[0]['tw_act_name'], api_df.iloc[0]['api_acct_id']
    else:
        return None


def get_topic_rundata(con=None):
    SQL = """SELECT tp_name, 
            tp_id, 
            tp_create_dt, 
            coalesce(tp_on_off, false) tp_on_off, 
            Null as rh_start_dt, 
            Null as rh_pid, 
            Null as rh_computer_name, 
            5000 as rh_tweet_count
            FROM topics 
            ORDER BY tp_name;"""

    if con is None:
        con = RDSconfig.get_connection()

    try:
        settorun_df = pd.read_sql(SQL, con)
    except Exception as e:
        return e

    if len(settorun_df ) > 0:
        return settorun_df
    else:
        return None


def get_running_topics(con=None):
    SQL = """SELECT tp_name, tp_id, tp_create_dt, rh_start_dt, rh_pid, rh_computer_name, rh_tweet_count
            FROM stream_run_hist
            INNER JOIN topics ON tp_id=rh_tp_topic_id
            WHERE tp_on_off=TRUE
            AND rh_end_dt IS NULL OR rh_status='running';"""

    if con is None:
        con = RDSconfig.get_connection()

    try:
        running_topics_df = pd.read_sql(SQL, con)
    except Exception as e:
        return e

    if len(running_topics_df) > 0:
        return running_topics_df
    else:
        return None


def update_user_indicator_counts(indicator_id, con=None):
    """Updates the user_indicator_counts table with the number of twitter 'leader-id's' followed by each user"""

    # delete the current records for this topic
    SQL = 'DELETE FROM user_indicator_counts WHERE ti_indicator_id = {}'.format(indicator_id)

    # get the connection
    if con is None:
        con = RDSconfig.get_connection(source)

    cur = con.cursor()

    try:
        cur.execute(SQL)
        con.commit()

    except Exception as e:
        print(e)
        return -1

    # Run the INSERT query
    SQL = """INSERT INTO user_indicator_counts (ti_user_id, ti_count, ti_indicator_id)
        SELECT tf_follower_id, count(tf_user_id), {}
        FROM user_followers
        INNER JOIN leader_indicators ON tf_user_id=li_user_id
        WHERE li_indicator_id={}
        GROUP BY tf_follower_id;""".format(indicator_id,indicator_id)

    try:
        cur.execute(SQL)
        con.commit()
        return 1

    except Exception as e:
        print(e)
        return -1


def get_indicators(con=None):
    """ Gets the list of defined indicators. Used in the UpdateFollowers process"""
    SQL = "SELECT * FROM indicators;"

    # get the connection
    if con is None:
        con = RDSconfig.get_connection(source)

    try:
        ind_df = pd.read_sql(SQL, con)
        if len(ind_df) > 0:
            return ind_df['indicator_id'].tolist()
        else:
            return []

    except Exception as e:
        print(e)
        return


def add_leader_indicator(indicator_id, user_id, conn):
    """Adds a Twitter user to the list of indicators for the topic defined by indicator_id

    Args:
        indicator_id = int, defines the topic and is found in the indicators table
        user_id = twitter user id for the user that is associated with the topic

    Returns:
        None

    """
    SQL = "INSERT INTO leader_indicators (li_user_id, li_indicator_id) VALUES ({}, {})".format(user_id, indicator_id)
    cur = conn.cursor()

    cur.execute(SQL)
    conn.commit()

    return ()


def get_topics_toprocess(con=None):
    """ Gets all of the topics that have models defined and are turned on"""
    SQL = """SELECT DISTINCT tp_id, tp_name, tp_threshold, tp_child_topics
            FROM topics t
            LEFT OUTER JOIN models m ON m.md_tp_id=t.tp_id
            WHERE tp_on_off = true 
            AND (m.md_filename is not NULL 
            AND m.md_vect_file is not null) 
            OR t.tp_child_topics is not NULL"""

    if con is None:
        con = RDSconfig.get_connection()

    try:
        process_topics_df = pd.read_sql(SQL, con)
    except Exception as e:
        return e

    if len(process_topics_df ) > 0:
        return process_topics_df
    else:
        return None



