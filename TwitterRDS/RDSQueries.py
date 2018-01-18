from datetime import datetime
from TwitterFunctions.AQTwitterFunctions import get_followers, parse_it
from . import RDSconfig
import pandas as pd
from datetime import datetime

# source='sqllite'
source='postgres'

def check_string(instring:str):
    if instring is None:
        return None
    else:
        return parse_it(instring)

def fix_none(SQL: str):
    SQL = SQL.replace("None", "NULL")
    SQL = SQL.replace("'NULL'", "NULL")
    return SQL


############ GENERAL ############
def getidentity(cursor, table, id_col):
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


def insert_user(id, name, screen_name,
                location=None, followers_count=None, friends_count=None, favourites_count=None,
                description=None, geo_enabled=None, lang=None, statuses_count=None,
                time_zone=None, created_at=None, verified=None, utc_offset=None,
                contributors_enabled=None, listed_count=None, protected=None, url=None, con=None):

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
        int(contributors_enabled), listed_count, int(protected), url)

    SQL = fix_none(SQL)

    try:
        cur.execute(SQL)
        con.commit()
        return getidentity(cur, 'users', 'id')

    except Exception as e:
        print(e)
        return -1


def update_user(id, name, screen_name,
                location=None, followers_count=None, friends_count=None, favourites_count=None,
                description=None, geo_enabled=None, lang=None, statuses_count=None,
                time_zone=None, created_at=None, verified=None, utc_offset=None,
                contributors_enabled=None, listed_count=None, protected=None, url=None, con=None):

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
    if con is None:
        con = RDSconfig.get_connection()

    update_time = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    SQL = "INSERT INTO follower_update_hist (fu_twitter_id, fu_records_added, fu_update_date) VALUES ("
    SQL = SQL + str(leaderId) +"," + str(record_count)+ ", '" + update_time + "')"

    con.execute(SQL)
    con.commit()


def get_prior_user_list(leader_id, con=None):
    if con is None:
        con = RDSconfig.get_connection()

    SQL = 'SELECT tf_follower_id FROM user_followers WHERE tf_user_id={};'.format(leader_id)
    existing_ids = pd.read_sql(SQL, con)

    return existing_ids['tf_follower_id'].tolist()


############ TOPICS ############
def insert_topic(topic_name: str, filters=[], exclusions=[], topic_description: str=None, con=None):
    if con is None:
        con = RDSconfig.get_connection(source)
    cur = con.cursor()

    SQL = """ INSERT INTO topics (tp_name, tp_filters, tp_exclusions, tp_description, tp_create_dt)
    VALUES ("{}","{}","{}","{}","{}");""".format(
        topic_name.replace("'"," "),
        str(filters),
        str(exclusions),
        topic_description,
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
                      "description": tr['tp_description']
        }
        return topic_dict
    else:
        return None


def read_topic_models(topic_id:int, con=None):
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
