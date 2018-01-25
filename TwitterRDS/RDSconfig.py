import psycopg2
import sqlite3 # for testing only

#postgres
hostname = 'ditwitter.c6rgtnn1vfuu.us-east-1.rds.amazonaws.com'
username = 'ditwitter_sa'
pwd = 'ThriventTwitter'
database = 'ditwitter'
from sqlalchemy import create_engine

#sqlite
def get_connection(source: str = 'postgres'):
    if source == 'sqllite':
        con = sqlite3.connect('D:/AQ/Clients/Thrivent/AQTwitter.db')
    elif source == 'postgres':
        con = psycopg2.connect(host=hostname, user=username, password=pwd, dbname=database )
    else:
        con = None
    return con


def get_sqlalchemy_engine(source: str = 'postgres'):
    if source == 'sqllite':
        engine = get_connection('sqllite')
    elif source =='postgres':
        user_str = '{}:{}@{}/{}'.format(username, pwd, hostname, database)
        engine = create_engine('postgresql+psycopg2://{}'.format(user_str))
    return engine


