"""
Created sometime in the Fall of 2017

Neo4J Cypher queries to support the connections between the graph and Entweeties objects.

Not currently used.

@author: tom trinter
"""


from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
from TwitterFunctions.TwitterFunctions import get_followers
from TwitterNeo4J import Neo4J_config

def connect_neo4j():
    """ Connects to the Neo4J instance using the user credentials in Neo4J_config.

    Args:
    None

    Returns:
    Connected Neo4J session
    """

    uri = Neo4J_config.auth['uri']
    user = Neo4J_config.auth['user']
    password = Neo4J_config.auth['password']

    driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
    session = driver.session()
    return session


def cypher_id_list(cypher, session=None):
    """ Queries Neo4J and returns a list of ids

    Args:
    cypher : string, cypher query that defines what list of
        object ids you want to retrieve. The cypher code must return 'n.id' for the nodes (n) and the property 'id'
        that you want to get in the list.
    session: the Neo4J session where you want to send the data. When None, creates its own connection from config data

    Returns:
    List of integers from n.id.
    """
    if session is None:
        session = connect_neo4j()

    ids = session.run(cypher)
    records = []
    for r in ids:
        records.append(int(r['n.id']))
    return records


def get_existing_followers(leader_id, session=None):
    """ Queries Neo4J for the list of known twitter followers for a single twitter user

    Args:
    leader_id : int, twitter id for the user of interest
    session: the Neo4J session where you want to send the data. When None, creates its own connection from config data

    Returns:
    list of ids."""

    if session is None:
        session = connect_neo4j()

    cypher = "MATCH (u:User {{id:{}}})<-[:FOLLOWS]-(n:User) RETURN n.id".format(leader_id)
    return cypher_id_list(cypher, session)


def update_follower_hist(leader_id, session=None):
    """ Updates Neo4J for a single User object setting the date of the last follower update

    Args:
    leader_id : int, twitter id for the user of interest
    session: the Neo4J session where you want to send the data. When None, creates its own connection from config data

    Returns: None
    """

    if session is None:
        session = connect_neo4j()

    update_time = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    cypher = "MATCH (u:User {{id:{}}}) SET u.follower_update_date='{}'".format(leader_id, update_time)
    session.run(cypher)


def add_user_and_edge(leader_id, follower_id, session=None):
    """ Adds an edge between two User nodes. If either node does not exist, it will create
     an empty shell node with only the id and id_str populated.

    Args:
    leader_id : int, twitter id for the Twitter user being followed
    follower_id: int, twitter id for the Twitter user following the leader_id
    session: the Neo4J session where you want to send the data. When None, creates its own connection from config data

    Returns: None
    """

    if session is None:
        session = connect_neo4j()

    update_time = str(datetime.now())
    cypher = """MATCH (l:User {{id:{}}})
                MERGE (u:User {{id:{}, id_str:'{}' }})
                WITH l, u MERGE (u)-[:FOLLOWS {create_dt:'{}']->(l)""".format(int(leader_id),
                                                                            int(follower_id),
                                                                            int(follower_id),
                                                                            update_time)
    session.run(cypher)
    return


def update_followers(leader_id, session=None, complete_ratio=0.99):
    """ Updates the known followers of the Twitter user of interest and adds nodes and edges for the new followers

    Args:
    leader_id : int, twitter id for the Twitter user being followed
    session: the Neo4J session where you want to send the data. When None, creates its own connection from config data
    complete_ratio: float between 0 and 1. Since the API is paged, the results are returned in groups of 5000.
    They should, but aren't necessarily returned in order. This parameter takes the list of returned followers
    and compares it to the list of known followers. When the ratio of the two is <= complete_ratio, the function
    stops gathering new followers.

    Returns:
    integer - number of followers added or 'error' if there is an error"""

    if session is None:
        session = connect_neo4j()

    existing_ids = get_existing_followers(leader_id, session)

    try:
        follower_ids = get_followers(leader_id, paged=True, existing_list=existing_ids)
        follower_ids = list(follower_ids)
        follower_ids = [int(i) for i in follower_ids]

        if len(follower_ids) > 0:
            for follower_id in follower_ids:
                add_user_and_edge(leader_id, int(follower_id), session)

            # Update the follower_update_hist table
            update_follower_hist(leader_id, session)

        return len(follower_ids)
    except:
        return "error"


def get_single_user(twitter_id=None, screen_name=None, by='twitter_id', session=None):
    """ Updates the known followers of the Twitter user of interest and adds nodes and edges for the new followers

    Args:
    twitter_id : int, Twitter user id for the Twitter user of interest
    screen_name: string, screen_name for the Twitter user of interest
    by: string, determines what value to use to look up the user.
    acceptable values: 'twitter_id', 'screen_name'. Default = 'twitter_id'
    session: the Neo4J session where you want to send the data. When None, creates its own connection from config data

    Returns:
    list - list of user attributes"""

    if by == 'twitter_id' and twitter_id is not None:
        cypher = "MATCH (u:User {{id:{}}}) RETURN u".format(twitter_id)
    elif by=='screen_name' and screen_name is not None:
        cypher = "MATCH (u:User {{screen_name:{}}}) RETURN u".format(screen_name)
    else:
        return "Missing twitter id or screen_name."

    if session is None:
        session = connect_neo4j()

    try:
        #send the query to Neo4J
        result = session.run(cypher)
        record = []

        #loop through the properties returned - there is no record count property
        #so if there is nothing found, this will return an empty dict
        for u in result:
            record.append(u['u'].properties)
        return record[0]
    except:
        return "Error executing cypher query"


def find_dehydrated_users(limit=1000, session=None):
    """ Query Neo4J for 'empty' nodes - indicated by a missing screen_name

    Args:
        limit (int): Limit the number of empty users to find. Default = 1000.
        session (neo4J session): A connected, Neo4J session. Default = None. When None, the function uses the
        default Neo4J config data to connect

    Returns:
        list: List of integers with the twitter user id for User nodes missing the screen_name attribute.
    """
    if session is None:
        session = connect_neo4j()
    cypher = "MATCH (n:User) WHERE n.hydrated=0 RETURN n.id LIMIT {:d}".format(limit)
    return cypher_id_list(cypher, session)


