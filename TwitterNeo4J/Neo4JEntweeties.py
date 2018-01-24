"""
Created sometime in the Fall of 2017

This file contains all of the connections between "Entweeties" objects and the NEO4J graph database

This is not currently used, but may be resurrected if ever we go back to a graph DB.

@author: tom trinter
"""

from Entweeties import TwitterUser, Tweet
from TwitterNeo4J import Neo4JQueries
from datetime import datetime
from TwitterFunctions import parse_it

def get_user_data(u: TwitterUser, session=None):
    """Pulls data FROM Neo4J for a single user object and updates that user's properties

    :parameter u: an Entweety User object that has at a minimum the twitter_id populated
    :parameter session: optional Neo4J session. If None, the function will use the system defined parameters
    :returns returns the TwitterUser with populated values
    """
    twitter_id = u.id
    user_data = Neo4JQueries.get_single_user(twitter_id)

    #Get the list of defined properties
    user_attr = list(vars(u).keys())

    #Look for each property in the data, set the property if found
    for attr in user_attr:
        if attr in user_data.keys():
            setattr(u, attr, user_data[attr] )

    return u

def merge_user(user: TwitterUser, session=None):
    """MERGES data TO Neo4J for a single user object (create if missing, update if existing - based in id)
    :parameter user: an Entweety User object that has at a minimum the twitter_id populated. This method
    will not get any new data for the user. It will only merge whatever data is passed into the Neo4J session.
    :parameter session: an open Neo4J session. This can be used to send data to a different instance of Neo4J.
    Default will send the data into the production Neo4J instance
    :returns None
    """

    if session is None:
        session = Neo4JQueries.connect_neo4j()

    twitter_id = user.id
    #get all properties as a dict
    prop_dict = vars(user)

    exclude_vars = ['id']

    first_update = True
    for key in prop_dict:
        if prop_dict[key] is not None and key not in exclude_vars:
            key_type = user.user_attributes[key]
            if key_type == "string":
                #parse_id removes bad characters that could choke the loading process
                prop = parse_it(prop_dict[key])
                set_str = 'u.`{}`="{}"'.format(key, prop)
            elif key_type in ['bool', 'int']:
                prop = prop_dict[key]
                set_str = "u.`{}`={}".format(key, prop)

            if first_update:
                set_clause = "SET " + set_str
                first_update = False
            else:
                set_clause += ", " + set_str

    # Only update if there is something in the SET clause
    if first_update == False:
        # MERGE - create the node if it does not already exist
        # using toInt to make sure the id is an integer (and not a string)
        cypher = "MERGE (u:User {{id:toInt({})}})".format(twitter_id) + set_clause
        session.run(cypher)


def merge_tweet(tweet: Tweet, session=None):
    """MERGES data TO Neo4J for a tweet (create if missing, update if existing - based in id)
    :parameter tweet: an Entweety Tweet object that has at a minimum the id, text and user (shell with id minimum)
        populated. This method will not call the Twitter API - only save data that is present in the Tweet object.
    :parameter session: an open Neo4J session. This can be used to send data to a different instance of Neo4J.
    Default will send the data into the production Neo4J instance
    :returns None

    The Tweet object from Twitter has a lot of data besides the Tweet itself in it. This method will do all of the
    following, if the data is present in the Tweet object that is sent in:

    1. Load/Merge the User object
    2. Load the Tweet object and create a "TWEET" relationship between the Tweet and the User
    3. Load/Merge the "in-reply-to" User, if present and create "REPLY" relationship to the in-reply-to user
    4. Load/Merge any users in the entities['user_mentions'] list and Create "MENTIONS" relationships

    """

    if session is None:
        session = Neo4JQueries.connect_neo4j()

    # 1. Load/Merge the User Object
    merge_user(tweet.user, session)

    # First line of the Tweet cypher finds this new user
    cypher = "MATCH (u:User {{id:{:d}}}) ".format(tweet.user.id)

    #2. Load the Tweet Object
    exclude_vars = ['id', 'user', 'entities','source']

    #get all properties as a dict
    prop_dict = vars(tweet)

    first_update = True
    for key in prop_dict:
        if prop_dict[key] is not None and key not in exclude_vars:
            key_type = tweet.tweet_attributes[key]
            if key_type == "string":
                #parse_id removes bad characters that could choke the loading process
                prop = parse_it(prop_dict[key])
                set_str = 't.`{}`="{}"'.format(key, prop)
            elif key_type in ['bool', 'int']:
                prop = prop_dict[key]
                set_str = "t.`{}`={}".format(key, prop)
            # TODO: figure out what to do with "place" and "geo"

            if first_update:
                set_clause = " SET " + set_str
                first_update = False
            else:
                set_clause += ", " + set_str

    # Only update if there is something in the SET clause
    if first_update == False:
        # MERGE - create the tweet node if it does not already exist
        # using toInt to make sure the id is an integer (and not a string)
        cypher = cypher + "MERGE (t:Tweet {{id:toInt({:d})}})".format(tweet.id) + set_clause

        # Add the relationship to the Tweeter
        cypher = cypher + " WITH u, t MERGE (u)-[:TWEET]->(t)"
        session.run(cypher)

    # 3. Load/Merge the "in-reply-to" User, if present and create "REPLY" relationship to the in-reply-to user
    # Looking through 200 tweets, none of these had "In Reply To" populated - this code may never execute
    if not tweet.in_reply_to_user_id is None:
        reply_user_id = tweet.in_reply_to_user_id
        # intentionally excluding "screen_name" since missing screen_name identifies a dehydrated user
        user_inreply = TwitterUser(id=reply_user_id)
        merge_user(user_inreply, session)
        cypher = "MERGE (u:User {{id:{:d}}})<-[:REPLY]-(t:Tweet {{id:{:d}}})".format(reply_user_id, tweet.id)
        session.run(cypher)

    # 4. Load mentioned users and create "MENTIONS" relationship
    tweet_mentions(tweet, session=session)


def tweet_mentions(tweet: Tweet, session=None):
    """Adds users in the Tweet.Entities['user_mentions'] and creates a "MENTIONS" relationship to the tweet.
    :parameter tweet: an Entweety Tweet object.
    :parameter session: an open Neo4J session. This can be used to send data to a different instance of Neo4J.
    Default will send the data into the production Neo4J instance
    :returns None

    User mentions are found in the Tweet.entities['user_mentions']- which is a dict that has id and screen name
    of users mentioned. This process will create each of those users if they don't exist (merge) and then add
    a MENTIONS relationship between each mentioned user and the Tweet.
    """

    # Check first to see if there are any mentions to load
    if len(tweet.entities['user_mentions']) == 0:
        return

    if session is None:
        session = Neo4JQueries.connect_neo4j()

    mention_list = []

    # Load/Merge the mentioned users
    for user in tweet.entities['user_mentions']:
        # intentionally leaving out 'screen_name' since that's our trigger for dehydrated users
        mention_list.append(user['id'])
        user_mentioned = TwitterUser(id=user['id'],name=user['name'])
        merge_user(user_mentioned, session)

    # Create the relationships
    cypher = """MATCH (t:Tweet {{id:{:d}}}) 
                MATCH (u:User) WHERE u.`id` IN {} 
                WITH t, u
                MERGE (t)-[:MENTIONS]->(u)""".format(tweet.id, mention_list)
    session.run(cypher)
