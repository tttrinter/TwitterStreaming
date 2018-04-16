from Entweeties import Tweet
from TwitterNeo4J.Neo4JEntweeties import get_user_data, merge_user, merge_tweet
from TwitterFunctions.TwitterFunctions import hydrate_user
import ast

# Test creating objects
# twitter_id = 2
# kwargs = {'contributors_enabled': None,
#  'created_at': None,
#  'description': "This is a test user, created from the EnTweeties user class",
#  'favourites_count': 0,
#  'followers_count': 0,
#  'friends_count': 0,
#  'geo_enabled': 0,
#  'id_str': '2',
#  'lang': 'en',
#  'listed_count': None,
#  'location': 'Avondoale, PA',
#  'name': 'Test User',
#  'protected': None,
#  'screen_name': 'Test User',
#  # 'state': 'Pennsylvania',
#  'statuses_count': 0,
#  'time_zone': '',
#  'url': None,
#  'utc_offset': None,
#  'verified': None}
#
# # kwargs={}
#
# new_user = TwitterUser(twitter_id, **kwargs)
#
# # get_user_data(new_user)
# merge_user_data(new_user)
#
# new_user2 = TwitterUser(3)
# merge_user_data(new_user2)

data_dir = 'D:/AQ/Clients/Thrivent/Life Events/Graduation/'
tweet_file = '20170522_lifeTweetsAllison.txt'

# Testing Tweets
#Import some data
i=0
tweets=[]
startline =7300
endline = 7400

with open(data_dir + tweet_file, encoding="UTF-8") as f:
    for line in f:
        if i > startline:
            tweets.append(ast.literal_eval(line))
        if i == endline:
            break
        i += 1

f.close()

#Create the tweet object
new_tweet = Tweet.from_dict(tweets[33])

# Send the data to Neo4J
merge_tweet(new_tweet)

#
# #Save the user
# merge_user_data(new_tweet.user)

# Test getting single user data from Neo4J
# userid = 713437986118090753
# user = TwitterUser(id=userid)

# # Get data from Twitter API
# hydrate_user(user)
#
# # Write data to Neo4J
# merge_user_data(user)
# print(user.screen_name)