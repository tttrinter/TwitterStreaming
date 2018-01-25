"""
Created on sometime in the second half of 2017

Tweet object to represent a tweet from Twitter

@author: tom trinter
"""

from . import TwitterUser


class Tweet():
    """Represents a Twitter tweet and all associated Twitter tweet attributes.

    Attributes:
           id(long int): unique Twitter tweet id
           id_str(string): string version of the tweet id, because sometimes the id is larger
                than acceptable for an integer. This will be set to the str(id) if left blank
           created_at(datetime): datetime the tweet was posted
           text(string): the text of the tweet
           user(TwitterUser): this is an Entweeties user object
           entities: this is a dictionary with lists of different entities. Keys are: hashtags, symbols, user_mentions
           favorite_count (int): number of times the tweet has been favorited
           favorited(bool): true or false if this tweet has been favorited
           geo(str): geo-location where the tweet was made
           in_reply_to_screen_name (str): screen name in the tweet was in reply to
           in_reply_to_status_id(int): id of the tweet this tweet was in reply to
           in_reply_to_user_id(int): user id of the twitter id whose tweet this tweet was in response to
           is_quote_status (bool): don't know what this is
           lang(str): default language of the user who tweeted
           place(str): not sure if this is a string or a place id. should reperesent the place where the tweet was tweeted
           retweet_count(int): number of times the tweet was retweeted
           retweeted(bool): has this tweet been retweeted?
           source(str): source of the tweet (mobile, web, etc...)
   """


    # tweet_properties = ['id', 'id_str', 'created_at', 'text', 'user',
    #                     'entities', 'favorite_count', 'favorited', 'geo',
    #                     'in_reply_to_screen_name', 'in_reply_to_status_id',
    #                     'in_reply_to_user_id', 'is_quote', 'lang', 'place',
    #                     'retweet_count', 'retweeted', 'source']

    tweet_attributes = {'id': 'int',
                        'id_str': 'string',
                        'created_at': 'string',
                        'text': 'string',
                        'user': 'TwitterUser',
                        'entities': 'list',
                        'favorite_count': 'int',
                        'favorited': 'bool',
                        'geo': 'dict',
                        'in_reply_to_screen_name': 'string',
                        'in_reply_to_status_id': 'int',
                        'in_reply_to_user_id': 'int',
                        'is_quote': 'bool',
                        'lang': 'string',
                        'place': 'dict',
                        'retweet_count': 'int',
                        'retweeted': 'bool',
                        'source': 'html'}


    def __init__(self, id, created_at, text, user, id_str=None,
                 entities=None, favorite_count=None, favorited=None, geo=None,
                 in_reply_to_screen_name=None, in_reply_to_status_id=None,
                 in_reply_to_user_id=None, is_quote=None,
                 lang=None, place=None, retweet_count=None, retweeted=False, source=None):
        self.id = id
        self.created_at = created_at
        self.text = text
        self.user = user
        self.id_str = id_str
        self.entities = entities
        self.favorite_count = favorite_count
        self.favorited = favorited
        self.geo = geo
        self.in_reply_to_screen_name = in_reply_to_screen_name
        self.in_reply_to_status_id = in_reply_to_status_id
        self.in_reply_to_user_id = in_reply_to_user_id
        self.is_quote = is_quote
        self.lang = lang
        self.place = place
        self.retweet_count = retweet_count
        self.retweeted = retweeted
        self.source = source

    @staticmethod
    def from_dict(data_dict)->'Tweet':
        """ Create a Tweet object from a dictionary
        Args:
            data_dict: dict containing Tweet data

        Returns:
            tweet object
        """
        min_keys = ['id','created_at','text','user']

        #make sure that the data passed in is a dictionary
        if type(data_dict) != dict:
            raise ValueError("Input must be a dictionary.")
            return -1

        #check to see if the minimum object keys are present
        elif not set(min_keys).issubset(list(data_dict.keys())):
            raise ValueError("Data dict is missing one of the minimum keys (id, created_at, text, or user")

        new_tweet = Tweet(id=data_dict['id'],
                          created_at=data_dict['created_at'],
                          text=data_dict['text'],
                          user=data_dict['user'])
        for key in data_dict.keys():
            if key in list(new_tweet.tweet_attributes.keys()):
                if key=='user':
                    user = TwitterUser.from_dict(data_dict['user'])
                    new_tweet.__setattr__('user', user)
                else:
                    new_tweet.__setattr__(key, data_dict[key])
        return new_tweet