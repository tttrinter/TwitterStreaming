import copy


class TwitterUser():
    """Represents a Twitter user and all associated Twitter user attributes.

       Attributes:
           id(long int): unique Twitter user id
           id_str(string): string version of the twitter_id, because sometimes the twitter_id is larger
                than acceptable for an integer. This will be set to the twitter_id if left blank
           name(string): Twitter user's name
           screen_name(string): screen name or twitter "handle"
           location(string): user's location - free-form text
           followers_count(int): number of other twitter users following this user
           friends_count(int): number of other twitter users that this user is following
           favourites_count(int): count of favorites
           description(string): free-form text user description
           geo_enabled(boolean): true/false if the user has enabled geo-location
           lang(string): user's chosen language setting
           statuses_count(int): total number of tweets
           time_zone(string): user's timezone
           created_at(datetime): date the user was created on Twitter
           verified(boolean): not sure what this means
           utc_offset(string): should correspond with the user's time_zone - showing the difference between
                local time and UTC.
           contributors_enabled(boolean): not sure what a contributor is
           listed_count(int): not sure what this means
           protected(boolean): shows if user is protected. If true, this limits what is visible to the public
           url(str): could be any URL, user selected and normally shortened to a https://t.co link
           hydrated: int - identifies if the user has been 'hydrated'.
                1 = hydrated,
                0 = not hydrated,
                -1 = error hydrating (maybe the user is protected) Using this so we don't keep trying
       """

    user_properties = ['id_str', 'name', 'screen_name', 'location', 'followers_count',
                       'friends_count', 'favourites_count', 'description', 'geo_enabled',
                       'lang', 'statuses_count', 'time_zone', 'created_at', 'verified', 'utc_offset',
                       'contributors_enabled', 'listed_count', 'protected', 'url']

    user_attributes = {'id': 'int',
                       'id_str': 'string',
                       'name': 'string',
                       'screen_name': 'string',
                       'location': 'string',
                       'followers_count': 'int',
                       'friends_count': 'int',
                       'favourites_count': 'int',
                       'description': 'string',
                       'geo_enabled': 'bool',
                       'lang': 'string',
                       'statuses_count': 'int',
                       'time_zone': 'string',
                       'created_at': 'string',
                       'verified': 'bool',
                       'utc_offset': 'int',
                       'contributors_enabled': 'bool',
                       'listed_count': 'int',
                       'protected': 'bool',
                       'url': 'string',
                       'hydrated': 'int'}

    def __init__(self, id, id_str=None, name=None, screen_name=None, location=None,
                 followers_count=None, friends_count=None, favourites_count=None,
                 description=None, geo_enabled=None, lang=None, statuses_count=None,
                 time_zone=None,created_at=None, verified=None, utc_offset=None,
                 contributors_enabled=None, listed_count=None, protected=None, url=None, hydrated=0):

        self.id = id
        if id_str is None:
            self.id_str = str(id)
        else:
            self.id_str = id_str

        self.name = name
        self.screen_name = screen_name
        self.location = location
        self.followers_count=followers_count
        self.friends_count = friends_count
        self.favourites_count = favourites_count
        self.description = description
        self.geo_enabled = geo_enabled
        self.lang = lang
        self.statuses_count = statuses_count
        self.time_zone = time_zone
        self.created_at = created_at
        self.verified = verified
        self.utc_offset = utc_offset
        self.contributors_enabled = contributors_enabled
        self.listed_count = listed_count
        self.protected = protected
        self.url = url
        self.hydrated = hydrated

    def copy(self):
        return copy.copy(self)


    @staticmethod
    def from_dict(data_dict)->'TwitterUser':
        new_user = TwitterUser(id=data_dict['id'])
        for key in data_dict.keys():
            if key in list(new_user.user_attributes.keys()):
                new_user.__setattr__(key, data_dict[key])
        return new_user


class TwitterUser2():
    """Represents a Twitter user and all associated Twitter user attributes.

       Attributes:
           twitter_id(long int): unique Twitter user id
           Key-word arguments:
               id_str(string): string version of the twitter_id, because sometimes the twitter_id is larger
                    than acceptable for an integer. This will be set to the twitter_id if left blank
               name(string): Twitter user's name
               screen_name(string): screen name or twitter "handle"
               location(string): user's location - free-form text
               followers_count(int): number of other twitter users following this user
               friends_count(int): number of other twitter users that this user is following
               favourites_count(int): count of favorites
               description(string): free-form text user description
               geo_enabled(boolean): true/false if the user has enabled geo-location
               lang(string): user's chosen language setting
               statuses_count(int): total number of tweets
               time_zone(string): user's timezone
               created_at(datetime): date the user was created on Twitter
               verified(boolean): not sure what this means
               utc_offset(string): should correspond with the user's time_zone - showing the difference between
                    local time and UTC.
               contributors_enabled(boolean): not sure what a contributor is
               listed_count(int): not sure what this means
               protected(boolean): shows if user is protected. If true, this limits what is visible to the public
               url(str): could be any URL, user selected and normally shortened to a https://t.co link
       """

    def __init__(self, twitter_id, **kwargs):
        self.twitter_id = twitter_id

        acceptable_kwargs = ['id_str', 'name', 'screen_name','location','followers_count',
                             'friends_count','favourites_count','description','geo_enabled',
                             'lang','statuses_count','time_zone','created_at','verified','utc_offset',
                             'contributors_enabled','listed_count','protected','url']

        for arg in acceptable_kwargs :
            setattr(self, arg, kwargs.get(arg, None))

        if self.id_str is None:
            self.id_str = str(twitter_id)




