"""
Created sometime in the Fall of 2017

1. Finds follower records in the database without matching user records.
This happens as a result of the update-follower process that finds new followers
for users of interest.

2. For followers missing user records, this calls Twitter via TwitterFunctions to hydrate those users

3. Saves the hydrated user to the users table

@author: tom trinter
"""


import logging
from numpy import ceil
from datetime import datetime
from TwitterFunctions import get_users
from TwitterFunctions import parse_it
from TwitterRDS.RDSQueries import get_dehydrated_followers
from TwitterRDS import RDSconfig

# Set up log
logging.basicConfig(filename='HydrateFollowers.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

source = 'postgres'
# source = 'sqllite'


engine = RDSconfig.get_sqlalchemy_engine(source)

def hydrate_followers():

    missing_users_count = 10000
    min_user = 0
    success = 0
    error = 0

    while missing_users_count > 1000:
        new_users = get_dehydrated_followers(minid=min_user)
        min_user = new_users[-1]
        iterations = int(ceil(len(new_users) / 100))
        # print('iterations:{}'.format(iterations))
        missing_users_count = len(new_users)

        for i in range(iterations):
            #     clear_output()
            start_index = i * 100
            end_index = start_index + 100

            # print some user feedback to keep track of where the process is
            if i % 10 == 0:
                print("index: " + str(i) + "; time: " + datetime.strftime(datetime.now(), '%H:%M:%S') +
                      "; Range:" + str(start_index) + ":" + str(end_index) +
                      " Successes: " + str(success) + ", Errors: " + str(error))
            if i % 100 == 0:
                msg = "index: {}, Range: {}-{}, Successes: {}, Errors:{}.".format(i,
                                                                                  start_index, end_index,
                                                                                  success, error)
                logging.info(msg)

            userlist = new_users[start_index:end_index]
            #         print(len(userlist))

            try:
                user_data = get_users(userlist)
                user_data['description'] = user_data['description'].apply(lambda x: parse_it(x))
                user_data['name'] = user_data['name'].apply(lambda x: parse_it(x))
                user_data['screen_name'] = user_data['screen_name'].apply(lambda x: parse_it(x))
                user_data['location'] = user_data['location'].apply(lambda x: parse_it(x))

                # user_data['state'] = user_data['location'].apply(find_long_state)

                user_data.to_sql('users', con=engine, if_exists='append')
                success += 1
            except Exception as e:
                error += 1
                msg = "Error with {}: {}".format(i, e)
                logging.error(msg)
                pass

