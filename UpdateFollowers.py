"""
Created in December of 2017

This script will take a list of Twitter Ids and update their follower lists.
It is intended for scheduling or running from the command line

If passed a list of Twitter Ids, it will pull all of the followers for each of those users.
If NOT passed a list of Twitter Ids, it will get the ids for all Christian leaders and colleges

@author: tom trinter
"""


import sys
import pandas as pd
import logging
from datetime import datetime
from TwitterFunctions.TwitterFunctions import get_followers
from TwitterRDS.RDSQueries import get_prior_user_list, update_follower_hist, get_leader_indicator_ids
from TwitterRDS import RDSconfig

source = 'postgres'
# source = 'sqllite'

print("Starting UpdateFollowers")
# Set up log
logging.basicConfig(filename='UpdateFollowers.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)


def update_followers(leader_id_list: list=None, complete_ratio=0.99):
    """
    Gathers user ids that follow each "leader" in the leader_id_list and saves to the DB
    Also saves the date-time that the follower list was updated for that user.

    Args:
        leader_id_list: list of Twitter users ids whose followers we want to gather
        complete_ratio: decimal, 0-1 - the results from the Twitter API are expected to come in reverse
        order with new followers returned first. That, however is not guaranteed by the API. The API only returns
        5000 followers at a time. The returned followers are compared with the list of known followers for that
        user. When the ratio of found:known-users is greater than the "complete_ratio", the process will exit.

    Returns:
        a list of ids. Returns an empty list if there were no ids returned from the API call
    """

    con = RDSconfig.get_connection(source)
    engine = RDSconfig.get_sqlalchemy_engine(source)

    i = 0
    for twitter_id in leader_id_list:
        i += 1
        existing_ids = get_prior_user_list(twitter_id, con)
        msg = "{}: {}: twitter_id = {}".format(i, datetime.strftime(datetime.now(), '%H:%M:%S'), twitter_id)
        if i%10 == 0:
            print(msg)
        logging.info(msg)

        df_one_leader = pd.DataFrame(columns=['tf_user_id', 'tf_follower_id'], dtype=str)

        try:
            follower_ids = get_followers(twitter_id,
                                         paged=True,
                                         existing_list=existing_ids,
                                         complete_ratio=complete_ratio)
            follower_ids = list(follower_ids)
            # including the list of existing_ids will reduce the results to only new values and assume

            if len(follower_ids) > 0:
                # reset the data frame
                df_one_leader['tf_follower_id'] = follower_ids
                df_one_leader['tf_user_id'] = twitter_id

                # Remove known users - moved to get_followers function
                df_one_leader = df_one_leader[~df_one_leader['tf_follower_id'].isin(existing_ids)]

                # Writer new users back to the "user_followers" table
                df_one_leader.to_sql('user_followers', con=engine, if_exists='append', index=False)
                new_record_count = len(df_one_leader)

                # Update the follower_update_hist table
                update_follower_hist(twitter_id, new_record_count, con)
        except Exception as e:
            logging.error(e)
            pass

if __name__ == "__main__":
    # List of leaders to update
    if len(sys.argv)>1:
        id_list = eval(sys.argv[1])
    else:
        # default to get the Christian and College followers - that's in the function
        id_list = get_leader_indicator_ids()

    # Complete Ratio
    if len(sys.argv) > 2:
        complete_ratio = float(sys.argv[2])
    else:
        complete_ratio = None

    # Call the process to loop through the list of ids and update them
    update_followers(id_list, complete_ratio)