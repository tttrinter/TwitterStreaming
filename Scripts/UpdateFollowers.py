### This is process will take a list of Twitter Ids and update their follower lists


import sys
import pandas as pd
import logging
from datetime import datetime
from TwitterFunctions.AQTwitterFunctions import get_followers
from TwitterRDS.RDSQueries import get_prior_user_list, update_follower_hist, get_leader_indicator_ids
from TwitterRDS import RDSconfig

# source = 'postgres'
source = 'sqllite'

# Set up log
logging.basicConfig(filename='UpdateFollowers.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)


def update_followers(leader_id_list: list=None, complete_ratio=0.99):
    con = RDSconfig.get_connection(source)
    engine = RDSconfig.get_sqlalchemy_engine(source)

    i = 0
    for twitter_id in leader_id_list:
        i += 1
        existing_ids = get_prior_user_list(twitter_id, con)
        msg = "{}: {}: twitter_id = {}".format(i, datetime.strftime(datetime.now(), '%H:%M:%S'), twitter_id)
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

                # Writer new users back to the "college_followers" table
                df_one_leader.to_sql('user_followers', con=engine, if_exists='append')
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