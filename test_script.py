from TwitterFunctions import find_long_state, get_name
from TwitterRDS.RDSQueries import upsert_usernames
from subprocess import call
# import multiprocessing as mp
from Streaming.Streamer import run_topic_continuous

# found_state = find_long_state("born and raised in Las Vegas, Nevada")
# print(found_state)

# names = get_name("Tommy Trinter")
# upsert_usernames(713437986118090753, names)
# print(names)

run_inputs = {'topic_id': 3,
                's3_bucket': 'di-thrivent',
                's3_path': 'twitter/Life Events/Divorce/',
                'tweet_count': 100}

call_line = 'python start_stream.py 3 "di-thrivent" "twitter/Life Events/Divorce/" 10000'
call(call_line)