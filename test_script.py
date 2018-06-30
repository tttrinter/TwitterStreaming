from TwitterFunctions import find_long_state, get_name
from TwitterRDS.RDSQueries import upsert_usernames
from subprocess import Popen
import os
# import multiprocessing as mp
from Streaming.Streamer import run_topic_continuous

# found_state = find_long_state("born and raised in Las Vegas, Nevada")
# print(found_state)

# names = get_name("Tommy Trinter")
# upsert_usernames(713437986118090753, names)
# print(names)

# Testing starting a new process
inputs = {'topic_id': 6,
          's3_bucket': 'di-thrivent',
          's3_path': 'twitter/Life Events/Moving/' ,
           'tweet_count': 100}

call_line = 'python start_stream.py {} "{}" "{}" {}'.format(
    inputs['topic_id'],
    inputs['s3_bucket'],
    inputs['s3_path'],
    inputs['tweet_count']
)

my_env = os.environ.copy()
# DETACHED_PROCESS = 0x00000008
try:
    # Popen(call_line, shell=True, creationflags=DETACHED_PROCESS, env=my_env)
    Popen(call_line, shell=True, env=my_env)

except Exception as e:
    print(e)