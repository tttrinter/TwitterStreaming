import os
import struct
import re
import pandas as pd
from time import sleep
from datetime import datetime
import logging
from subprocess import Popen
from TwitterRDS.RDSQueries import get_running_topics, dead_stream_log, get_topic_rundata, dead_stream_log_bytopic

# Set Constraints:
max_runtime = 8*60 # if the process runs longer than this time in minutes, kill and restart
min_memory_delta = 100 # if the memory doesn't change by this minimum amount in kbytes, kill and restart
check_interval = 5 * 60 # how frequently to check the processes in search of hanging streams
comp_name = os.environ['COMPUTERNAME']
t1_df = None
t2_df = None
STREAM_LIMIT = 3

def kill_processes(pid_list):
    for pid in pid_list:
        os.system("taskkill /f /pid {}".format(int(pid)))


def check_tasks():
    # set fixed column width format for task list reading
    fieldwidths = (25, -1, 8, -1, 16, -1, 11, -1, 12)  # negative widths represent ignored padding fields
    fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                         for fw in fieldwidths)
    fieldstruct = struct.Struct(fmtstring)
    parse = fieldstruct.unpack_from

    # read tasks from task list
    tasks = os.popen('tasklist').readlines()

    # delete header row and --- row
    del tasks[0]
    del tasks[1]

    # parse the task list into a DataFrame
    tasks = [parse(bytes(x, 'utf-8')) for x in tasks]
    task_list = [[x.strip().decode("utf-8") for x in list(task)] for task in tasks]
    tasks_df = pd.DataFrame(task_list[1:], columns=['image_name', 'pid', 'session_name', 'session_num', 'mem_usage'])

    # convert mem-usage to value
    tasks_df['mem_usage_val'] = [int(re.sub(',', '', x[:-2])) for x in tasks_df['mem_usage']]
    tasks_df['pid'] = tasks_df['pid'].astype('int')
    tasks_df = tasks_df.loc[tasks_df.image_name == 'python.exe'].sort_values(['pid'])

    return tasks_df


def check_running(set_df):
    # topics running or stalled
    running_df = get_running_topics()
    # Each stream seems to use 25% CPU. We'll limit the number of streams running per CPU to 3
    running_this_computer = len(running_df.loc[running_df.rh_computer_name == comp_name])

    # Topics to Start
    if running_df is not None:
        topics_to_start = list(set(set_df.loc[set_df['tp_on_off']==True]['tp_id']) - set(running_df.tp_id))
    else:
        topics_to_start = list(set_df.loc[set_df['tp_on_off']==True]['tp_id'])

    # Topics to Stop - on this computer ONLY
    topics_to_stop = list(set(set_df.loc[set_df['tp_on_off'] == False]['tp_id']) &
                          set(running_df.loc[running_df.rh_computer_name == comp_name, 'tp_id']))

    return running_df, running_this_computer, topics_to_start, topics_to_stop

def compare_timepoints(topic_df, t1_df, t2_df):
    # merge with start values
    comp_df = topic_df.merge(t1_df[['pid', 'mem_usage_val']], left_on='rh_pid', right_on='pid', how='left')
    comp_df.rename(columns={"mem_usage_val": "mem1"}, inplace=True)

    # merge with end values
    comp_df = comp_df.merge(t2_df[['pid', 'mem_usage_val']], left_on='rh_pid', right_on='pid', how='left')
    comp_df.rename(columns={"mem_usage_val": "mem2"}, inplace=True)

    # lose extra columns
    comp_df.drop(['pid_x', 'pid_y'], axis=1, inplace=True)

    # calculate constraints
    comp_df.fillna(0, inplace=True)
    # create and initialize run_time column
    comp_df['run_time'] = 0
    # where there is a real start date, use it to calculate run_time
    comp_df['run_time'] = [(datetime.now() - x).total_seconds() / 60 for x in comp_df['rh_start_dt']]
    # evaluate the incremental memory used since last check - to see if the t
    comp_df['mem_delta'] = comp_df['mem2'] - comp_df['mem1']

    return comp_df


def restart_stream(inputs):
    my_env = os.environ.copy()
    DETACHED_PROCESS = 0x00000008
    call_line = 'python start_stream.py {} "{}" "{}" {}'.format(
        inputs['topic_id'],
        inputs['s3_bucket'],
        inputs['s3_path'],
        inputs['tweet_count']
    )
    Popen(call_line, shell=True, creationflags=DETACHED_PROCESS, env=my_env)
    # Popen(call_line, shell=True)

# Set up log
logging.basicConfig(filename='watchdog.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

logging.info("Starting watchdog.")
while True:
    # topics set to run
    set_df = get_topic_rundata()

    # topics running or stalled
    running_df, running_this_computer, topics_to_start, topics_to_stop = check_running(set_df)
    # start any streams not already running
    # if running_df is not None:
    #     topics_to_start = list(set(set_df.loc[set_df['tp_on_off']==True]['tp_id']) - set(running_df.tp_id))
    # else:
    #     topics_to_start = list(set_df.loc[set_df['tp_on_off']==True]['tp_id'])

    # for tp_id in topics_to_start:
    while (len(topics_to_start) > 0 and running_this_computer < STREAM_LIMIT) :
        tp_id = topics_to_start[0] # get the first in the list (list is updated at end of this loop)
        row = set_df.loc[set_df.tp_id==tp_id].iloc[0]
        tweet_count = row['rh_tweet_count']
        s3_path = 'twitter/Life Events/{}/'.format(row['tp_name'])
        run_inputs = {'topic_id': tp_id,
                      's3_bucket': 'di-thrivent',
                      's3_path': s3_path,
                      'tweet_count': tweet_count}
        restart_stream(run_inputs)
        # sleeping for 20 seconds so that it has time to update the user before starting a new stream
        sleep(20)

        # Update run data to pick up new starts/stops
        running_df, running_this_computer, topics_to_start, topics_to_stop = check_running(set_df)

    # kill any topics running that are NOT set to run
    if len(topics_to_stop) > 0:
        kill_processes(topics_to_stop)
        for tpid in topics_to_stop:
            dead_stream_log_bytopic(tpid, comp_name)

    # status of running topics
    t1_df = check_tasks()
    sleep(check_interval)
    t2_df = check_tasks()

    #compare data
    comp_df = compare_timepoints(running_df, t1_df, t2_df)

    # check constraints
    pids_to_kill = []

    # min memory increment
    stalled = comp_df.loc[(comp_df['mem_delta'] < min_memory_delta) & (str(comp_df['rh_computer_name']) == comp_name)]['rh_pid'].tolist()
    pids_to_kill.extend(stalled)

    # running too long
    timedout = comp_df.loc[(comp_df['run_time']>max_runtime) & (comp_df['rh_computer_name']==comp_name)]['rh_pid'].tolist()
    pids_to_kill.extend(timedout)
    pids_to_kill = list(set(pids_to_kill))

    # kill processes
    kill_processes(pids_to_kill)

    # restart streams
    for pid in pids_to_kill:
        row = comp_df.loc[comp_df['rh_pid'] == pid].iloc[0]
        tp_id = row['tp_id']
        tweet_count = row['rh_tweet_count']
        s3_path = 'twitter/Life Events/{}/'.format(row['tp_name'])
        run_inputs = {'topic_id': tp_id,
                's3_bucket': 'di-thrivent',
                's3_path': s3_path,
                'tweet_count': tweet_count}
        dead_stream_log(pid, comp_name)
        restart_stream(run_inputs)
        # sleeping for 20 seconds so that it has time to update the user before starting a new stream
        sleep(20)







