import os
import struct
import re
import pandas as pd
from time import sleep
from datetime import datetime
import logging
from subprocess import call

from TwitterRDS import get_running_topics, dead_stream_log

# Set Constraints:
max_runtime = 8*60 # if the process runs longer than this time in minutes, kill and restart
min_memory_delta = 100 # if the memory doesn't change by this minimum amount in kbytes, kill and restart
check_interval = 1 * 60 # how frequently to check the processes in search of hanging streams
comp_name = os.environ['COMPUTERNAME']

def kill_processes(pid_list):
    for pid in pid_list:
        os.system("taskkill /f /pid {}".format(pid))

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
    tasks_df.loc[tasks_df.image_name == 'python.exe'].sort_values(['pid'])

    return tasks_df


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
    comp_df['mem_delta'] = comp_df['mem2'] - comp_df['mem1']
    comp_df['run_time'] = [(datetime.now() - x).total_seconds() / 60 for x in comp_df['rh_start_dt']]

    return comp_df


def restart_stream(inputs):
    call_line = 'python start_stream.py {} "{}" "{}" {}'.format(
        inputs['topic_id'],
        inputs['s3_bucket'],
        inputs['s3_path'],
        inputs['tweet_count']
    )
    call(call_line)

while True:
    # get topic data
    topic_df = get_running_topics()
    # get starting task_manager reference data
    if t1_df is None:
        if t2_df is None:
            t1_df = check_tasks()
        else:
            t1_df = check_tasks()

    sleep(check_interval)
    t2_df = get_running_topics()

    #compare data
    comp_df = compare_timepoints(topic_df, t1_df, t2_df)

    # check constraints
    pids_to_kill = []

    # min memory increment
    stalled = comp_df.loc[(comp_df['mem_delta']<min_memory_delta) and (comp_df['rh_computer_name']==comp_name)]['rh_pid'].tolist()
    pids_to_kill.extend(stalled)

    # running too long
    timedout = comp_df.loc[(comp_df['run_time']<max_runtime) and (comp_df['rh_computer_name']==comp_name)]['rh_pid'].tolist()
    pids_to_kill.extend(timedout)

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








