"""
Created sometime in the Spring of 2016

Contains post-processing functions for parsing and cleaning up Twitter data before saving.
It also has functions of name matching and state matching. I've commented some of these
out because of file access errors that I was unable to resolve.

All that is left is "parse_it"

@author: mostly Ben Wood, some additions by Tom Trinter
"""

import pandas as pd
import re
import os
# import sys
import string
from nltk.tokenize import TweetTokenizer

root = os.path.expanduser('~')
# filepath = "D:\\OneDrive\\Documents\\Data Insights\\Clients\\Thrivent\\Twitter\\TwitterFunctions\\"
filepath = ".\\data_files\\"
# print(sys.path[0])

def parse_it(tweetText):
    """
    Removes bad characters that could cause problems with saving or parsing strings
    :param tweetText: the string to be parsed
    :return: a 'clean' string with the bad characters removed
    """

    # Remove all pipes and double-quotes
    if type(tweetText) == bytes:
        tweetText = tweetText.decode('utf-8')

    tweet_text = tweetText.translate(str.maketrans('|",','   '))

    # Remove new lines
    tweet_text = tweet_text.replace('\r', '').replace('\n', '')

    # Remove blank escape characters (TTT addition 10/13/17)
    tweet_text = tweet_text.replace('\ ', ' ')

    # Remove single quotes
    tweet_text = tweet_text.replace("'", " ")

    # remove non-utf8 characters, encode back to ascii and ignore everything else
    tweet_text = tweet_text.encode('ascii', 'ignore')
    tweet_text = tweet_text.decode('utf-8', 'strict')

    return tweet_text

# STATE IDENTIFICATION
# identify the state if possible
# Upload list of states and abbreviations
# This file is giving me troubles - cannot find it unless running from this directory! Need to fix!
state_file = 'state_codes.csv'

states = pd.read_csv(filepath + state_file)

# 1. Look for comma space and two capital letters as the common state identifyer


def clean_text_col(df, text_col, new_col, blist=False):
    """
    Cleans a pandas dataframe text column removing punctuation, hyperlinks, and non-ascii chars and splits
    the field into a list, adding the new column to the dataframe.

    :param df: pandas dataframe to modify
    :param text_col: string, name of the text column to clean
    :param new_col: string, name of the new column where the list of terms will go
    :param blist: boolean, return string or list. When true, results are split by spaces and returned as a list
    :return: returns the dataframe passed in with the addition of the new, cleaned column
    """

    table = str.maketrans({key: None for key in string.punctuation})
    df[new_col] = [str(s).translate(table) for s in df[text_col]]

    # Remove non ascii characters
    df[new_col] = [str(x).encode('ascii', 'ignore').decode('ascii') for x in df[new_col]]

    # Remove carriage returns
    df[new_col] = [str(x).replace("\r", '').replace('\n', '') for x in df[new_col]]

    # Remove hyperlinks and split into words
    df[new_col] = [re.sub(r'http\S+', '', x.lower()) for x in df[new_col]]

    # Create list if blist=True
    if blist:
        df[new_col] = [x.split() for x in df[new_col]]

    return df


def find_state_abrv(loc):
    if len(loc) == 2:
        loc = " " + loc

    comma = re.search(r'[ ,.;:][A-Z]{2}\b',loc)
    if comma is None:
        state = ''
    else:
        start = comma.start()
        state = loc[start+1:start+4]
        state = state.upper()
    return state


# Since the states are written out in the Google location list, we'll find the written out state names
# Start with the 2 letter abbreviation first - then look for full state name if no abbrev. found


def find_long_state(loc):
    foundstate = ""

    # look for 2 letter state abbreviation
    state_abrv = find_state_abrv(loc)
    if len(state_abrv) > 0:
        foundstate = states.loc[states['Abbreviation'] == state_abrv,'State']
        if len(foundstate) == 0:
            foundstate = ""
        else:
            foundstate = foundstate.item()

    # Look for the full state name if no abbreviation found
    if foundstate == "":
        # Look for West Virginia first -since those always result to 'Virginia" otherwise
        if re.search('West Virginia', loc, re.IGNORECASE):
            foundstate = 'West Virginia'
        else:
            # Check all state names
            for state in states['State']:
                if re.search(state, loc, re.IGNORECASE):
                    foundstate = state
                    break

    return foundstate


# Fixing Names
# Get the data first
first_names = []
for n in open(filepath+'first_names.txt', 'r'):
    first_names.append(n.lower().replace('\n', ''))

last_names = []
for n in open(filepath+'last_names.txt', 'r'):
    last_names.append(n.lower().replace('\n', ''))


def get_name(tweet_name):
    tknzr = TweetTokenizer()

    names = {'first': None,
             'last': None,
             'full': None,
             'first_match': False,
             'last_match': False
             }

    try:
        # twitter_name = str(tweet_name)
        twitter_name = re.sub('["]', '', tweet_name)
    except:
        print(tweet_name)
        return names

    twitter_name_tokens = tknzr.tokenize(twitter_name.lower())

    if len(twitter_name_tokens) != 1:

        for nametok in twitter_name_tokens:

            if not names['first_match']:
                if nametok in first_names:
                    names['first'] = nametok
                    names['first_match'] = True

            elif not names['last_match']:
                if nametok in last_names:
                    names['last'] = nametok
                    names['last_match'] = True

        if names['first_match'] and names['last_match']:
            names['full'] = names['first'] + ' ' + names['last']

        if not names['full'] and len(twitter_name_tokens) == 2:
            names['first'] = twitter_name_tokens[0]
            names['last'] = twitter_name_tokens[1]
            names['full'] = names['first'] + ' ' + names['last']

    return names
