from TwitterFunctions import find_long_state, get_name
from TwitterRDS.RDSQueries import upsert_usernames

# found_state = find_long_state("born and raised in Las Vegas, Nevada")
# print(found_state)

names = get_name("Thomas Trinter")
upsert_usernames(713437986118090753, names)