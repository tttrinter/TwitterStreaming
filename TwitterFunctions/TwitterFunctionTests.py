import sqlite3
import pandas as pd
from TwitterFunctions.TwitterFunctions import get_users

#Connect to SQLlite DB to get some test data:
# data_path = 'D:\\AQ\\Clients\\Thrivent\\'
# conn = sqlite3.connect(data_path + 'AQTwitter.db')

# test_data = pd.read_sql("SELECT * FROM users LIMIT 250", con=conn)
# test_data.head()

# test id_list - the one in the middle no longer exists
id_list = [786786011904745472,
834092397684482048,
 34523264]
# user_list = test_data.id.tolist()
# user_data = get_users(id_list)
# print(user_data.loc[786786011904745472])
print(user_data)