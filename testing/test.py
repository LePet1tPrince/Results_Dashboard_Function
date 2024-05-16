import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'
import time
import math
from Reach import REACH
from Meta import META


def query_session(query):
    """This function takes a query and returns a pandas dataframe from the Snowflake session."""
    sql_table = session.sql(query)
    df = sql_table.to_pandas()
    df.columns= df.columns.str.lower()
    return df

user = 'timothy_bender@worldvision.ca'


read_credentials = {
    'account' : 'pr43333.canada-central.azure',
    'user' :   user,
    'authenticator' : 'externalbrowser',
    'role' : 'DATA_INSIGHTS',
    'warehouse' : 'DATA_INSIGHTS_WH',
    'database'  : 'ANALYTICS_SANDBOX',
        # 'database'  : 'IVS_DPMS_DEV',
    'schema'    : 'INSIGHTS' ,
    'authenticator' : 'externalbrowser'
    }
# write_credentials = { #ANALYTICS SANDBOX
# 	'account' : 'pr43333.canada-central.azure',
# 	'user' :   user,
# 	'authenticator' : 'externalbrowser',
#     'role' : 'DATA_INSIGHTS',
#     'warehouse' : 'DATA_INSIGHTS_WH',
#     'database'  : 'ANALYTICS_SANDBOX',
#     'schema'    : 'INSIGHTS' ,
# 	'authenticator' : 'externalbrowser'
# 	}
#the write_credentials need to have write permissions to whatever database you want to output to.
write_credentials = { #DPMS DEV
    'account' : 'pr43333.canada-central.azure',
    'user' :   user,
    'authenticator' : 'externalbrowser',
    'role' : 'sf_ivs_dpms_dev_user',
    'warehouse' : 'ivs_dpms_nonprod_user_wh',
    'database'  : 'ivs_dpms_DEV',
    'schema'    : 'analytics' ,
    'authenticator' : 'externalbrowser'}

session = Session.builder.configs(read_credentials).create()

test_df = query_session('select * from HUB_TABLE;')
print('test 1 worked')



session.use_database(write_credentials['database'])
session.use_schema(write_credentials['schema'])
session.use_role(write_credentials['role'])
session.use_warehouse(write_credentials['warehouse'])  

test_df = query_session('select * from HUB_TABLE;')
print('test 2 worked')