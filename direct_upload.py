## This script acts as a quick way to upload data to Snowflake from a local Excel file. Index.py should
## be used to upload to snowflake, but it takes a long time to run. If something is off in snowflake and the data just needs
## to be reuploaded, this script can be used to quickly upload the data to Snowflake.

import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from datetime import datetime
import time

start = time.time()
# credentials = {
#     	'account' : 'pr43333.canada-central.azure',
#     	'user' :   'timothy_bender@worldvision.ca',
#     	'authenticator' : 'externalbrowser',
#         'role' : 'DATA_INSIGHTS',
#         'warehouse' : 'DATA_INSIGHTS_WH',
#         'database'  : 'ANALYTICS_SANDBOX',
#             # 'database'  : 'IVS_DPMS_DEV',
#         'schema'    : 'INSIGHTS' ,
#     	'authenticator' : 'externalbrowser'
#     	}
credentials = {'account' : 'pr43333.canada-central.azure',
    	'user' :   'timothy_bender@worldvision.ca',
    	'authenticator' : 'externalbrowser',
        'role' : 'sf_ivs_dpms_dev_user',
        'warehouse' : 'ivs_dpms_nonprod_user_wh',
        'database'  : 'ivs_dpms_dev',
        'schema'    : 'analytics' ,
    	'authenticator' : 'externalbrowser'}
session = Session.builder.configs(credentials).create()

print('Reading Excel data...')

def clean_df(excelfile):
    #read the file
    df = pd.read_excel(excelfile)
    #we don't need this column
    df.drop(columns=['Unnamed: 0'], inplace=True)
    #change True/False from string value to boolean
    df['IS_PEOPLE'] = df['IS_PEOPLE'].map({'True': True, 'False': False})

    #use new time to populate datefields
    nowtime = datetime.now()
    df['INSERT_DATE'] = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    df['UPDATE_DATE'] = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    
    return df


agg_df = clean_df('Interim/agg_results.xlsx')
ind_df = clean_df('Interim/indicator_results.xlsx')

print('Uploading data to Snowflake...')

#print to snowflake
session.sql('TRUNCATE table if exists ANLT_IA_AGG_RESULTS;').collect()
session.write_pandas(df = agg_df, table_name='ANLT_IA_AGG_RESULTS', overwrite=False, auto_create_table=False)

session.sql('TRUNCATE table if exists ANLT_IA_INDICATOR_RESULTS;').collect()
session.write_pandas(df = ind_df, table_name='ANLT_IA_INDICATOR_RESULTS', overwrite=False, auto_create_table=False)


print('Tables uploaded successfully!')
end = time.time()
print(f'Time to run script: {(end - start)/60} minutes')

session.close()