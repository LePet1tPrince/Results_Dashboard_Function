## This script acts as a quick way to upload data to Snowflake from a local Excel file. Index.py should
## be used to upload to snowflake, but it takes a long time to run. If something is off in snowflake and the data just needs
## to be reuploaded, this script can be used to quickly upload the data to Snowflake.

import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from datetime import datetime
import time

def archive_table(sess, table_name):
    #pull the current results
    prev_results = sess.sql(f'select * from {table_name};').to_pandas()
    prev_results['ARCHIVE_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #append those results to the archive table
    prev_results['INSERT_DATE'] = pd.to_datetime(prev_results['INSERT_DATE']).dt.tz_localize('UTC')
    prev_results['UPDATE_DATE'] = pd.to_datetime(prev_results['UPDATE_DATE']).dt.tz_localize('UTC')
    
    sess.write_pandas(df = prev_results, table_name = f'{table_name}_ARCHIVE', overwrite=False, auto_create_table=False)
    #if there are more than 10 entries to the archive table, delete the oldest one.
    sess.sql(f'''DELETE FROM {table_name}_ARCHIVE WHERE ARCHIVE_DATE IN (
    -- Select all rows that are not in the 10 newest archive dates. delete them
    SELECT DISTINCT ARCHIVE_DATE
    FROM {table_name}_ARCHIVE
    WHERE ARCHIVE_DATE NOT IN (
        -- Find the 10 newest archive dates. Keep those
            SELECT DISTINCT ARCHIVE_DATE
            FROM {table_name}_ARCHIVE
            ORDER BY ARCHIVE_DATE DESC
            LIMIT 10
        )
    ORDER BY ARCHIVE_DATE DESC
    );''').collect()
    #truncate the main table
    print(f'{table_name} table archived')
    return

def main():
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

    archive_table(session, 'ANLT_IA_AGG_RESULTS')
    session.sql('TRUNCATE table if exists ANLT_IA_AGG_RESULTS;').collect()
    session.write_pandas(df = agg_df, table_name='ANLT_IA_AGG_RESULTS', overwrite=False, auto_create_table=False)

    archive_table(session, 'ANLT_IA_INDICATOR_RESULTS')
    session.sql('TRUNCATE table if exists ANLT_IA_INDICATOR_RESULTS;').collect()
    session.write_pandas(df = ind_df, table_name='ANLT_IA_INDICATOR_RESULTS', overwrite=False, auto_create_table=False)


    print('Tables uploaded successfully!')
    end = time.time()
    print(f'Time to run script: {(end - start)/60} minutes')

    session.close()

if __name__ == '__main__':
    main()