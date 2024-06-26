###
###
### This script is used to revert the ANLT_IA_AGG_RESULTS and ANLT_IA_INDICATOR_RESULTS tables in Snowflake to a previous date.
### The user will be prompted to select which table they would like to revert, and then select the date they would like to revert to.
### The script will then truncate the current table and replace it with the selected date's data from the archive table.


import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from datetime import datetime



def main():
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
    write_credentials = { #DPMS DEV
        'account' : 'pr43333.canada-central.azure',
    	'user' :   user,
    	'authenticator' : 'externalbrowser',
        'role' : 'sf_ivs_dpms_dev_user',
        'warehouse' : 'ivs_dpms_nonprod_user_wh',
        'database'  : 'ivs_dpms_DEV',
        'schema'    : 'analytics' ,
    	'authenticator' : 'externalbrowser'}
    
    session = Session.builder.configs(write_credentials).create()
    
    def unarchive_results(session, table_name):
        #pull the current results
        print(f'Fetching archive dates for {table_name}...')
        archive_results = session.sql(f'select * from {table_name}_ARCHIVE;').to_pandas()
        archive_dates = sorted(archive_results['ARCHIVE_DATE'].unique(), reverse=True)
        
        archive_lut = {}
        print(table_name.upper())
        print(f'The following archive dates are available to revert to in the {table_name} table: ')
        for i, date in enumerate(archive_dates):
            archive_lut[i+1] = date
            print(f'{i+1}. {date}')
        while True:
            try:
                selected_date = input('Please select the number corresponding with the date you would like to revert to: ')
                selected_date = archive_lut[int(selected_date)]
                selected_rows = archive_results[archive_results['ARCHIVE_DATE'] == selected_date]
                break
            except:
                print('Invalid input.')
                
        print(f'You have selected {selected_date}, which contains {selected_rows.shape[0]} rows.')
        confirm = input(f'Are you sure you would like to revert the {table_name} table? The current data will be permanenty deleted. (y/n) ')
        if confirm == 'y':
            # #append those results to the archive table
            selected_rows['INSERT_DATE'] = pd.to_datetime(selected_rows['INSERT_DATE']).dt.tz_localize('UTC')
            selected_rows['UPDATE_DATE'] = pd.to_datetime(selected_rows['UPDATE_DATE']).dt.tz_localize('UTC')
            selected_rows.drop(columns=['ARCHIVE_DATE'], inplace=True)
            
            session.sql(f'truncate table if exists {table_name};').collect()
            session.write_pandas(df = selected_rows, table_name = f'{table_name}', overwrite=False, auto_create_table=False)
            # #if there are more than 10 entries to the archive table, delete the oldest one.
            print(f'{table_name} table reverted to {selected_date}')
        else:
            print('Revert cancelled.')
        return
    
    ##first ask if the user wants to revert both aggregated results and indicator results
    
    loop = True
    while loop:
        revert_tables = input('''Please select the number corresponding with the option you would like:
                            1. Revert Aggregated Results. ANLT_IA_AGG_RESULTS
                            2. Revert Indicator Results. ANLT_IA_INDICATOR_RESULTS
                            3. Both 1 and 2 \n''')
        
        if revert_tables == '1':
            revert_agg = True
            revert_ind = False
            loop = False
        elif revert_tables == '2':
            rever_agg = False
            revert_ind = True
            loop = False
        elif revert_tables == '3':
            revert_agg = True
            revert_ind = True
            loop = False
        else:
            print('Invalid input. Please try again.')
            
    if revert_agg:
        unarchive_results(session, 'ANLT_IA_AGG_RESULTS')
    if revert_ind:
        unarchive_results(session, 'ANLT_IA_INDICATOR_RESULTS')

if __name__ == '__main__':
    main()