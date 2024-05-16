"""
This script is meant to operate the Reach and Meta Script. It outputs the results to Snowflake.
Inputs: There are several inputs, almost all are from snowflake:
ANALYTICS_SANDBOX.INSIGHTS.HUB_TABLE
ANALYTICS_SANDBOX.INSIGHTS.AGELIST
ANALYTICS_SANDBOX.INSIGHTS.AGEMAP_TEST
ANALYTICS_SANDBOX.INSIGHTS.AGEMAP
ANALYTICS_SANDBOX.INSIGHTS.CAUSES
There is one last input which is a local excel file that contains the GHC data. This is a manual input.

Outputs: The script outputs the results to Snowflake in the ANLT_IA_AGG_RESULTS table and a local excel version. 
This table is then used to create visualizations in Sisense.
This script also outputs 4 excel files to the Interim folder. Two of them are the raw results from the Reach and Meta scripts before cleaning. 
Two of them are copies of the same tables uploaded to snowflake. If you generate these tables and the snowflake upload fails for some reason, 
you can run the 'direct_upload.py' script to upload the data from excel to snowflake, which takes much less time than rerunning the entire script.


Overview:
This script acts as the controller for the Reach and Meta Scripts.
We want a single table as the result of the script, but we need results from many diffferent combinations 
of parameters to achieve that.
This script will run the Reach and Meta scripts with different parameters and then output the results to Snowflake.

More detailed documentaation is available here
https://worldvisioncanada.atlassian.net/wiki/spaces/IART/pages/4047372334/Snowflake+Results+Pipeline+Script



""" 
## This script is meant to operate the Reach and Meta Script. It outputs the results to Snowflake.

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

def main():
        
    ## Reach Calculation
    
    
    
    range_from = 16 #this should stay
    range_to = 23 #create a dynamic way to for this to be the current year
    user = 'timothy_bender@worldvision.ca'
    #%% Read Data
        
    fullstart = time.time()
    #th data insights role has read access to a lot of resources. Use this for ingesting data.
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
    
    
    def query_session(query):
        """This function takes a query and returns a pandas dataframe from the Snowflake session."""
        sql_table = session.sql(query)
        df = sql_table.to_pandas()
        df.columns= df.columns.str.lower()
        return df
    
    ## read in all the tables we need
    df1 = query_session("select * from ANALYTICS_SANDBOX.INSIGHTS.HUB_TABLE")
    df1 = df1.rename(columns={'sector_for_reporting_in_fy23': 'sector', 'external_programming_type': 'programming_type'})
    
    df_agelist = query_session("select * from ANALYTICS_SANDBOX.INSIGHTS.AGELIST")
    df_agemap_test = query_session("select * from ANALYTICS_SANDBOX.INSIGHTS.AGEMAP_TEST")
    df_package = query_session("select * from ANALYTICS_SANDBOX.INSIGHTS.CAUSES")
    df_agemap = query_session("select * from ANALYTICS_SANDBOX.INSIGHTS.AGEMAP")
    
    
    
    end = time.time()
    print("Done reading :",
          (end-fullstart) /60, "min")
     
    
    
    #%% Reach Calculation
    start = time.time()
    
    agg_results_df = pd.DataFrame() #this will be the large aggregated results table. we will append to it through
    data_request_df = pd.DataFrame()
    indicator_results_df = pd.DataFrame()
    
    #here we can set the parameters we want to run through the reach script. The reach script will run j * i times so be careful about adding too many parameters.
    parameters = ['sector','programming_type','causeid','']
    # parameters = ['']
    single_multi = ['single','multi']
    
    for i in parameters:
        for j in single_multi:
            if i == 'causeid' and j == 'multi':
                continue
            print(f'Running the reach script for {i}-{j}...')
            pjt_summary, pgm_summary, country_summary, ind_pjt_summary = REACH(i,j,range_from,range_to, df1,df_agelist, df_agemap_test, df_package)
            pjt_summary['result_type'] = j + '-year_' + i + '_Project_Reach'
            pgm_summary['result_type'] = j + '-year_' + i + '_Program_Reach'
            country_summary['result_type'] = j + '-year_' + i + '_Country_Reach'
            ind_pjt_summary['result_type'] = j + '-year_' + i + '_indicator_Project_Reach'
            if i == '': #if this parameter is blank, we want to keep the indicator by project summary
                indicator_results_df = pd.concat([indicator_results_df, ind_pjt_summary], axis=0)            
            agg_results_df = pd.concat([agg_results_df, pjt_summary, pgm_summary, country_summary], axis=0)            

    indicator_results_df.to_excel('Interim/indicator_project_results_raw.xlsx')
    # indicator_results_df = pd.read_excel('Interim/indicator_project_results_raw.xlsx')
    end = time.time()
    print("Reach DONE!! :",
          (end-start) /60, "min") 
      
    
     
     
     
     
# %%Meta Calculation
    start = time.time()

    #there will be 4 cases. They are listed out as such.
    single_multi_list = ["single", "multi"]
    meta_list = [True, True]
    causeid_list = [False, False]
    project_cause_list = [False, False]
    # single_multi_list = ["single"]
    # meta_list = [True]
    # causeid_list = [False]
    # project_cause_list = [False]
    
    for i in range(len(single_multi_list)):
        print("Runing META Case", i+1)
        if project_cause_list[i]: #if project_cause_list is true, then the function behaves differently
            dfProject_summary = META(single_multi_list[i], range_from, range_to, meta_list[i], causeid_list[i], project_cause_list[i], df1, df_agelist, df_agemap, df_package)
            dfProject_summary['result_type'] = 'Meta Single Project CauseID Summary'
            keep_df = dfProject_summary[['result_type','period','country','causeid','ivs_program_code','project_code','p_np','boys','men','girls','women']]
            data_request_df = pd.concat([data_request_df, keep_df], axis=0)
        else: #normal case
            dfProject_summary, dfProgram_summary, dfCountry_summary = META(single_multi_list[i], range_from, range_to, meta_list[i], causeid_list[i], project_cause_list[i], df1, df_agelist, df_agemap, df_package)
            if causeid_list[i]:
                dfProject_summary['result_type'] = f"{single_multi_list[i]}-year_CauseID_Project_Meta"
                dfProgram_summary['result_type'] = f"{single_multi_list[i]}-year_CauseID_Program_Meta"
                dfCountry_summary['result_type'] = f"{single_multi_list[i]}-year_CauseID_Country_Meta"
                # dfPVT_summary['result_type'] = f"{single_multi_list[i]}-year_CauseID_PVT_Meta"
            else:
                dfProject_summary['result_type'] = f"{single_multi_list[i]}-year_Project_Meta"
                dfProgram_summary['result_type'] = f"{single_multi_list[i]}-year_Program_Meta"
                dfCountry_summary['result_type'] = f"{single_multi_list[i]}-year_Country_Meta"
                # dfPVT_summary['result_type'] = f"{single_multi_list[i]}-year_PVT_Meta"
                
            
            agg_results_df = pd.concat([agg_results_df, dfProject_summary, dfProgram_summary, dfCountry_summary], axis=0)
        
    end = time.time()
    print("META DONE!! :",
          (end-start) /60, "min")
    
    ##ADDING GHC
    start = time.time()
    print("Adding in GHC Data...")
    # ghc_reach_df = pd.read_excel('Input/GHC_PGM_META.xlsx')
    # ghc_reach_df['result_type'] = 'single-year__Program_Reach_GHC'
    ghc_meta_df = pd.read_excel('Input/GHC_PGM_META.xlsx')
    ghc_meta_df['result_type'] = 'single-year__Program_Meta_GHC'
    
    
    agg_results_df = pd.concat([agg_results_df, ghc_meta_df], axis=0)
    end = time.time()
    
    print("GHC DONE!! :", (end-start) /60, "min")
    
    
    agg_results_df.to_excel('Interim/raw_agg_results.xlsx')
    
    # agg_results_df = pd.read_excel('Interim/raw_agg_results.xlsx')
    
    
    
    ## Clean results
    print('Cleaning Results...')
    cleantime1 = time.time()
    def clean_people(df):
        #This converts the values in the 'p_np' column to True or False
        if df['p_np'] == 'people':
            return True
        elif df['p_np'] == 'non-people':
            return False
        else:
            return None
        
    agg_results_df['p_np'] = agg_results_df.apply(clean_people, axis=1)
    
    def clean_period(df):
        #this function converts multi-year periods to FY_ALL
        if "_" in df['period']:
            return "FY_ALL"
        else:
            return df['period']
    agg_results_df['period'] = agg_results_df.apply(clean_period, axis=1)
    
 
    ## combine sector column with meta_sector column
    agg_results_df.rename(columns={'sector':'sector_temp'}, inplace=True)
    agg_results_df['sector'] = agg_results_df['sector_temp'].fillna(agg_results_df['meta_sector'])
    
    
    ## lookup various codes (project, program, country etc) and find the ID's of that table.
    #find the project ID
    sf_proj_df = query_session("select DIM_PROJECT_ID as Project_ID,ivs_project_code from ivs_dpms_dev.publish.dim_project_prod_26042024;")
    sf_proj_lut = sf_proj_df.reset_index().set_index('ivs_project_code').to_dict()['project_id']
    agg_results_df['project_ID'] = agg_results_df['project_code'].map(sf_proj_lut)
    
    #find the program ID
    sf_prog_df = query_session("select DIM_PROGRAM_ID as program_Id, IVS_PROGRAM_CODE from IVS_DPMS_DEV.PUBLISH.DIM_PROGRAM_PROD_03052024;")
    sf_prog_lut = sf_prog_df.reset_index().set_index('ivs_program_code').to_dict()['program_id']
    agg_results_df['program_ID'] = agg_results_df['ivs_program_code'].map(sf_prog_lut)
    
    #Find the indicator ID -- we have no indicator ID yet in the IRT so commenting this out.
    # sf_ind_df = query_session("select  as indicator_id, CRD30_NAME as ivs_indicator_code from IVS_DPMS_PROD.RAW.RAW_CE_CRD30_DPMSINDICATOR;")
    # sf_ind_lut = sf_ind_df.reset_index().set_index('ivs_indicator_code').to_dict()['indicator_id']
    # agg_results_df['indicator_ID'] = agg_results_df['meta_link'].map(sf_ind_lut)
    # agg_results_df = agg_results_df.merge(sf_ind_df, left_on='meta_link', right_on='ivs_indicator_code', how='left')
    
    ##fill out any blanks with 0s
    agg_results_df.fillna({'girls': 0, 'boys': 0, 'men': 0, 'women': 0}, inplace=True)

    
    
    
    agg_results_df.rename(columns={'meta_link': 'IVS_Indicator_Code',
                                'project_code':'IVS_Project_Code',
                                'country': 'country_name',
                                'p_np': 'is_People'}, inplace=True)
    
    
    agg_results_df.columns = agg_results_df.columns.str.upper() 
    
    nowtime = datetime.now()
    agg_results_df['INSERT_DATE'] = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    agg_results_df['UPDATE_DATE'] = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    
    #This is a lookup table to determine the batch ID. This is a way to group the results together.
#     CODE FORMAT: "1ABCDE"
# A: Indicates reach or meta
# B: indicates whether the measurement on the project, program, country or indicator level
# C: indicates whether there is a further disaggregation by sector, programming_type or cause_id
# D: single or multi year
# E: 1 if it’s the GHC project (this is manual

    batch_weight_lut = {
        'reach': 1E4,
        'meta': 2E4,
        
        'project': 1E3,
        'program': 2E3,
        'country': 3E3,
        
        
        #blanks will have a 0 in this field
        'sector': 1E2,
        'programming_type': 2E2,
        'causeid': 3E2,
        'indicator': 4E2,
        
        'single': 1E1,
        'multi': 2E1,
        
        'ghc': 1
    }
    
    def get_batch_id(df):
        base_id = 100000
        for word in batch_weight_lut:
            if word in df['RESULT_TYPE'].lower():
                base_id += batch_weight_lut[word]
        return base_id

    agg_results_df['BATCH_ID'] = agg_results_df.apply(lambda x: get_batch_id(x),axis=1)
            
        
    
    dump_df = agg_results_df[['BATCH_ID','RESULT_TYPE',
                              #'COUNTRY_ID', 
                              'COUNTRY_NAME','PERIOD','SECTOR','PROGRAM_ID','IVS_PROGRAM_CODE','PROJECT_ID',
                              'IVS_PROJECT_CODE',
                              'PROGRAMMING_TYPE', 
                            #   'DATA_REQUEST_ID',
                            #   'INDICATOR_ID',
                              'IVS_INDICATOR_CODE','IS_PEOPLE','GIRLS','BOYS','MEN','WOMEN','TOTAL','INSERT_DATE',
                              'UPDATE_DATE'
                              ]].copy()
    
        
    dump_df = dump_df.reset_index(drop=True)
    dump_df.to_excel('Interim/agg_results.xlsx')
    
    #change up session qualities so we can write to snowflake
    session.use_database(write_credentials['database'])
    session.use_schema(write_credentials['schema'])
    session.use_role(write_credentials['role'])
    session.use_warehouse(write_credentials['warehouse'])    
    
    session.sql('truncate table if exists ANLT_IA_AGG_RESULTS;').collect()
    session.write_pandas(df = dump_df, table_name = 'ANLT_IA_AGG_RESULTS', overwrite=False, auto_create_table=False)
    
    cleantime1end = time.time()
    print("Results table cleaned and uploaded!! :",
          (cleantime1end-cleantime1) /60, "min")
    
    # -----------------------------------------------------
    #write the indicator_results table
    cleantime2 = time.time()
    ##turn people/nonpeople into boolean    
    indicator_results_df['p_np'] = indicator_results_df.apply(clean_people, axis=1)
    ##turn multi-year periods into FY_ALL
    indicator_results_df['period'] = indicator_results_df.apply(clean_period, axis=1)
    ## get project ID
    indicator_results_df['project_ID'] = indicator_results_df['project_code'].map(sf_proj_lut)
    indicator_results_df['program_ID'] = indicator_results_df['ivs_program_code'].map(sf_prog_lut)
    indicator_results_df.fillna({'girls': 0, 'boys': 0, 'men': 0, 'women': 0}, inplace=True)
    
    
    indicator_results_df.rename(columns={
                                'project_code':'IVS_Project_Code',
                                'country': 'country_name',
                                'p_np': 'is_People',
                                'meta_link': 'meta_indicator_code'}, inplace=True)
            
    indicator_results_df.columns = indicator_results_df.columns.str.upper() 
    
    nowtime = datetime.now()
    indicator_results_df['INSERT_DATE'] = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    indicator_results_df['UPDATE_DATE'] = nowtime.strftime("%Y-%m-%d %H:%M:%S")
    ##get batch ID
    indicator_results_df['BATCH_ID'] = indicator_results_df.apply(lambda x: get_batch_id(x),axis=1)
    
    dump_df = indicator_results_df[['BATCH_ID','RESULT_TYPE',
                              #'COUNTRY_ID',
                              'COUNTRY_NAME',
                              'PERIOD','YEAR',
                              'SECTOR','PROGRAM_ID','IVS_PROGRAM_CODE',
                              'PROJECT_ID', 'IVS_PROJECT_CODE',
                              'PROGRAMMING_TYPE', 
                              'OBJECTIVE_LEVEL',
                              'IVS_OBJECTIVE_CODE',
                              'INDICATOR_CODE',
                              'AGE_GROUP',
                              #'AGE_GROUP_ID',
                              'UNIT_OF_MEASURE', 'UNIT_OF_ANALYSIS', 'IS_PEOPLE','SEX_DISAGGREGATION',
                              'META_INDICATOR_CODE',
                            #   'META_INDICATOR_ID',
                              'MULTI_YEAR',
                              'NUMERATOR','DENOMINATOR','GIRLS','BOYS','MEN','WOMEN','TOTAL',
                              'INSERT_DATE',
                              'UPDATE_DATE'
                              ]].copy()
    
    dump_df.to_excel('Interim/indicator_results.xlsx')
    session.sql('truncate table if exists ANLT_IA_INDICATOR_RESULTS;').collect()
    session.write_pandas(df = dump_df, table_name = 'ANLT_IA_INDICATOR_RESULTS', overwrite=False, auto_create_table=False)
    
    cleantime2end = time.time()
    print("Indicator table cleaned and uploaded!! :",
          (cleantime2end-cleantime2) /60, "min")
    
    end = time.time()
    print("Total Time!! :",
          (end-fullstart) /60, "min")    
    
    
    session.close()
    print("Session Closed")
    print("Done")

 # %%

if __name__ == '__main__':
    main()