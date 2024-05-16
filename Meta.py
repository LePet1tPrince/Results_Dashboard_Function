#!/usr/bin/env python
# coding: utf-8
#!/usr/bin/env python -W ignore::DeprecationWarning

# This script includes the following functionality
# - Includes the functionality where we assign Funding Stream per ProgramCode, 
#     not Project

# This script consolidate the following scripts:
    # - META - Finalize how will calculate Non-People
    #   This is a different discussion than Multi-Year, but COULD be linked.
 
    # - META MultiYear  - Handled in UserInput
    #   - need more testing

    
    # Still Outstanding
    # - Package the script
    # - Read from new ImpactDB
    # Write output to snowflake (new IMpact DB)

import pandas as pd
import numpy as np
from datetime import datetime
import math
import sys
import time

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
start = time.time()
pd.set_option('display.max_columns', None)
#%% Get User Input


# TODO
# Manual INPUT
# The data available in our ITT range from these years:

def META(single_multi, range_from, range_to, meta, causeid, project_cause,df1, df_agelist, df_agemap, df_package):
  

    # single_multi = "single"  #multi / single
    # range_from = 19
    # range_to = 23

    year = [*range(range_from,range_to+1)]
    
    # Convert to Text
    list_year = [str(x) for x in year]
    # Add FY in front of text
    list_year = list(map('FY'.__add__,list_year)) 


    # Deterine Disaggretation for the script
    # TODO User Input

    # meta = True
    # causeid = True
    # project_cause = False
    # True / False


    grouping_list_master = ['period','country','multi_year','ivs_program_code', 'project_code', 'overlap','funding','p_np']
    #testing new line
    # grouping_list_master = ['period','country','meta_link','meta_sector','meta_statement','multi_year','ivs_program_code', 'project_code', 'overlap','funding','p_np','level_of_change']

    # Cannot run Meta and Project_Cause together
    # Cannot run Multi and Project_Cause together
    if meta:
        project_cause = False
        grouping_list_master = ['period','country','meta_link','meta_sector','meta_statement','multi_year','ivs_program_code', 'project_code', 'overlap','funding','p_np','level_of_change']
    if single_multi == "multi":
        project_cause = False
    # grouping_list_master = ['period','country','meta_link','meta_sector','multi_year','ivs_program_code', 'project_code', 'overlap','funding','p_np','level_of_change']    

    # to run project_cause, we need causid to be true
    if project_cause:
        causeid = True


    if causeid:
        disaggregation = 'causeid'
        grouping_list_master.insert(2,disaggregation)
    else:
        disaggregation = ''    
        

    grouping_list = grouping_list_master.copy()


    #%% Read Data
    # In[Set paths for reading data and add to Dataframes]
    # [Read ITT]


    
    # Filter out any Indicators without Meta
    df1['meta_link'] = df1['meta_link'].fillna("")
    df1 = df1[df1['meta_link'].str.contains('IVS')]

    # df1 = df1[df1['period'].str.contains('FY23')]


    # TODO
    # Remember to sort out the Sector Column
    df1 = df1.rename(columns={'sector_for_reporting_in_fy23': 'sector'})

   


    # TODO
    # Changes for CauseID calculation
    if disaggregation == 'causeid':
        # # Read and filter based on packages information
       
        
        # Do an outer join to merge all Projects in prockages with all the projects-ITT data available
        df_P = pd.merge(df1, df_package, left_on=['project_code'], right_on=['ivs_project_code'], how ="outer")
        
            # Filter based on available data
        df1 =df_P[df_P["fy23_data_status"] == 'Available']
        # df1 =df1[df1["period"] == 'FY22'] 
        
        df1 = df1[df1['project_code'].notna()]
    # TODO
        
        df1 = df1[df1['period']== 'FY23']
        # df1 = df1[df1['meta_link']== 'IVS-GIC-112']    
        
        
        
        # This section is needed to filter out only the GIK Projects that links objectivecodes
        # in the Datapackage file to Objective codes in the Df1 file
        df1["objectivecode"] =  np.where(df1['objectivecode'].isnull(), "All", df1["objectivecode"])
        
        # Create a filter to remove cases where there is no link between objectivecodes for GIK projects
        # Project marked as ALL should not be affected 
        df1["filter"] =  np.where(df1['objectivecode']== "All", 1,
                        np.where(df1['objectivecode'] == df1['ivs_objective_code'], 1, 0))
        
        # drop Descode
        df1 = df1.drop("descode", axis = 1)        
        
        # drop Dupliates
        df1.drop_duplicates(keep='last', inplace=True, ignore_index=True)    
        
        # df1.to_excel('results\df1.xlsx')    
        df1 = df1[df1["filter"] == 1]
        # remove last 4 columns as we do not need these anymore
        df1 = df1.iloc[:,:-4]
        



    # end = time.time()
    # print("Done reading :",
    #     (end-start) /60, "min")


    def allocate_difference(row):
        # Calculate the total before any rounding
        # If % contribution was applied, we round. If the value is below 1, we make it 1
        pre_roundedsum =row['girls'] + row['boys'] + row['women']+row['men']
        if pre_roundedsum <1:
            pre_roundedsum    =1
        else:                
            # in some cases python math leads to values such as 1.0000000002 and
            # we do not want this to be ceiling, but floor
            diff = pre_roundedsum - math.floor(pre_roundedsum)

            if diff < 0.0001:
                pre_roundedsum = math.floor(pre_roundedsum)
            elif diff != 0.0001:
                pre_roundedsum = math.ceil(pre_roundedsum)   
        
        # Round calculated values for B/G/M/W 
        girls = round(row['girls'],0)
        boys = round(row['boys'],0)
        women =round(row['women'],0)
        men = round(row['men'],0)
        
        # Calculate difference between RoundedSum and PreRoundedSum
        rounded_sum = girls + boys + women + men
        difference = pre_roundedsum - rounded_sum

        # Prioritize G/B/W/M and allocate the  difference to that bucket
        # Note that we look at the value as calculated in the dataframe that was sent,
        # as we may have a situation where the rounded figure is 0.
        if row['girls'] != 0:
            girls += difference
        elif row['boys'] != 0:
            boys += difference
        elif row['women'] != 0:
            women += difference
        elif row['men'] != 0:
            men += difference

        return pd.Series({'girls': girls, 'boys': boys, 'women': women, 'men': men})

    # Function to create summary values for the different high level age groups
    def calculate_M_F( dfx ): 
        dfx['boys'] = dfx[M_under18].sum(axis=1)   
        dfx['men'] = dfx[M_above18].sum(axis=1)   
        dfx['girls'] = dfx[F_under18].sum(axis=1)    
        dfx['women'] = dfx[F_above18].sum(axis=1) 

        # Define the allocate_difference function to round numbers
        # Test that DF is not empty
        if not dfx.empty:
            # Apply the allocate_difference function to each row
            dfx[['girls', 'boys', 'women', 'men']] = dfx.apply(allocate_difference, axis=1)
        
        #Add male, female, total columns
        dfx['male']= dfx['boys']+dfx['men']
        dfx['female']= dfx['girls']+dfx['women']
        dfx['total']= dfx['male']+dfx['female']

        return

    # Create Zero for total of all non-people
    def non_people(df=None):
        #demographic_brackets.remove('total')
        for col in demographic_brackets:
            df[col] = 0
        return df

    # In[Filter and adjust for MultiYear]

    # Filter only those years that should be included in the calculation
    if single_multi == 'multi':
        df1 = df1[df1['period'].isin(list_year)]
        
        # range_to
        df1['shift'] = range_to - df1['period'].str[-2:].astype(int) 
        # print(df1['shift'])
        
        # Convert All years to be the same
        df1['period'] = "FY"+str(range_from)+"_"+str(range_to)
    else:
        df1['shift'] = 0

    
    #%% Break down M and F down to Age Groups based on Equivalency*ProRateValue, Create the list of Values for People

    # ToDO
    # Idea is to move this bit to the aggregations scripts


    #get the age brackets from the AgeList table
    brackets = df_agelist['bracket'].tolist()

    def newBracket(age, shift, unique):  
        # get start and end ages for the range to enquire
        # we also add the shift as per the YEar on request
        start = df_agemap.loc[df_agemap.age_group.eq(age), 'start_group'].iloc[0] + shift
        end = df_agemap.loc[df_agemap.age_group.eq(age), 'end_group'].iloc[0] + shift
            
        # create blank dataframe for calculation purposes
        dfx = pd.DataFrame({'age_group':[],
                            'shift': [],
                            'bracket':[],
                        'cumulative':[],
                        'relative':[]})    
        # for each element in the age brackets, determine if that age bracket is 
        # applicabe in this age category
        for ele in brackets:
            start_bracket = df_agelist.loc[df_agelist.bracket.eq(ele), 'start_bracket'].iloc[0]    
            length_bracket = df_agelist.loc[df_agelist.bracket.eq(ele), 'length_bracket'].iloc[0]
            # Determine if this age bracket falls within the enquiry period
            if start_bracket < start:
                cumulative = 0
            else:
                if end > (start_bracket + length_bracket):
                    cumulative = length_bracket
                else:
                    if (end - start_bracket) > 0:
                        cumulative = (end - start_bracket)
                    else:
                        cumulative = 0     
            newrow = pd.Series([age, shift,ele,cumulative,0], index=dfx.columns)
            # dfx = dfx.append(newrow,ignore_index=True)
            dfx = pd.concat([dfx, newrow.to_frame().transpose()], ignore_index=True)
            # dfx = dfx.concat(newrow,ignore_index=True)   
            # dfx = dfx.concat(newrow,ignore_index=True)
    
        ratio =   dfx['cumulative'].sum()  
        
        if age in ["Not Applicable","N/A","Non-people" ]:
            dfx['relative'] =0
        else:
            dfx['relative'] = dfx['cumulative'] /ratio

        # swop rows and columns to create a df to send back 
        dfy = dfx.pivot( index = ['age_group', "shift"], columns='bracket', values='relative')    
        dfy = dfy.reset_index()
        dfy['unique'] = unique
        return dfy    
        

    # Get combination of all ages and shifts for the dataset
    project_ages = df1[["age_group", "shift"]].copy()
    project_ages.drop_duplicates( keep='last', inplace=True, ignore_index=True) 
    project_ages['unique'] = project_ages.index
    

    for index, row in project_ages.iterrows():
        age_group = (row[0])
        shift = (row[1])
        unique = (row[2])
        # Get Age %'s for each age bracket, for each lin, use the NewBrcket function
        values = newBracket(age_group, shift, unique)
        # values['unique'] = values.index
        # append line to the project_ages Df
        # project_ages = project_ages.append(values)
        project_ages= pd.concat([project_ages, values], ignore_index=True)
    
    
        
    project_ages.drop_duplicates(subset=['unique'], keep='last', inplace=True, ignore_index=True)    

    
    # Merge Age Brackets table back to the DF that contains the data
    df1 = pd.merge(df1, project_ages, on=['age_group', "shift"])    


    # Using the calculated % spread, calculate all the values in all the 
    # Age brackets, based on Numerator and Equivalency   
    c = df1['numerator'] *df1['equivalency']
    for ele in brackets:    
        df1[ele] *= c
        
    df1 = df1.drop("unique", axis = 1)    
        
    # break up the different AgeBrackets for calculations
    under18 = brackets[0:19]
    above18 = brackets[19:24]

    # above18 = brackets[19:24]

    # Define M/F/over and under 18 ages
    M_under18 = list(map('m'.__add__,under18))
    M_above18 = list(map('m'.__add__,above18))  

    F_under18 = list(map('f'.__add__,under18))
    F_above18 = list(map('f'.__add__,above18))  
    #%% TESTFILTERS




    for demographic in brackets:
        df1['male_percentage_x'] = df1['male_percentage'].copy()
        df1['male_percentage_x'] =  np.where(df1['sex_disaggregation'] == "Male", df1[demographic],
                                    np.where(df1['sex_disaggregation'] == "Female", 0,    
                                    np.where(df1['sex_disaggregation'] == "Total", 
                                    df1[demographic] *df1['male_percentage_x'],
                                            0)))
                                    
        df1.rename(columns={"male_percentage_x":'m' + demographic}, inplace=True)
        
    # Create the columns for the Female Age Brackets, and calculate the values based
    # on the Male% from the country Table

    # for demographic in brackets:
        df1['f' + demographic] = np.where(df1['sex_disaggregation'] == "Female", df1[demographic],  
                                        np.where(df1['sex_disaggregation'] == "Male", 0,    
                                        np.where(df1['sex_disaggregation'] == "Total",
                                        df1[demographic]  - df1['m' + demographic]
                                                ,0)))

    #Call function to create summary calculations
    calculate_M_F(df1)

    #%% Create the Value for non-people
    df1['total'] = np.where(df1['p_np'] == "non-people", df1['numerator'],df1['total'])

    # df1['total'] = np.where(df1['p_np'] == "non-people", 
    #                np.where(df1['numerator'] <1, 1,df1['numerator'].apply(np.ceil)),df1['total'])
    # .apply(np.floor) 

    print('df1')
    #%% DF2 - Caculate df2 = Max per Project Totals
    #Calculate the Max value per ivs_project_code/fy per Child Indicator, per Age Group, M & F

    # Demographic Brackets will be the last 55 columns in df1
    columnheadings = df1.columns.values.tolist()
    demographic_brackets = columnheadings[-55:]
    demographic_brackets.remove('total')

    #Calculate the Max value per ivs_project_code/period per Child Indicator, per Age Group, M & F
        # Copy columns from df1 (By Indicator) to Df2 (By ivs_project_code) and remove duplicates.
    df2 = df1[grouping_list].copy()

    if single_multi == "single":

    # In[DF2 - SingleYear ]
    # In[DF2 - non_WFP ]
    # Split between WFP and nonWFP projects
    # Non WFP projects will be MAXed
        print(single_multi) 
        df2 = df2[df2['funding']!= "WFP"]
        # df2.to_excel("results\\project_causeID_before.xlsx")
        # exit
        df2 = df2.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
        # df2.to_excel("results\\project_causeID_after.xlsx")
        print(grouping_list)
        
        # Max People
        df2_people  = df2[df2['p_np']== "people"]
        
        for demographic in demographic_brackets:
                subset = (df1.groupby(grouping_list)[demographic].max())  
                subset = pd.DataFrame(subset)
                subset = subset.reset_index()
        
                #  Now we will Merge the new subset value with df2
                df2_people = pd.merge(df2_people,subset, how = 'left', on = grouping_list)
        
        #Solve Totals  for people
        calculate_M_F(df2_people)
        
        # Sum NonPeople
        df2_non_people  = df2[df2['p_np']== "non-people"] 
        subset = (df1.groupby(grouping_list)['total'].sum())  
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        
        #  Now we will Merge the new subset value with df2
        df2_non_people = pd.merge(df2_non_people,subset, how = 'left', on = grouping_list)
        # df2_non_people['total'] = df2_non_people['total'].apply(np.floor) 
        df2_non_people['total']  = np.where(df2_non_people['total'] <1, 1,
                                            df2_non_people['total'].apply(np.floor))  
            
        #Solve Totals for non-people
        df2_non_people = non_people(df2_non_people)
        
        # Consolidate People & NonPeople
        df2_nonWfP = pd.concat([df2_people,df2_non_people], ignore_index=True, axis=0)
        
        print('df2')
        
        # In[DF2 - WFP ]
        
        df2 = df1[grouping_list].copy()
        
        # WFP projects will be SUMed
        df2 = df2[df2['funding']== "WFP"]
        df2 = df2.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
        
        
        # Max People
        df2_people  = df2[df2['p_np']== "people"]
        
        for demographic in demographic_brackets:
                subset = (df1.groupby(grouping_list)[demographic].sum())  
                subset = pd.DataFrame(subset)
                subset = subset.reset_index()
        
                #  Now we will Merge the new subset value with df2
                df2_people = pd.merge(df2_people,subset, how = 'left', on = grouping_list)
        
        #Solve Totals  for people
        calculate_M_F(df2_people)
                
        # Sum NonPeople
        # Filter for non-people
        # Create Zero value for all 
        df2_non_people  = df2[df2['p_np']== "non-people"] 
        
        subset = (df1.groupby(grouping_list)['total'].sum())  
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        
        #  Now we will Merge the new subset value with df2
        df2_non_people = pd.merge(df2_non_people,subset, how = 'left', on = grouping_list)
        # df2_nonpeople['total'] = df2_nonpeople['total'].apply(np.floor) 
        df2_non_people['total']  = np.where(df2_non_people['total'] <1, 1,
                                            df2_non_people['total'].apply(np.floor)) 

        #Solve Totals for non-people
        df2_non_people = non_people(df2_non_people)
        
        df2_WfP = pd.concat([df2_people,df2_non_people], ignore_index=True, axis=0)
        
        
        # Concat the WFP and Non WFP
        df2 = pd.concat([df2_WfP, df2_nonWfP])
    # In[DF2 - MultiYear ]
        
    else:
        print(single_multi)
        # exit
        # In[Split DB into People/NonPeople, aslo Multi-year SUM and MAX]
        #  First we are handling People
        df2People_Max = df2[(df2["p_np"] == "people" )&(df2["multi_year"] == "Max" )]
        df2People_Sum = df2[(df2["p_np"] == "people" )&(df2["multi_year"] == "Sum" )]
        
        df2People_Max = df2People_Max.drop_duplicates(subset = ['country','meta_link', 'project_code',"multi_year"],
        keep = 'last').reset_index(drop = True)
        df2People_Sum = df2People_Sum.drop_duplicates(subset = ['country','meta_link', 'project_code',"multi_year"],
        keep = 'last').reset_index(drop = True)
        
        
        # In[People: Calculate the Demographic per Column heading]
        
        # Split PEOPLE into SUM and MAX subsets based on the code in the META table for multi_year
        # Calculate the Demographic per Column heading, either Sum or Max
        for demographic in demographic_brackets:
            # #Create GroupBY Seriesbased on certain columns and do the Max in certain demographic column
            max_subset = (df1.groupby(['country','meta_link', 'project_code',"multi_year"])[demographic].max())           
            max_subset = pd.DataFrame(max_subset)
            max_subset = max_subset.reset_index()
            #  Now we will Merge the new demo-max value with df2
            df2People_Max = pd.merge(df2People_Max,max_subset, how = 'left', on = ['country','meta_link', 'project_code',"multi_year"])
            max_subset.drop
        
        # for demographic in demographic_brackets:
            # #Create GroupBY Seriesbased on certain columns and do the Max in certain demographic column
            sum_subset = (df1.groupby(['country','meta_link', 'project_code',"multi_year"])[demographic].sum())           
            sum_subset = pd.DataFrame(sum_subset)
            sum_subset = sum_subset.reset_index()
            #  Now we will Merge the new demo-max value with df2
            df2People_Sum = pd.merge(df2People_Sum,sum_subset, how = 'left', on = ['country','meta_link', 'project_code',"multi_year"])
            sum_subset.drop
            
            
        df2People = pd.concat([df2People_Max, df2People_Sum])
        
        # Do the sums for M/F
        calculate_M_F( df2People );
        
        
        
        # In[Non-People: Calculate the Demographic per Column heading]
        #  Now we are handling non-People
        df2non_people_Max = df2[(df2["p_np"] == "non-people")&(df2["multi_year"] == "Max" )]
        df2non_people_Sum = df2[(df2["p_np"] == "non-people")&(df2["multi_year"] == "Sum" )]
        
        df2non_people_Max_Index = df2non_people_Max.drop_duplicates(subset = ['country','meta_link', 'project_code','multi_year'],
        keep = 'last').reset_index(drop = True)
        df2non_people_Sum_Index = df2non_people_Sum.drop_duplicates(subset = ['country','meta_link', 'project_code','multi_year'],
        keep = 'last').reset_index(drop = True)
        
        
        
        
        # In[64]:
        # MAX
        # Do the sums for non_people (Should cthis be total or numerator?)
        subset = (df1.groupby(['country','meta_link','project_code',"p_np","multi_year"])["total"].max())  
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        
        #Drop the People and SUM values from this set
        subset = subset[(subset["p_np"] == "non-people")&(subset["multi_year"] == "Max" )]
        subset = subset.drop('p_np', axis=1)
        
        
        df2non_people_Max = pd.merge(df2non_people_Max_Index,subset, how = 'left', on = ['country','meta_link','project_code',"multi_year"])
        
        # Sum
        # Do the sums for non_people (Should cthis be total or numerator?)
        subset = (df1.groupby(['country','meta_link','project_code',"p_np","multi_year"])["total"].sum())  
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        
        
        #Drop the People and SUM values from this set
        subset = subset[(subset["p_np"] == "non-people")&(subset["multi_year"] == "Sum" )]
        subset = subset.drop('p_np', axis=1)
        
        
        df2non_people_Sum = pd.merge(df2non_people_Sum_Index,subset, how = 'left', on = ['country','meta_link','project_code',"multi_year"])
        
        # frames =      
        df2non_people = pd.concat([df2non_people_Max, df2non_people_Sum])
        # df2non_people['total'] = df2non_people['total'].apply(np.floor) 
        df2non_people['total']  = np.where(df2non_people['total'] <1, 1,
                                            df2non_people['total'].apply(np.floor))  
        
        # In[63]:
        
        
        for col in demographic_brackets:
            df2non_people[col] = 0
        df2non_people['boys'] = 0
        df2non_people['men'] =0
        df2non_people['girls'] = 0
        df2non_people['women'] =0
        
        # In[Merge People/Non-people]
        
        df2 = pd.concat([df2People, df2non_people], ignore_index=True, axis=0)
        df2.head(50)
        
        
        # In[75]:
        
        
        df2 = df2.sort_values(by = ['country','meta_link','project_code'], ascending = [True, True,True], na_position = 'last')    
        
    df2['totalrank'] = (np.where(df2['p_np'] == "non-people", 0,df2['total']))

    if project_cause:
        print("DataReqeust Output")
        # df2.to_excel("testing\\project_causeID.xlsx")
        return df2

    #%% DF3 - Group Data by ProgramCode
    # In[DF3a - All AP's ]
    #Calculate the Max value per ivs_program_code /period per Child Indicator, per Age Group, M & F.
    print('df3')
    # Set up the list for Grouping per ProgramCode
    grouping_list_master.remove('project_code')
    grouping_list = grouping_list_master.copy()
    # program_list = ['rc_number','avg_hh_size','male_percentage', 'female_percentage','under_18_percentage','over_18_percentage']
    # program_list = grouping_list + program_list

    temp_list = grouping_list.copy()
    temp_list.remove('overlap')
    temp_list.remove('funding')

    df3 = df2.copy()
    df3a = df3[df3['funding']== "SPN"]
    df3a = df3a[grouping_list].copy()
    df3a = df3a.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)

    df3a_people = df3a[df3a.p_np=='people']
    df3a_nonpeople = df3a[df3a.p_np=='non-people']


    #Group based on a specific subset of information 

    # Create GroupBY Seriesbased on certain columns and do the calculation in demographic column
    for demographic in demographic_brackets:
            subset = (df2.groupby(temp_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            df3a_people = pd.merge(df3a_people,subset, how = 'left', on = temp_list)
            
    calculate_M_F( df3a_people );

    # Now Calculate Non-People
    subset = (df2.groupby(temp_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df3a_nonpeople = pd.merge(df3a_nonpeople,subset, how = 'left', on = temp_list)
    non_people(df3a_nonpeople)
    # Concatenate people and non-people
    df3a = pd.concat([df3a_people, df3a_nonpeople], ignore_index=True, axis=0)        

    #Create new dataframe with columns in the order you want  
    #This fix was needed for somme outlier situations where not all fundingstreams 
    #were run and the column order got mixed up 
    #Make a list of all of the columns in the df
    cols = list(df3a.columns.values)

    # Remove elements that ARE in the Groupinglist
    cols = [ele for ele in cols if ele not in grouping_list]

    df3a = df3a[grouping_list+cols] 



    test = df3a.copy()


    # In[DF3x - AP's with OverlapCode]

    df3a = test.copy()
    # df3a = df3.copy() 
    # Use DF3a, as this is list of summed AP's and contains the Funding Stream per AP as well as the overlap per AP

    # We will now drop 'ivs_program_code' from the grouping, as we want to sum per 'overlap'
    temp_list = grouping_list.copy()
    temp_list.remove('ivs_program_code')
    # temp_list.remove('overlap')

    # print(grouping_list)
    # print(temp_list)

    # exit()
    # Copy columns from df3a (By APCode) to (By Overlap) and remove duplicatfes.
    df3x = df3a[temp_list].copy()
    df3x = df3x.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)
    # df3x = df3x.dropna(subset=['overlap'])

    df3x_people = df3x[df3x.p_np=='people']
    df3x_nonpeople = df3x[df3x.p_np=='non-people']

            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
        subset = (df3a.groupby(temp_list)[demographic].sum())     
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        #  Now we will Merge the new subset value with df2
        df3x_people = pd.merge(df3x_people,subset, how = 'left', on = temp_list)
        
    calculate_M_F( df3x_people );

    # Calculate non-people
    subset = (df3a.groupby(temp_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df3x_nonpeople = pd.merge(df3x_nonpeople,subset, how = 'left', on = temp_list)
    non_people(df3x_nonpeople)
    # Concatenate people and non-people
    df3x = pd.concat([df3x_people, df3x_nonpeople], ignore_index=True, axis=0)  
        
        
                
    # In[DF3b - PNS, GNT, WFP, OTH]
    
    temp_list = grouping_list.copy()
    temp_list.remove('overlap')

    df3b = df2[temp_list].copy()

    #  Filter out SPN and GIK
    listl = list(df3b.funding.unique())

    if 'SPN' in listl: listl.remove('SPN')
    if 'GIK' in listl: listl.remove('GIK')

    #print(listl)
    df3b = df3b[df3b.funding.isin(listl)]

    # temp_list.remove('ivs_program_code')
    
    df3b = df3b.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)

    df3b_people = df3b[df3b.p_np=='people']
    df3b_nonpeople = df3b[df3b.p_np=='non-people']


    # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(temp_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            df3b_people = pd.merge(df3b_people,subset, how = 'left', on = temp_list)
            
    calculate_M_F( df3b_people );

    # Calculate non-people
    subset = (df2.groupby(temp_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df3b_nonpeople = pd.merge(df3b_nonpeople,subset, how = 'left', on = temp_list)
    non_people(df3b_nonpeople)
    # Concatenate people and non-people
    df3b = pd.concat([df3b_people, df3b_nonpeople], ignore_index=True, axis=0)         


    # In[DF3c - GIK]

    temp_list = grouping_list.copy()
    temp_list.remove('overlap')

    df3c = df2[temp_list].copy()
    #  Filter for only GIK
    df3c = df3c[(df3c['funding']== "GIK")]

    # temp_list.remove('ivs_program_code')
        
    df3c = df3c.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)

    df3c_people = df3c[df3c.p_np=='people']
    df3c_nonpeople = df3c[df3c.p_np=='non-people']


    # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(temp_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            df3c_people = pd.merge(df3c_people,subset, how = 'left', on = temp_list)
            
    calculate_M_F( df3c_people )

    # Calculate non-people
    subset = (df2.groupby(temp_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df3c_nonpeople = pd.merge(df3c_nonpeople,subset, how = 'left', on = temp_list)
    non_people(df3c_nonpeople)
    # Concatenate people and non-people
    df3c = pd.concat([df3c_people, df3c_nonpeople], ignore_index=True, axis=0)


    # In[DF3 - df3a,df3b,df3c]
    
    df3 = pd.concat([df3a, df3b, df3c])
    df3 = df3.drop('overlap', axis=1)

    df3['totalrank'] = (np.where(df3['p_np'] == "non-people", 0,df3['total']))
    #%% DF4 - PNS
    # Create Sum in same RegionCode PNS’s (currently this is identified by the OverlapCode) 
    # Use DF2, as this is list of summed projects   
    # Copy columns from df2 to Df4 (By Overlap) and remove duplicates.
    print('df4')
    # Drop ivs_program_code for the rest of the script - sum/max across differnt ivs_program_codes
    grouping_list.remove('ivs_program_code')
        
    df4 = df2[grouping_list].copy()
    df4 = df4.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
        
        #  Drop non-verlapping rows
    df4 = df4.dropna(subset=['overlap'])
        
        #  Drop non-PNS rows
    df4 = df4.drop(df4[(df4["funding"] != 'PNS')].index)

    df4_people = df4[df4.p_np=='people']
    df4_nonpeople = df4[df4.p_np=='non-people']
        

            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with database
            df4_people = pd.merge(df4_people,subset, how = 'left', on = grouping_list)

    calculate_M_F(df4_people)

    # Calculate non-people
    subset = (df2.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df4_nonpeople = pd.merge(df4_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df4_nonpeople)
    # Concatenate people and non-people
    df4 = pd.concat([df4_people, df4_nonpeople], ignore_index=True, axis=0)

    df4['totalrank'] = (np.where(df4['p_np'] == "non-people", 0,df4['total']))
    #%% DF5 - GNT
    # Create Max in same RegionCode GNT’s (currently this is identified by the OverlapCode)
    print('df5') 
    df5 = df2[grouping_list].copy()
    df5 = df5.drop_duplicates(
        subset = grouping_list,
        keep = 'last').reset_index(drop = True)
        
        #  Drop non-verlapping rows
    df5 = df5.dropna(subset=['overlap'])
        
        #  Drop non-PNS rows
    df5 = df5.drop(df5[(df5["funding"] != 'GNT')].index)

    df5_people = df5[df5.p_np=='people']
    df5_nonpeople = df5[df5.p_np=='non-people']

            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(grouping_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with df
            df5_people = pd.merge(df5_people,subset, how = 'left', on = grouping_list)

    calculate_M_F(df5_people)

    # Calculate non-people
    subset = (df2.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df5_nonpeople = pd.merge(df5_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df5_nonpeople)
    # Concatenate people and non-people
    df5 = pd.concat([df5_people, df5_nonpeople], ignore_index=True, axis=0)

    df5['totalrank'] = (np.where(df5['p_np'] == "non-people", 0,df5['total']))

    #%% DF6 Max between DF3x, DF4, DF5
    # df6_temp = df3x.append([df4,df5], ignore_index=True)
    df6_temp = pd.concat([df3x,df4,df5], ignore_index=True)
    print('df6') 
    # Drop funding for the rest of the script - sum/max across differnt funding streams
    grouping_list.remove('funding')
    
    df6 = df6_temp[grouping_list].copy()
    df6 = df6.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    df6['funding'] = "Mixed"

    df6_people = df6[df6.p_np=='people']
    df6_nonpeople = df6[df6.p_np=='non-people']
    
            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
        subset = (df6_temp.groupby(grouping_list)[demographic].max())           
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        #  Now we will Merge the new subset value with df
        df6_people = pd.merge(df6_people,subset, how = 'left', on =grouping_list)

    calculate_M_F(df6_people)

    # Calculate non-people
    subset = (df6_temp.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df6_nonpeople = pd.merge(df6_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df6_nonpeople)
    # Concatenate people and non-people
    df6 = pd.concat([df6_people, df6_nonpeople], ignore_index=True, axis=0)

    df6['totalrank'] = (np.where(df6['p_np'] == "non-people", 0,df6['total']))
    #%% DF7 - Sum overlapping and NonOVerlapping projects
    # Ensure the correct columns are present for DF7
    print('df7')
    df7_prep = df2.copy()
    df7_prep = df7_prep[df6.columns]

    #  Select all projects with No Overlap
    df7_prep = df7_prep[df7_prep.overlap == 'No Overlap']

    # Add to overlapping dataset, df6
    # df7_temp = df6.append(df7_prep, ignore_index=True)
    df7_temp = pd.concat([df6,df7_prep], ignore_index=True)

    # Drop overlap for the rest of the script - sum/max across differnt overlap
    grouping_list.remove('overlap')
        
    # Get the Sum of these values
    df7 = df7_temp[grouping_list].copy()
    df7 = df7.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)

    df7_people = df7[df7.p_np=='people']
    df7_nonpeople = df7[df7.p_np=='non-people']

            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in demographic_brackets:
            subset = (df7_temp.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with df2
            df7_people = pd.merge(df7_people,subset, how = 'left', on = grouping_list)
            subset.drop

    calculate_M_F(df7_people)

    # Calculate non-people
    subset = (df7_temp.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df7_nonpeople = pd.merge(df7_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df7_nonpeople)
    # Concatenate people and non-people
    df7 = pd.concat([df7_people, df7_nonpeople], ignore_index=True, axis=0)

    df7['totalrank'] = (np.where(df7['p_np'] == "non-people", 0,df7['total']))
    #%% DF8 - - MAX GIK with the above
    #  Get dataset with GIK projects, ensure the correct columns are listed
    print('df8')
    df8_prep = df2.copy()
    df8_prep = df8_prep[df8_prep.funding == 'GIK']
    df8_prep = df8_prep[df7.columns]

    # Add to SPN/GNT/PNS/OTH dataset, DF7
    # df8_temp = df8_prep.append(df7, ignore_index=True)
    df8_temp = pd.concat([df8_prep,df7], ignore_index=True)
        
        # Get the Max of these values
    df8 = df8_temp[grouping_list].copy()
    df8 = df8.drop_duplicates(subset =grouping_list,keep = 'last').reset_index(drop = True)

    df8_people = df8[df8.p_np=='people']
    df8_nonpeople = df8[df8.p_np=='non-people']

            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in demographic_brackets:
            subset = (df8_temp.groupby(grouping_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with df2
            df8_people = pd.merge(df8_people,subset, how = 'left', on = grouping_list)
            subset.drop

    calculate_M_F(df8_people)

    # Calculate non-people
    subset = (df8_temp.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df8_nonpeople = pd.merge(df8_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df8_nonpeople)
    # Concatenate people and non-people
    df8 = pd.concat([df8_people, df8_nonpeople], ignore_index=True, axis=0)

    df8['totalrank'] = (np.where(df8['p_np'] == "non-people", 0,df8['total']))

    #%% DF9 - Sum WFP with the above 
    print('df9')
    #  Get dataset with WFP projects, ensure the correct columns are listed
    df9_prep = df2.copy()
    df9_prep = df9_prep[df9_prep.funding == 'WFP']
    df9_prep = df9_prep[df8.columns]

    # Add to GIK dataset, DF8
    # df9_temp = df9_prep.append(df8, ignore_index=True)
    df9_temp = pd.concat([df9_prep,df8], ignore_index=True)

        # Get the Sum of these values
    df9 = df9_temp[grouping_list].copy()
    df9 = df9.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    #df6['funding'] = "Mixed"

    df9_people = df9[df9.p_np=='people']
    df9_nonpeople = df9[df9.p_np=='non-people']

            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in demographic_brackets:
            subset = (df9_temp.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            # print(subset)
            #  Now we will Merge the new subset value with df2
            df9_people = pd.merge(df9_people,subset, how = 'left', on = grouping_list)

    calculate_M_F(df9_people)

    # Calculate non-people
    subset = (df9_temp.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df9_nonpeople = pd.merge(df9_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df9_nonpeople)
    # Concatenate people and non-people
    df9 = pd.concat([df9_people, df9_nonpeople], ignore_index=True, axis=0)

    df9['totalrank'] = (np.where(df9['p_np'] == "non-people", 0,df9['total']))

    #%% DF10 - PVTSummary
    print('df10')
    #Final Summary

        
    #Final Summary
    # This is executed as a pivot table, summing only the main categories
    summary_list = ['boys','girls','men','women']
    grouping_list.remove('country')

    # Get the Sum of these values
    df10 = df9[grouping_list].copy()
    df10 = df10.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    #df6['funding'] = "Mixed"

    df10_people = df10[df10.p_np=='people']
    df10_nonpeople = df10[df10.p_np=='non-people']

            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in summary_list:
            subset = (df9.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            # print(subset)
            #  Now we will Merge the new subset value with df2
            df10_people = pd.merge(df10_people,subset, how = 'left', on = grouping_list)

    df10_people['male']= df10_people['boys']+df10_people['men']
    df10_people['female']= df10_people['girls']+df10_people['women']
    df10_people['total']= df10_people['male']+df10_people['female']

    # Calculate non-people
    subset = (df9.groupby(grouping_list)['total'].sum())           
    subset = pd.DataFrame(subset)
    subset = subset.reset_index()
    df10_nonpeople = pd.merge(df10_nonpeople,subset, how = 'left', on = grouping_list)
    non_people(df10_nonpeople)
    # Concatenate people and non-people
    df10 = pd.concat([df10_people, df10_nonpeople], ignore_index=True, axis=0)

    df10['totalrank'] = (np.where(df10['p_np'] == "non-people", 0,df10['total']))

    #%% Rank datasets
    # Ranking will only be applied on DF1(Indicator) Df2(Project) Df3 (Program) df9(Country) and Df10 (Summary)

    def ranking(dfx):
        dfx['LOC_Ranking'] = (np.where(dfx['level_of_change'] == '3.a. Access - Improved infrastructure, availability and quality', 3,   
                            np.where(dfx['level_of_change'] == '3.b. Change in Knowledge, Skills or Capacity', 3,                                 
                            np.where(dfx['level_of_change'] == '1. Change of State | Status', 1,
                            np.where(dfx['level_of_change'] == '4.a. Direct Provision', 4,         
                            np.where(dfx['level_of_change'] == '2.d.1. Enabling Policy Environment', 2,         
                            np.where(dfx['level_of_change'] == '2.b. Individual Behaviour Change', 2,
                            np.where(dfx['level_of_change'] == '5.a. Infrastructure', 5,         
                            np.where(dfx['level_of_change'] == '2.d.2. Institutional Sustainability', 2,         
                            np.where(dfx['level_of_change'] == '3.c. Monitoring & Accountability', 3,         
                            np.where(dfx['level_of_change'] == '4.b. Learning', 4,
                            np.where(dfx['level_of_change'] == '2.c.1. Perceived Agency and Powers', 2,         
                            np.where(dfx['level_of_change'] == '4.c. Systems Strengthening', 4,
                            np.where(dfx['level_of_change'] == '5.b.c. Training & Capacity Building', 5,
                            np.where(dfx['level_of_change'] == '2.c.2. Attitudes and Norms', 2,
                            np.where(dfx['level_of_change'] == '2.a. Utilization or Uptake of Services',2,
                            np.where(dfx['level_of_change'] == '6. Distribution of Resources',6,9)))))))))))))))))
        
        # UOA is now replaced with p/np as UAO is not available at this stage of the calculation
        dfx['UOA_Ranking'] = (np.where(dfx['p_np'] == "people", 1, 999999   
                                ))
        
        #dfx = dfx.sort_values(by = ['country','project_pbas','LOC_Ranking','UOA_Ranking','Indicator_Ranking',"indStatementRank"], ascending = [True, True, True, True,True,False], na_position = 'last')
        
        return


    # In[Rank DF1]
    df1['totalrank'] = (np.where(df1['p_np'] == "non-people", 0,df1['total']))
    ranking(df1)

    df1['Indicator_Ranking'] = 999

    #Child Indicator and parent indicator ranking 
    df1['Indicator_Ranking'] = (np.where((df1['priority_link'] == 'No PRTority link')|(df1['priority_link'] == 'Missing')|(df1['priority_link'] == 'Poorly defined')|(df1['priority_link'].isna()),999,1))
    df1["indStatementRank"]= df1.groupby(["indicator_statement"])["indicator_statement"].transform('count')

    df1 = df1.sort_values(by = ['country','project_code','LOC_Ranking','totalrank','UOA_Ranking','Indicator_Ranking',"indStatementRank"], 
                        ascending = [True, True, True,False, True,True,False], na_position = 'last')
    df1['rank'] = 1
    df1['rank'] = df1.groupby(['country','project_code', "period"])['rank'].cumsum()

    # In[Rank DF2]
    ranking(df2)
    df2 = df2.sort_values(by = ['period','country','project_code','LOC_Ranking','totalrank','UOA_Ranking'], 
                        ascending = [True, True, True, True,False, True], na_position = 'last')
    df2['rank'] = 1

    df2_people = df2[df2["p_np"]=="people"]
    df2_non_people = df2[df2["p_np"]=="non-people"].sort_values(by = ['total'], ascending = [False], na_position = 'last')
    df2 = pd.concat([df2_people,df2_non_people])
    # df2 = df2.reset_index()

    df2['rank'] = df2.groupby(['country','project_code',"period"])['rank'].cumsum()

    # In[Rank DF3]
    ranking(df3)
    df3 = df3.sort_values(by = ['period','country','ivs_program_code','LOC_Ranking','totalrank','UOA_Ranking'], 
                        ascending = [True,True, True, True,False, True], na_position = 'last')
    df3['rank'] = 1
    df3_people = df3[df3["p_np"]=="people"]
    df3_non_people = df3[df3["p_np"]=="non-people"].sort_values(by = ['total'], ascending = [False], na_position = 'last')
    df3 = pd.concat([df3_people,df3_non_people])
    df3['rank'] = df3.groupby(['country','ivs_program_code',"period"])['rank'].cumsum()

    # In[Rank DF9]
    ranking(df9)
    df9 = df9.sort_values(by = ['period','country','LOC_Ranking','totalrank','UOA_Ranking'], ascending = [True,True, True,False, True], na_position = 'last')
    df9['rank'] = 1
    df9_people = df9[df9["p_np"]=="people"]
    df9_non_people = df9[df9["p_np"]=="non-people"].sort_values(by = ['total'], ascending = [False], na_position = 'last')
    df9 = pd.concat([df9_people,df9_non_people])
    df9['rank'] = df9.groupby(['country',"period"])['rank'].cumsum()

    # In[Rank DF10]
    ranking(df10)
    df10 = df10.sort_values(by = ['period','LOC_Ranking','totalrank','UOA_Ranking'], ascending = [True,True,False, True], na_position = 'last')
    df10['rank'] = 1
    df10_people = df10[df10["p_np"]=="people"]
    df10_non_people = df10[df10["p_np"]=="non-people"].sort_values(by = ['total'], ascending = [False], na_position = 'last')
    df10 = pd.concat([df10_people,df10_non_people])
    df10['rank'] = df10.groupby(["period"])['rank'].cumsum()


    #%% Prep for Print all =================================================================
    # Create the column heading for the summary pages
    summary_list_master = [
    "period",
    'meta_link',
    'meta_sector',
    'meta_statement',
    'p_np',
    "boys",
    "men",
    "male",
    "girls",
    "women",
    "female",
    "total"]

    # Modify the column headings for specific summaries
    Program_Summary_list = summary_list_master.copy()
    Program_Summary_list.insert(1,'ivs_program_code')
    Program_Summary_list.insert(1,'country')
    Country_summary_list = summary_list_master.copy()
    Country_summary_list.insert(1,'country')
    Project_Summary_list = summary_list_master.copy()
    Project_Summary_list.insert(1,'country')
    Project_Summary_list.insert(1,'project_code')
    PVT_Summary_list = summary_list_master.copy()

    # Add disaggregation column if need to
    if disaggregation != "":
            Program_Summary_list.insert(1,disaggregation)
            Country_summary_list.insert(1,disaggregation)        
            Project_Summary_list.insert(1,disaggregation)
            PVT_Summary_list.insert(1,disaggregation)
    
    #Create summaries of specific datasets 
    dfProgram_summary = (df3[Program_Summary_list])
    dfCountry_summary = (df9[Country_summary_list])
    dfProject_summary = (df2[Project_Summary_list])
    dfPVT_summary = (df10[PVT_Summary_list])

    # dfProgram_Summary = dfProgram_Summary.drop_duplicates(subset = ["ivs_program_code", "total", "period"],keep = 'last').reset_index(drop = True)

    #Print total as per the Country Summary 
    total = df2['total'].sum()
    print ("Project:  ")
    print (total)

    #Print total as per the Country Summary 
    total = df3['total'].sum()
    print ("Program ")
    print (total)

    #Print total as per the Country Summary 
    total = df9['total'].sum()
    print ("Country ")
    print (total)

    #Print total as per the Country Summary 
    total = df10['total'].sum()
    print ("PVT ")
    print (total)


    #%% PRINT
    # end = time.time()
    # print("Done :",
    #     (end-start) /60, "min")
    

    return dfProject_summary, dfProgram_summary, dfCountry_summary
    # return dfProject_summary
    

