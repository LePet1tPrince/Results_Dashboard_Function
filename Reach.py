#!/usr/bin/env python
# coding: utf-8
#!/usr/bin/env python -W ignore::DeprecationWarning

# This script includes the following functionality
# - Select MultiYear or not
# - Select disaggration by either nothing, or Sector or ProgrammingType. 
# - Any other disaggreation is possible
# - Includes the functionality where we assign Funding Stream per ProgramCode, 
#     not Project
# - Includes fix for WFP that will be ALWAYS summed
# This script consolidate the following scripts:
    # - Reach per Country
    # - Reach per Sector
    # - Reach per ProgramingType
    # - MultiYear
    
    # Still Outstanding
    # - Package the script
    # - Read from new ImpactDB
    # Write output to snowflake (new IMpact DB)
    # - 


import pandas as pd
import numpy as np
from snowflake.snowpark import Session
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None  # default='warn'
import time
import math

def REACH(disaggregation,single_multi,range_from,range_to,df1, df_agelist, df_agemap, df_package):
 

    pd.set_option('display.max_columns', None)

    year = [*range(range_from,range_to+1)]
      
      # Convert to Text
    list_year = [str(x) for x in year]
      # Add FY in front of text
    list_year = list(map('FY'.__add__,list_year)) 
    
    # Deterine Disaggretation for the script
    
    # disaggregation = 'causeid'
    # disaggregation_ = ''/'sector' / 'programming_type / causeid'
    
    # GroupingListMaster will be used to determine how Disaggregation will work 
    # through different levels in the script
    
    grouping_list_master = ['period','country','overlap','funding','ivs_program_code', 'project_code']
    if disaggregation != "":
            grouping_list_master.insert(2,disaggregation)
    
    grouping_list = grouping_list_master.copy()
    
  
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
    # def df1_calculate_M_F( dfx ): 
    def calculate_M_F( dfx ):         
        dfx['boys'] = dfx[M_under18].sum(axis=1)   
        dfx['men'] = dfx[M_above18].sum(axis=1)   
        dfx['girls'] = dfx[F_under18].sum(axis=1)    
        dfx['women'] = dfx[F_above18].sum(axis=1) 
        
   
        if not dfx.empty:
             # Apply the allocate_difference function to each row
            dfx[['girls', 'boys', 'women', 'men']] = dfx.apply(allocate_difference, axis=1)
        
        #Add male, female, total columns
        dfx['male']= dfx['boys']+dfx['men']
        dfx['female']= dfx['girls']+dfx['women']
        dfx['total']= dfx['male']+dfx['female']
    
        return
    
    # def calculate_M_F( dfx ): 
    #     dfx['boys'] = dfx[M_under18].sum(axis=1).apply(np.floor)  
    #     dfx['men'] = dfx[M_above18].sum(axis=1).apply(np.floor)
    #     dfx['girls'] = dfx[F_under18].sum(axis=1).apply(np.floor)    
    #     dfx['women'] = dfx[F_above18].sum(axis=1).apply(np.floor) 
    
    #     # if not dfx.empty:
    #     #      # Apply the allocate_difference function to each row
    #     #     dfx[['girls', 'boys', 'women', 'men']] = dfx.apply(allocate_difference, axis=1)
        
    #     #Add male, female, total columns
    #     dfx['male']= dfx['boys']+dfx['men']
    #     dfx['female']= dfx['girls']+dfx['women']
    #     dfx['total']= dfx['male']+dfx['female']
    
    #     return    
    
    
    
    # TODO
    # Changes for CauseID calculation
    if disaggregation == 'causeid':
        
        print('ehllo')
     
        # Do an outer join to merge all Projects in prockages with all the projects-ITT data available
        
        df_P = pd.merge(df1, df_package, left_on=['project_code'], right_on=['ivs_project_code'], how ="outer")
        
            # Filter based on available data
        df1 =df_P[df_P["fy23_data_status"] == 'Available']
        # df1 =df_P[df_P["fy23_data_status"] == 'Available']
        
        df1 =df1[df1["period"] == 'FY23'] 
        
        # This section is needed to filter out only the GIK Projects that links objectivecodes
        # in the Datapackage file to Objective codes in the Df1 file
        # df1["objectivecode"] =  np.where(df1['objectivecode'].isnull(), "All", df1["objectivecode"])
        
        # Create a filter to remove cases where there is no link between objectivecodes for GIK projects
        # Project marked as ALL should not be affected 
        df1["filter"] =  np.where(df1['objectivecode']== "All", 1,
                         np.where(df1['objectivecode'] == df1['ivs_objective_code'], 1, 0)).copy()
        
        df1 = df1[df1["filter"] == 1]
        # remove last 4 columns as we do not need these anymore
        df1 = df1.iloc[:,:-4]
        
        df1.dropna(subset = ['project_code'], inplace = True) 
        
        df1.to_excel('df1.xlsx')
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
        
        if age in ["Not Applicable","N/A" , "Non-people"]:
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
    for ele in brackets:    
        df1[ele] = df1[ele] *df1['numerator'] *df1['equivalency']
        
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
    
    
      # In[Create summary for Demographics]
    # Create the columns for the Male Age Brackets, and calculate the values based
    # on the Male% from the country Table
    
    for demographic in brackets:
        df1['male_percentage_x'] = df1['male_percentage'].copy()
        df1['male_percentage_x'] =  np.where(df1['sex_disaggregation'] == "Male", df1[demographic],
                                    np.where(df1['sex_disaggregation'] == "Female", 0,    
                                    np.where(df1['sex_disaggregation'] == "Total", 
                                    df1[demographic] *df1['male_percentage_x'],
                                            0)))
                                      
        df1 = df1.rename(columns={"male_percentage_x":'m' + demographic})
        
    # Create the columns for the Female Age Brackets, and calculate the values based
    # on the Male% from the country Table
    
    for demographic in brackets:
        df1['f' + demographic] = np.where(df1['sex_disaggregation'] == "Female", df1[demographic],  
                                          np.where(df1['sex_disaggregation'] == "Male", 0,    
                                          np.where(df1['sex_disaggregation'] == "Total",
                                          df1[demographic]  - df1['m' + demographic]
                                                   ,0)))
    
    #Call function to create summary calculations
    
    calculate_M_F(df1)
    
    
    #%% DF2 - Caculate df2 = Max per Project Totals
    #Calculate the Max value per ivs_project_code/fy per Child Indicator, per Age Group, M & F
    
    # Demographic Brackets will be the last 55 columns in df1
    columnheadings = df1.columns.values.tolist()
    demographic_brackets = columnheadings[-55:]
    
    
    #Calculate the Max value per ivs_project_code/period per Child Indicator, per Age Group, M & F
        # Copy columns from df1 (By Indicator) to Df2 (By ivs_project_code) and remove duplicates.
    df2 = df1[grouping_list].copy()
    # Split between WFP and nonWFP projects
    # Non WFP projects will be MAXed
    
    
    
    df2 = df2[df2['funding']!= "WFP"]
    
    
    df2 = df2.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    
    
    for demographic in demographic_brackets:
            subset = (df1.groupby(grouping_list)[demographic].max())  
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
      
            #  Now we will Merge the new subset value with df2
            df2 = pd.merge(df2,subset, how = 'left', on = grouping_list)
    
    df2_nonWfP = df2.copy()
 
    
    df2 = df1[grouping_list].copy()
    
   
    # WFP projects will be SUMed
    df2 = df2[df2['funding']== "WFP"]
    
    df2 = df2.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    
    for demographic in demographic_brackets:
            subset = (df1.groupby(grouping_list)[demographic].sum())  
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
      
            #  Now we will Merge the new subset value with df2
            df2 = pd.merge(df2,subset, how = 'left', on = grouping_list)
    
    df2_WfP = df2.copy()
    

    
    # Concat the WFP and Non WFP
    df2 = pd.concat([df2_WfP, df2_nonWfP])
   
    
    calculate_M_F(df2)
    
    
    
    #%% DF3 - Group Data by ProgramCode
      #Calculate the Max value per ivs_program_code /period per Child Indicator, per Age Group, M & F. o	This is the M3 Value
    
    # Set up the list for Grouping per ProgramCode
    # For ProgramCode we add countryspecific information such as M/F % and AVG
    grouping_list_master.remove('project_code')
    grouping_list = grouping_list_master.copy()
    program_list = ['rc_number','avg_hh_size','male_percentage', 'female_percentage','under_18_percentage','over_18_percentage']
    program_list = grouping_list + program_list
    
    temp_list = grouping_list.copy()
    temp_list.remove('overlap')
    temp_list.remove('funding')
    
    df3 = df1.copy()
    df3 = df3[df3['funding']== "SPN"]
      
    df3 = df3[program_list].copy()
      
    df3 = df3.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)
    
    # exit
    #Group based on a specific subset of information 
    # temp_list = grouping_list.copy()
    # temp_list.remove('overlap')
    # temp_list.remove('funding')
    
    # Create GroupBY Seriesbased on certain columns and do the calculation in demographic column
    for demographic in demographic_brackets:
            subset = (df2.groupby(temp_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            df3 = pd.merge(df3,subset, how = 'left', on = temp_list)
    
    
    #Create new dataframe with columns in the order you want  
    #This fix was needed for somme outlier situations where not all fundingstreams 
     #were run and the column order got mixed up 
     #Make a list of all of the columns in the df
    cols = list(df3.columns.values)
    
    # Remove elements that ARE in the Groupinglist
    cols = [ele for ele in cols if ele not in grouping_list]
    
    df3 = df3[grouping_list+cols] 
    
    
    
      # In[DF3a - SPN]
    
      # In[RC#HH]
    
        # Determine the HouseHold size per Registered Child Family, based on the Countrytables
        # Get average HouseHold size per country
    
    # Multiply the Average HH with the tc_plan
    df3['avg_hh_size'] = df3['avg_hh_size'].fillna(0)
    df3['RC_HH'] = df3['avg_hh_size']*df3['rc_number']
    
      # In[Male Percentage	female_percentage	under_18_percentage	over_18_percentage for RC_HH]
    
        # RC#HH girls
    df3.insert(len(df3.columns),'RC_HH_girls',df3['RC_HH']*df3['female_percentage']*df3['under_18_percentage'])
        
        # RC#HH boys
    df3.insert(len(df3.columns),'RC_HH_boys',df3['RC_HH']*df3['male_percentage']*df3['under_18_percentage'])
        
        # RC#HH women
    df3.insert(len(df3.columns),'RC_HH_women',df3['RC_HH']*df3['female_percentage']*df3['over_18_percentage']) 
        
        # RC#HH Men
    df3.insert(len(df3.columns),'RC_HH_men',df3['RC_HH']*df3['male_percentage']*df3['over_18_percentage'])
    
    
    df3['RC_HH_under18'] = df3['RC_HH_girls'] + df3['RC_HH_boys']
    df3['RC_HH_over18'] = df3['RC_HH_women'] + df3['RC_HH_men']
    
    
      # In[M3 or RC#HH]
      
    #•	Compare the M3 Values against the RC_HH values, and determine which one is biggest
        
        # Do the sums - Under18
    df3['Male_M3_under18'] = df3[M_under18].sum(axis=1)
    df3['Female_M3_under18'] = df3[F_under18].sum(axis=1)
    df3['M3_under18Total'] = df3['Male_M3_under18'] + df3['Female_M3_under18'] 
        
        # Do the sums - Over 18
    df3['Male_M3_above18'] = df3[M_above18].sum(axis=1)
    df3['Female_M3_above18'] = df3[F_above18].sum(axis=1)
    df3['M3_over18Total'] = df3['Male_M3_above18'] + df3['Female_M3_above18']  
        
    df3['under_18_M3orRCHH'] = (np.where(df3['Female_M3_under18'] >= df3['RC_HH_girls'], "M3","RC_HH"))
    df3['over_18_M3orRCHH'] = (np.where(df3['Female_M3_above18'] >= df3['RC_HH_women'], "M3","RC_HH")) 
    
    
    # df3RC1 = df3.copy()
    
      # In[Final Male Calculations]
    
        #•	For each Area Program, per Age Group, M & F, select the bigger values of M3/RC_HH
        # Under 18
    
    #  Get %values for Under18 and Over18 per age group
    
    data = [['0y-17.9y (All children)', 0,0], ['18y-99.9y (Adults) - 18+', 0,1]]
      
    # Create the pandas DataFrame
    under_over18 = pd.DataFrame(data, columns=['age_group', 'shift', 'unique'])
    under_over18['unique'] = under_over18.index
     
    
    for index, row in under_over18.iterrows():
        age_group = (row[0])
        shift = (row[1])
        unique = (row[2])
        # Get Age %'s for each age bracket, for each line, use the NewBrcket function
        values = newBracket(age_group, shift, unique)
        # append line to the project_ages Df
        # under_over18 = under_over18.append(values)
        under_over18 = pd.concat([under_over18,values], ignore_index=True)
        
    under_over18.drop_duplicates(subset=['unique'], keep='last', inplace=True, ignore_index=True) 
    
    
    for demographic in M_under18:
            columnX = demographic.replace('m',"",1)
            under18_value = under_over18.loc[under_over18['age_group'] == '0y-17.9y (All children)', columnX][0]
            # print(under18_value)
            df3['Final_'+demographic] = (np.where(df3['under_18_M3orRCHH'] == 'M3', df3[demographic],df3["RC_HH_boys"]*under18_value))
        
        # Over 18
    for demographic in M_above18:
            columnX = demographic.replace('m',"",1)
            above18_value = under_over18.loc[under_over18['age_group'] == '18y-99.9y (Adults) - 18+', columnX][1]
            # print(above18_value)
            df3['Final_'+demographic] = (np.where(df3['over_18_M3orRCHH'] == 'M3', df3[demographic],df3["RC_HH_men"]*above18_value))
            
    
            #####    #Sum
        # Create the lists 
    Final_M_under18 = list(map('Final_'.__add__,M_under18))
    Final_M_above18 = list(map('Final_'.__add__,M_above18))    
        
        # Do the sums
    df3['Final_boys'] = df3[Final_M_under18].sum(axis=1).apply(np.floor) 
    df3['Final_men'] = df3[Final_M_above18].sum(axis=1).apply(np.floor) 
    df3['Final_male'] = df3['Final_boys'] + df3['Final_men']
    
    
      # In[Final FeMale Calculations]
        # Under 18
    for demographic in F_under18:
            columnX = demographic.replace('f',"",1)
            under18_value = under_over18.loc[under_over18['age_group'] ==  '0y-17.9y (All children)', columnX][0] 
            df3['Final_'+demographic] = (np.where(df3['under_18_M3orRCHH'] == 'M3', df3[demographic],df3["RC_HH_girls"]*under18_value))
          
        # Over 18
    for demographic in F_above18:
            columnX = demographic.replace('f',"",1)
            above18_value = under_over18.loc[under_over18['age_group'] == '18y-99.9y (Adults) - 18+', columnX][1]
            # under18_value = df_AG[columnX]['0-17.9y']
            df3['Final_'+demographic] = (np.where(df3['over_18_M3orRCHH'] == 'M3', df3[demographic],df3["RC_HH_women"]*above18_value))
         
           
        #Sum
        # Create the lists 
    Final_F_under18 = list(map('Final_'.__add__,F_under18))
    Final_F_above18 = list(map('Final_'.__add__,F_above18))    
        
        # Do the sums
    df3['Final_girls'] = df3[Final_F_under18].sum(axis=1).apply(np.floor) 
    df3['Final_women'] = df3[Final_F_above18].sum(axis=1).apply(np.floor) 
    df3['Final_female'] = df3['Final_girls'] + df3['Final_women'] 
        
      # In[Final Sum]
        
    df3['Final_under18'] = df3['Final_girls'] + df3['Final_boys'] 
    df3['Final_above18'] = df3['Final_women'] + df3['Final_men'] 
    df3['Final_total'] = df3['Final_under18'] + df3['Final_above18'] 
    
    
    #Create new dataframe with columns in the order you want  
    #This fix was needed for somme outlier situations where not all fundingstreams 
     #were run and the column order got mixed up 
     #Make a list of all of the columns in the df
    cols = list(df3.columns.values) 
    
    cols = [ele for ele in cols if ele not in grouping_list]
    
    df3 = df3[grouping_list+cols] 
    
    df3_columns = list(df3.columns.values) 
    a = len(grouping_list)
    b = a+76
    # Drop a range of columns from the table that is not needed anymore
    df3.drop(df3.iloc[:,a:b], axis=1, inplace=True)
    df3.columns = df3.columns.str.replace('Final_', '')
    
    
      # In[DF3x - AP's with OverlapCode]
    
    df3a = df3.copy() 
    # Use DF3a, as this is list of summed AP's and contains the Funding Stream per AP as well as the overlap per AP
    
    # We will now drop 'ivs_program_code' from the grouping, as we want to sum per 'overlap'
    temp_list = grouping_list.copy()
    temp_list.remove('ivs_program_code')
    # temp_list.remove('overlap')
    
    # exit()
    # Copy columns from df3a (By APCode) to (By Overlap) and remove duplicates.
    df3x = df3a[temp_list].copy()
    df3x = df3x.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)
    df3x = df3x.dropna(subset=['overlap'])
    
    
            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
        subset = (df3a.groupby(temp_list)[demographic].sum())     
        subset = pd.DataFrame(subset)
        subset = subset.reset_index()
        #  Now we will Merge the new subset value with df2
        df3x = pd.merge(df3x,subset, how = 'left', on = temp_list)
                
    
      # In[DF3b - PNS, GNT, WFP, OTH]
      
    temp_list = grouping_list.copy()
    
    temp_list.remove('overlap')
    
    df3b = df2[temp_list].copy()
    
    #  Filter out SPN and GIK
    df3b = df3b[(df3b['funding']!= "SPN")]
    df3b = df3b[(df3b['funding']!= "GIK")]
    
    # temp_list.remove('ivs_program_code')
    
        
    df3b = df3b.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)
    
    # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(temp_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            df3b = pd.merge(df3b,subset, how = 'left', on = temp_list)
    
    
      # In[DF3c - GIK]
    
    temp_list = grouping_list.copy()
    temp_list.remove('overlap')
    
    df3c = df2[temp_list].copy()
    #  Filter for only GIK
    df3c = df3c[(df3c['funding']== "GIK")]
    
    # temp_list.remove('ivs_program_code')
        
    df3c = df3c.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)
    
    # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(temp_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            df3c = pd.merge(df3c,subset, how = 'left', on = temp_list)
            
    calculate_M_F(df3)
      # In[DF3 - df3a,df3b,df3c]
      
    df3 = pd.concat([df3a, df3b, df3c])
    df3 = df3.drop('overlap', axis=1)
    
    
      # In[DF3_all - Programs per Country ]
    # We now want to SUM all per Country, so we remove some paramaters
    # Taken out because this is a simple pivot table
    # temp_list = grouping_list.copy()
    # temp_list.remove('overlap')
    # temp_list.remove('country')
    # temp_list.remove('funding')  
      
    
    # df3_sum = df3[temp_list]
    
    # df3_sum = df3_sum.drop_duplicates(subset = temp_list,keep = 'last').reset_index(drop = True)
    
        
    # # Create GroupBY Series based on demographic columns and do calculation on column
    # for demographic in demographic_brackets:
    #         subset = (df3.groupby(temp_list)[demographic].sum())           
    #         subset = pd.DataFrame(subset)
    #         subset = subset.reset_index()
    #         df3_sum = pd.merge(df3_sum,subset, how = 'left', on =temp_list)
    
    # calculate_M_F(df3_sum)
    
    #%% DF4 - PNS
    # Create Sum in same RegionCode PNS’s (currently this is identified by the OverlapCode) 
    # Use DF2, as this is list of summed projects   
    # Copy columns from df2 to Df4 (By Overlap) and remove duplicates.
    
    # Drop ivs_program_code for the rest of the script - sum/max across differnt ivs_program_codes
    grouping_list.remove('ivs_program_code')
        
    df4 = df2[grouping_list].copy()
    df4 = df4.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
        
        #  Drop non-verlapping rows
    df4 = df4.dropna(subset=['overlap'])
        
        #  Drop non-PNS rows
    df4 = df4.drop(df4[(df4["funding"] != 'PNS')].index)
        
    
            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with database
            df4 = pd.merge(df4,subset, how = 'left', on = grouping_list)
    
    calculate_M_F(df4)
    
    
    #%% DF5 - GNT
    # Create Max in same RegionCode GNT’s (currently this is identified by the OverlapCode)
     
    df5 = df2[grouping_list].copy()
    df5 = df5.drop_duplicates(
          subset = grouping_list,
          keep = 'last').reset_index(drop = True)
        
        #  Drop non-verlapping rows
    df5 = df5.dropna(subset=['overlap'])
        
        #  Drop non-PNS rows
    df5 = df5.drop(df5[(df5["funding"] != 'GNT')].index)
    
    
            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
            subset = (df2.groupby(grouping_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with df
            df5 = pd.merge(df5,subset, how = 'left', on = grouping_list)
    
    calculate_M_F(df5)
    
    
    #%% DF6 Max between DF3x, DF4, DF5
    # df6_temp = df3x.append([df4,df5], ignore_index=True)
    df6_temp = pd.concat([df3x,df4,df5], ignore_index=True)
    
    # Drop funding for the rest of the script - sum/max across differnt funding streams
    grouping_list.remove('funding')
       
    df6 = df6_temp[grouping_list].copy()
    df6 = df6.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    df6['funding'] = "Mixed"
       
            # Create GroupBY Series based on demographic columns and do calculation on column
    for demographic in demographic_brackets:
           subset = (df6_temp.groupby(grouping_list)[demographic].max())           
           subset = pd.DataFrame(subset)
           subset = subset.reset_index()
           #  Now we will Merge the new subset value with df
           df6 = pd.merge(df6,subset, how = 'left', on =grouping_list)
    
    calculate_M_F(df6)
    #%% DF7 - Sum overlapping and NonOVerlapping projects
    # Ensure the correct columns are present for DF7
    
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
    
            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in demographic_brackets:
            subset = (df7_temp.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with df2
            df7 = pd.merge(df7,subset, how = 'left', on = grouping_list)
            subset.drop
    
    calculate_M_F(df7)
    #%% DF8 - - MAX GIK with the above
    #  Get dataset with GIK projects, ensure the correct columns are listed
    df8_prep = df2.copy()
    df8_prep = df8_prep[df8_prep.funding == 'GIK']
    df8_prep = df8_prep[df7.columns]
    
        
    # Add to SPN/GNT/PNS/OTH dataset, DF7
    # df8_temp = df8_prep.append(df7, ignore_index=True)
    df8_temp = pd.concat([df8_prep,df7], ignore_index=True)
        
        # Get the Max of these values
    df8 = df8_temp[grouping_list].copy()
    df8 = df8.drop_duplicates(subset =grouping_list,keep = 'last').reset_index(drop = True)
    
            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in demographic_brackets:
            subset = (df8_temp.groupby(grouping_list)[demographic].max())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            #  Now we will Merge the new subset value with df2
            df8 = pd.merge(df8,subset, how = 'left', on = grouping_list)
            subset.drop
    
    calculate_M_F(df8)
    
    
    #%% DF9 - Sum WFP with the above 
    
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
    
            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in demographic_brackets:
            subset = (df9_temp.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            # print(subset)
            #  Now we will Merge the new subset value with df2
            df9 = pd.merge(df9,subset, how = 'left', on = grouping_list)
    
    
    calculate_M_F(df9)
    
    #%% DF10 - PVTSummary
    
    #Final Summary
    # This is executed as a pivot table, summing only the main categories
    summary_list = ['boys','girls','men','women']
    grouping_list.remove('country')
    
    # Get the Sum of these values
    df10 = df9[grouping_list].copy()
    df10 = df10.drop_duplicates(subset = grouping_list,keep = 'last').reset_index(drop = True)
    #df6['funding'] = "Mixed"
    
            # Create GroupBY Series based on demographic columns and do calculation on column    
    for demographic in summary_list:
            subset = (df9.groupby(grouping_list)[demographic].sum())           
            subset = pd.DataFrame(subset)
            subset = subset.reset_index()
            # print(subset)
            #  Now we will Merge the new subset value with df2
            df10 = pd.merge(df10,subset, how = 'left', on = grouping_list)
    
    
    df10['male']= df10['boys']+df10['men']
    df10['female']= df10['girls']+df10['women']
    df10['total']= df10['male']+df10['female']
    
    
    #%% Prep for Print all =================================================================
    df1 = df1.sort_values(by = ['period','country','project_code'], ascending = [True, True,True], na_position = 'first')  
    df2 = df2.sort_values(by = ['period','country', 'project_code'], ascending = [True, True, True], na_position = 'first')
    df3 = df3.sort_values(by = ['period','country', 'ivs_program_code'], ascending = [True,True,  True], na_position = 'first')  
    df4 = df4.sort_values(by = ['period','country', 'overlap'], ascending = [True, True,True], na_position = 'first')
    df5 = df5.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df6 = df6.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df6_temp = df6_temp.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df7 = df7.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df7_temp = df7_temp.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first') 
    df8 = df8.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df8_temp = df8_temp.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df9 = df9.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    df9_temp = df9_temp.sort_values(by = ['period','country'], ascending = [True,True], na_position = 'first')
    
    
    # Create the column heading for the summary pages
    summary_list_master = [
     "period",
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
    # total = df2['total'].sum()
    # print ("Project:  ")
    # print (total)
    
    #  #Print total as per the Country Summary 
    # total = df3['total'].sum()
    # print ("Program ")
    # print (total)
    
    #  #Print total as per the Country Summary 
    # total = df9['total'].sum()
    # print ("Country ")
    # print (total)
    
    
    #%% PRINT
    
    savepath = "results\\"
    # savepath = "testing\\"    
    ext = datetime.now().strftime('_%Y%m%d_%H%M.xlsx')
    if single_multi == "single":
        filename =  "Reach_FY23_"
    else:
        filename =  "Reach_multi_16_23"  
        
    return dfProject_summary, dfProgram_summary, dfCountry_summary, df1
    
#     writer = pd.ExcelWriter(savepath+filename+disaggregation  + ext, engine='xlsxwriter')
        
#     frames = {
#         # "PJT_Indicator" : df1,
#         # 'df2': df2,
#         # "df3" : df3,"df3a" : df3a , "df3b" : df3b,"df3c" : df3c,"df3x" : df3x,
#         # "df4" : df4,
#         #         "DF5": df5, "DF6": df6, "DF7": df7, "DF8":df8, "df9":df9, 
#                 # "PJT_summary":dfProject_summary
#                 # , "PGM_summary" : dfProgram_summary, 
#                 "Country_summary": dfCountry_summary, "PVT": dfPVT_summary
#               }    
        
    
        
#         #now loop thru and put each on a specific sheet
#     for sheet, frame in  frames.items(): # .use .items for python 3.X
#         frame.to_excel(writer, sheet_name = sheet)
        
#         #critical last step
#     # writer.save()
#     writer.close()
    

    
#     end = time.time()
#     print("Done ",disaggregation,":",
#           (end-start) /60, "min")
    
# if __name__ == '__main__':
#     #%% Get User Input
    
#     single_multi = "single"  #multi / single
#     range_from = 16
#     range_to = 23
    
#     #%% Read Data
#     # In[Set paths for reading data and add to Dataframes]
#     # [Read ITT]
    
#     start = time.time()
#     credentials = {
#     	'account' : 'pr43333.canada-central.azure',
#     	'user' :   'marthe_lotz@worldvision.ca',
#     	'authenticator' : 'externalbrowser',
#         'role' : 'DATA_INSIGHTS',
#         'warehouse' : 'DATA_INSIGHTS_WH',
#         'database'  : 'ANALYTICS_SANDBOX',
#             # 'database'  : 'IVS_DPMS_DEV',
#         'schema'    : 'INSIGHTS' ,
#     	'authenticator' : 'externalbrowser'
#     	}
    
#     session = Session.builder.configs(credentials).create()
    
#     query  = "select * from ANALYTICS_SANDBOX.INSIGHTS.HUB_TABLE"
#     sql_table = session.sql(query)
#     df1 = sql_table.to_pandas()
#     df1.columns= df1.columns.str.lower()
    
#     # df1 = pd.read_excel('data\df_HUB_upload.xlsx', sheet_name = 'Sheet1')

#     # df1.columns= df1.columns.str.lower()

    
#     #%% FILTERS    
    
#     # path = "testing\\Reach_Testdata_080923.xlsx" 
#     # df1 = pd.read_excel(path, sheet_name='data')
    
#     # TODO
#     # Remember to sort out the Sector Column
#     # Double check if ProgrammingType is coming in correctly
#     df1 = df1.rename(columns={'sector_name': 'sector'})
#     df1 = df1.rename(columns={'external_programming_type': 'programming_type'})
    
#     #Fix for SuperProgrammingType - Turn on or Off
#     #Deal with ProgrammingType For ALLRESPONSES 
#     # df1['programming_type'] = df1['programming_type'].str.replace('Crisis Response','Response') 
#     # df1['programming_type'] = df1['programming_type'].str.replace('Chronic emergencies & fragile contexts','Response')   
    
#     # df1 = df1[(df1['funding'] == "SPN")] 
#     # # df1 = df1[(df1['period'] == "FY22")|(df1['period'] == "FY23")]
#     # df1 = df1[(df1['period'] == "FY23")]
    
#     # df1 = df1[(df1['project_code'] == "PJT-SPN-191467-FY17")]
#               # |(df1['project_code'] == "PJT-WFP-220938-FY23")|
#     #            (df1['project_code'] == "PJT-PNS-213329-FY20")|(df1['project_code'] == "PJT-PNS-213304-FY20")|
#     #            (df1['project_code'] == "PJT-PNS-218520-FY22")|(df1['project_code'] == "PJT-GNT-219271-FY22")|
#     #           (df1['project_code'] == "PJT-PNS-220423-FY22")] 
  

#     query  = "select * from ANALYTICS_SANDBOX.INSIGHTS.AGELIST"
#     sql_table = session.sql(query)
#     df_agelist = sql_table.to_pandas()
#     df_agelist.columns= df_agelist.columns.str.lower()
    
    
#     query  = "select * from ANALYTICS_SANDBOX.INSIGHTS.AGEMAP"
#     sql_table = session.sql(query)
#     df_agemap = sql_table.to_pandas()
#     df_agemap.columns= df_agemap.columns.str.lower()
    
#     query  = "select * from ANALYTICS_SANDBOX.INSIGHTS.CAUSES_TEST"
#     sql_table = session.sql(query)
#     df_package = sql_table.to_pandas()
#     df_package.columns= df_package.columns.str.lower()
    
#     end = time.time()
#     print("Done reading :",
#           (end-start) /60, "min")
    
    
#     # Call the REACH calculation with different disaggregations and parameters
#     REACH('',single_multi,range_from,range_to, df1)
#     # REACH('sector',single_multi,range_from,range_to, df1)
#     # REACH('programming_type',single_multi,range_from,range_to, df1)
#     # REACH('causeid',single_multi,range_from,range_to, df1)
#     end = time.time()
#     print("DONE!! :",
#           (end-start) /60, "min")    