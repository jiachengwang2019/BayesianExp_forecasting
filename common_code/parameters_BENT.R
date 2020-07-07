#  #######################################################################
#       File-Name:      parameters_BENT.R
#       Version:        R 3.5.2
#       Date:           Nov 4, 2019
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        contains all grouping definitions related to data preparation for BENT
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   NONE
#       Required by:    NONE
#       Status:         APPROVED
#       Machine:        NBCU laptop
#  #######################################################################


# Columns for grouping raw data
gen_group_by_cols = c('Source','Network', 'Demo', 'SL_NT_Daypart','Broadcast_Year','Broadcast_Qtr','Week','Date','hourly',
                      'Half_Hr','Start_Time','Show_Name','NBC_Show_Name', 'ShowName_Orig','program_type',
                      'Genre','FirstAiring','Premiere','Finale','daylight_indi','Season_num','Episode','Episode_num','Epi_order','LF_Season',
                      'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun','Jul', 'Aug','Sep','Oct', 'Nov', 'Dec', 
                      'New_Years_Eve_ind','Memorial_Day_week_ind','Memorial_Day_ind','Halloween_ind',              
                      'Thanksgiving_ind','Christmas_week_ind','Christmas_ind','Christmas_week_bfr_ind',    
                      'Halloween_week_ind','Thanksgiving_week_ind','Independence_Day_week_ind','Easter_week_ind',             
                      'New_Years_Eve_week_ind','Easter_ind','Independence_Day_ind','Thanksgiving_week_aft_ind',
                      'DOW', 'Mon','Tue', 'Wed', 'Thu','Fri','Sat','Sun')

# Columns for grouping model-specific data (organized by time)
wkly_exclusions = c('Christmas_ind', 'Date','Easter_ind','Epi_order','Episode','Episode_num','Finale','FirstAiring',
                    'Genre','Half_Hr','Halloween_ind','Independence_Day_ind','LF_Season','Memorial_Day_ind',
                    'NBC_Show_Name','New_Years_Eve_ind','Premiere','Season_num','ShowName_Orig','Show_Name',
                    'Start_Time','Thanksgiving_ind','daylight_indi','hourly','program_type',
                    'DOW', 'Mon','Tue', 'Wed', 'Thu','Fri','Sat','Sun')
gen_weekly_group_by_cols = union(gen_group_by_cols[! gen_group_by_cols %in% wkly_exclusions],"Broadcast_Season")

daily_exclusions = c('Epi_order','Episode','Episode_num','Finale','FirstAiring','Genre','Half_Hr',
                     'LF_Season','NBC_Show_Name','Premiere','Season_num','ShowName_Orig','Show_Name',
                     'Start_Time','hourly','program_type')
gen_daily_group_by_cols = union(gen_group_by_cols[! gen_group_by_cols %in% daily_exclusions],"Broadcast_Season")

hrly_exclusions = c('Finale', 'Half_Hr', 'LF_Season', 'Start_Time')
gen_hourly_group_by_cols = union(gen_group_by_cols[! gen_group_by_cols %in% hrly_exclusions],c("Broadcast_Season","Show_Season","Show_Season_Ep"))

hfhrly_exclusions = c('Start_Time')
gen_halfhourly_group_by_cols = union(gen_group_by_cols[! gen_group_by_cols %in% hfhrly_exclusions],c("Broadcast_Season","Show_Season","Show_Season_Ep"))


# Columns to keep from raw data
other_cols = c('End_Time','LS_Dur','LS_Imps','L_Dur','L_Imps','Nielsen_LS_Rating','Nielsen_L_Rating','Nielsen_SC3_Rating','SC3_C_Dur','SC3_Impressions',
               'Nielsen_SC7_Rating','SC7_C_Dur','SC7_Impressions','Tot_UE')
gen_keep_cols = union(gen_group_by_cols,other_cols)