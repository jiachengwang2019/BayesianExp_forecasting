#  #######################################################################
#       File-Name:      parameters_CENT.R
#       Version:        R 3.5.2
#       Date:           Nov 4, 2019
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        contains all grouping definitions related to data preparation for CENT
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
gen_group_by_cols = c('Source','Network', 'Demo', 'HH_NT_Daypart','SL_NT_Daypart', 'Broadcast_Year','Cable_Qtr',
                      'Broadcast_Qtr','Week','Date', 'hourly','Half_Hr','Start_Time','Show_Name','program_type',
                      'FirstAiring', 'Premiere', 'Finale','daylight_indi','Season_num','Episode','Episode_num','Epi_order','LF_Season',
                      'Jan', 'Feb', 'Mar', 'Apr','May', 'Jun', 'Jul', 'Aug','Sep','Oct', 'Nov', 'Dec', 
                      'New_Years_Eve_ind', 'Memorial_Day_week_ind','Memorial_Day_ind','Halloween_ind','Columbus_Day_ind','Columbus_Day_week_ind',
                      'Thanksgiving_ind','Christmas_week_ind','Christmas_ind','Christmas_week_bfr_ind','Halloween_week_ind',
                      'Thanksgiving_week_ind','Independence_Day_week_ind','Easter_week_ind','New_Years_Eve_week_ind',
                      'Easter_ind','Independence_Day_ind','Thanksgiving_week_aft_ind', 'DOW',
                      'NBC_Show_Name', 'ShowName_Orig', 'Mon','Tue', 'Wed', 'Thu','Fri','Sat','Sun')

# Columns for grouping model-specific data (organized by time)
wkly_exclusions = c('Show_Name', 'NBC_Show_Name', 'ShowName_Orig', 'program_type','Date','Half_Hr','hourly', 'Start_Time',
                    'FirstAiring','Premiere', 'Finale','Season_num','Episode','Episode_num','Epi_order','LF_Season','daylight_indi',
                    'Christmas_ind','Easter_ind','Halloween_ind','Independence_Day_ind', 'Memorial_Day_ind','Columbus_Day_ind',
                    'New_Years_Eve_ind','Thanksgiving_ind', 'DOW','Mon','Tue', 'Wed', 'Thu','Fri','Sat','Sun')
gen_weekly_group_by_cols = gen_group_by_cols[! gen_group_by_cols %in% wkly_exclusions]

daily_exclusions = c('Show_Name', 'NBC_Show_Name', 'ShowName_Orig', 'program_type', 'Half_Hr','hourly', 'Start_Time',
                     'FirstAiring','Premiere', 'Finale','Season_num','Episode','Episode_num','Epi_order','LF_Season')
gen_daily_group_by_cols = gen_group_by_cols[! gen_group_by_cols %in% daily_exclusions]

hrly_exclusions = c('NBC_Show_Name','ShowName_Orig','Half_Hr','Season_num','Episode','Episode_num','Epi_order','LF_Season', 'Start_Time')
gen_hourly_group_by_cols = gen_group_by_cols[! gen_group_by_cols %in% hrly_exclusions]

hfhrly_exclusions = c('NBC_Show_Name','ShowName_Orig','Start_Time')
gen_halfhourly_group_by_cols = gen_group_by_cols[! gen_group_by_cols %in% hfhrly_exclusions]


# Columns to keep from raw data
other_cols = c('End_Time','LS_Dur','LS_Imps','L_Dur','L_Imps','Nielsen_LS_Rating','Nielsen_L_Rating','Nielsen_SC3_Rating','SC3_C_Dur','SC3_Impressions',
               'Nielsen_SC7_Rating','SC7_C_Dur','SC7_Impressions','Tot_UE')
gen_keep_cols = union(gen_group_by_cols,other_cols)