#  #######################################################################
#       File-Name:      data_prep_functions_v2.R
#       Version:        R 3.5.2
#       Date:           Nov 4, 2019
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        contains all functions related to data preparation
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   NONE
#       Required by:    NONE
#       Status:         APPROVED
#       Machine:        NBCU laptop
#  #######################################################################

library(tidyverse)
library(sparklyr)
library(stringr)

#::::::::::::::::::::::: Function to read the whole data and prepare based on given rules

create_raw_data <- function(inputFilePath,
                            network = "Cable",
                            group_by_cols = 'default'){
  
  #' @description this function reads data from the S3 file and prepares half-hour level data
  #' corresponding to the columns provided by the user
  #' 
  #' @param inputFilePath S3 path from where the data should be read, of the form: "s3://nbcu-ds-linear-forecasting/..."
  #' @param group_by_cols list of columns to be used as grouping variables 
  #'                      `default` is provided in either `parameters_BENT.R` or `parameters_CENT.R` file
  #' 
  #' @return list of
  #'         - R dataframe with half-hour level data according to the path and conditions provided
  #'         - R dataframe with relevant grouping information for model-details-table
  
  # set up the spark connection
  SparkR::sparkR.session()
  sc <- spark_connect(method = "databricks")
  
  # read the file from S3 and create raw data
  df_source <- spark_read_csv(sc, name = "our_data", path = inputFilePath)
  inputFile <- collect(df_source)
  inputFile$Week <- as.Date(inputFile$Week,format = "%Y-%m-%d")
  inputFile$Date <- as.Date(inputFile$Date,format = "%Y-%m-%d")
  raw_data <- inputFile %>%
    filter(is.na(Nielsen_L_Rating) == F | is.na(Nielsen_SC3_Rating) == F | Source == 'Compass') %>%
    type_convert(col_types = cols(
      Start_Time = col_character(),
      End_Time = col_character(),
      Half_Hr = col_character())
    )
  
  # get information about cycle
  cycle = substr(unique(inputFile$cycle),1,1)
  # get information from data_path
  parts1 = unlist(strsplit(x = inputFilePath,split = "/"))
  if ("pacing" %in% parts1){
    refresh = 'Pacing'
    data_version = parts1[(which(parts1 == 'pacing')+1)]
    cycle_year = paste(c("20",substr(data_version,1,2)),collapse = "")
  }else if ("cycles" %in% parts1){
    refresh = 'Planning'
    data_version = parts1[(which(parts1 == 'cycles')+2)]
    cycle = substr(data_version,3,3)
    cycle_year = paste(c("20",substr(data_version,1,2)),collapse = "")
  }else if ("adhoc" %in% parts1){
    refresh = 'Adhoc'
    data_version = parts1[(which(parts1 == 'adhoc')+1)]
    cycle_year = paste(c("20",substr(data_version,1,2)),collapse = "")
  }
  
  # readjust keep_cols and group_by_cols definitions
  if (group_by_cols == 'default'){
    extra_keep_cols = NA
    extra_group_by_cols = NA
    remove_group_by_cols = NA
    group_by_cols = gen_group_by_cols
    keep_cols = gen_keep_cols
  } else {
    tmp = setdiff(group_by_cols,gen_keep_cols)
    if (length(tmp)>0){
      extra_keep_cols = paste(tmp,collapse = ";")
      keep_cols = union(gen_keep_cols,tmp)
    } else {
      extra_keep_cols = NA
    }
    tmp = setdiff(group_by_cols,gen_group_by_cols)
    if (length(tmp)>0) extra_group_by_cols = paste(tmp,collapse = ";") else extra_group_by_cols = NA
    tmp = setdiff(gen_group_by_cols,group_by_cols)
    if (length(tmp)>0) remove_group_by_cols = paste(tmp,collapse = ";") else remove_group_by_cols = NA
  }
  
  # check for presence of columns in the data
  if (length(setdiff(keep_cols,colnames(raw_data))) > 0){
    warning("all values in keep_cols are not in the base data")
    keep_cols = intersect(keep_cols,colnames(raw_data))
  }
  if (length(setdiff(group_by_cols,colnames(raw_data))) > 0){
    warning("all grouping columns do not exist in base data")
    group_by_cols = intersect(keep_cols,group_by_cols)
  }
  
  
  # summarize the data as needed
  raw_data <- raw_data %>%
    dplyr::select(keep_cols) %>%
    group_by(.dots = group_by_cols) %>%
    summarize(L_Imps = weighted.mean(L_Imps, L_Dur, na.rm = T),
              L_Rating = weighted.mean(Nielsen_L_Rating, L_Dur, na.rm = T),
              L_Dur = sum(L_Dur, na.rm = T),
              LS_Imps = weighted.mean(LS_Imps, LS_Dur, na.rm = T),
              LS_Rating = weighted.mean(Nielsen_LS_Rating, LS_Dur, na.rm = T),
              LS_Dur = sum(LS_Dur, na.rm = T),
              SC3_Impressions = weighted.mean(SC3_Impressions, SC3_C_Dur, na.rm = T),
              C3_Rating = weighted.mean(Nielsen_SC3_Rating, SC3_C_Dur, na.rm = T),
              SC3_C_Dur = sum(SC3_C_Dur, na.rm = T),
              SC7_Impressions = weighted.mean(SC7_Impressions, SC7_C_Dur, na.rm = T),
              C7_Rating = weighted.mean(Nielsen_SC7_Rating, SC7_C_Dur, na.rm = T),
              SC7_C_Dur = sum(SC7_C_Dur, na.rm = T)
    ) %>%
    ungroup()
  
  # add new columns for BROADCAST/NBC
  if (network == 'Broadcast'){
    netname = unique(raw_data$Network) 
    check_name = (unlist(strsplit(unique(raw_data$Network),split = "_"))[1] %in% c('NBC','ABC','CBS','FOX'))
    if (netname == 'NBC_NEWS' | check_name == FALSE) stop("Broadcast should only be used for NBC/ABC/CBS/FOX network")
    # add new columns - broadcast-season number, show-season number, episode number
    raw_data = raw_data %>%
      mutate(
        Broadcast_Season = Broadcast_Year - 2012,
        Broadcast_Season = ifelse(Broadcast_Qtr == '4Q',Broadcast_Season + 1,Broadcast_Season),
        Show_Season = 0
      )
    
    raw_data = raw_data %>%
      group_by(Show_Name) %>%
      mutate(
        min_broadcast_season = min(Broadcast_Season[Broadcast_Season >= 0]),
        Show_Season = FirstAiring*(Broadcast_Season - min_broadcast_season + 1)
      )
    
    season_ep_data = raw_data %>%
      filter(FirstAiring == 1) %>%
      dplyr::select(Date, Show_Name, Show_Season, FirstAiring) %>%
      unique() %>%
      group_by(Show_Name, Show_Season, FirstAiring) %>%
      mutate(
        Show_Season_Ep = rank(Date,ties.method = "min")
      )
    
    raw_data = raw_data %>% left_join(season_ep_data,by = c("Date", "Show_Name", "Show_Season", "FirstAiring"))
    # change NA values to 0 for the new columns
    raw_data$Show_Season_Ep[is.na(raw_data$Show_Season_Ep)] = 0
    
  }
  
  raw_data = raw_data[order(raw_data$Date,raw_data$Half_Hr),]
  mdt = data.frame(refresh,data_version,cycle,cycle_year,extra_keep_cols,extra_group_by_cols,remove_group_by_cols)
  
  return(list(raw_data = raw_data,
              data_prep_details = mdt))
}


#:::::::::::::::::::::::::: Function to Prepare aggregated data 

create_aggregated_data <- function(raw_data,
                                   interactive_input = TRUE,
                                   filter_sl_nt_daypart = NA,
                                   filter_hh_nt_daypart = NA,
                                   filter_program_type = NA,
                                   filter_show_name = NA,
                                   filter_date = NA,
                                   filter_half_hr = NA,
                                   filter_cols = NA,
                                   network = "Cable",
                                   time_id,
                                   agg_group_by_cols = 'default'){
  
  #' @description this function converts half-hour level data to aggregated data, 
  #' based on the aggregation rules provided by the user
  #' 
  #' @param raw_data object returned from the `create_raw_data()` function
  #' @param interactive_input if inputs will be provided interactively, default TRUE, if FALSE enter arguments carefully
  #' @param filter_sl_nt_daypart enter all SL dayparts to be filtered, separated by semicolons
  #' @param filter_hh_nt_daypart enter all HH dayparts to be filtered, separated by semicolons
  #' @param filter_program_type enter all program-types to be filtered, separated by semicolons
  #' @param filter_show_name enter all show-names to be filtered, separated by semicolons
  #' @param filter_date enter partial dates to filter the data, appropriate format provided in GitHub guidelines
  #' @param filter_half_hr enter all half-hours to be filtered, separated by semicolons
  #' @param filter_cols enter other columns to be filtered, appropriate format provided in GitHub guidelines
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param time_id time-frame to aggregate the data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param agg_group_by_cols list of columns to be used as grouping variables 
  #'                          `default` is provided in either `parameters_BENT.R` or `parameters_CENT.R` file
  #' 
  #' @return R dataframe with aggregated data according to the path and conditions provided
  
  # preliminary checks on input arguments
  if (!network %in% c('Broadcast','Cable')){
    stop("network has to be either 'Broadcast' or 'Cable'")
  }
  if (!time_id %in% c('Week','Date','Hour','Half_Hr')){
    stop("time_id is not a valid choice")
  }
  
  # set default group_by columns
  if (time_id == 'Week'){
    gen_agg_group_by_cols = gen_weekly_group_by_cols
  } else if (time_id == 'Date'){
    gen_agg_group_by_cols = gen_daily_group_by_cols
  } else if (time_id == 'Hour'){
    gen_agg_group_by_cols = gen_hourly_group_by_cols
  } else if (time_id == 'Half_Hr'){
    gen_agg_group_by_cols = gen_halfhourly_group_by_cols
  }
  
  # readjust keep_cols and group_by_cols definitions
  if (agg_group_by_cols == 'default'){
    agg_group_by_cols = gen_agg_group_by_cols
    extra_agg_group_by_cols = NA
    remove_agg_group_by_cols = NA
  } else {
    tmp = setdiff(agg_group_by_cols,gen_agg_group_by_cols)
    if (length(tmp)>0) extra_agg_group_by_cols = paste(tmp,collapse = ";") else extra_agg_group_by_cols = NA
    tmp = setdiff(gen_agg_group_by_cols,agg_group_by_cols)
    if (length(tmp)>0) remove_agg_group_by_cols = paste(tmp,collapse = ";") else remove_agg_group_by_cols = NA
  }
  
  # check if all columns exist in the raw_data
  if (length(setdiff(agg_group_by_cols,colnames(raw_data$raw_data))) > 0){
    stop("all columns of agg_group_by_cols are not in the data")
  }
  
  filtered_data = raw_data$raw_data 
  
  if (interactive_input == T){
    # filter by sl_nt_daypart
    enter_sl_dp = readline(prompt = "Do you want to filter by any SL daypart? (y/n) ")
    if (toupper(enter_sl_dp) == "Y"){
      filter_sl_nt_daypart = readline(prompt = "Enter SL daypart(s) you want to filter, separated by semicolons: ")
      sl_dps = unlist(strsplit(x = filter_sl_nt_daypart,split = ";"))
      sl_dps = trimws(sl_dps,which = "both")
      sl_dps = intersect(sl_dps,unique(as.character(filtered_data$SL_NT_Daypart)))
      if (length(sl_dps) == 0){
        warning("Dayparts may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(SL_NT_Daypart %in% sl_dps)
      }
    }
    
    # filter by hh_nt_daypart
    enter_hh_dp = readline(prompt = "Do you want to filter by any HH daypart? (y/n) ")
    if (toupper(enter_hh_dp) == "Y"){
      filter_hh_nt_daypart = readline(prompt = "Enter HH daypart(s) you want to filter, separated by semicolons: ")
      hh_dps = unlist(strsplit(x = filter_hh_nt_daypart,split = ";"))
      hh_dps = trimws(hh_dps,which = "both")
      hh_dps = intersect(hh_dps,unique(as.character(filtered_data$HH_NT_Daypart)))
      if (length(hh_dps) == 0){
        warning("Dayparts may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(HH_NT_Daypart %in% hh_dps)
      }
    }
    
    # filter by program_type
    enter_prg = readline(prompt = "Do you want to filter by any program type? (y/n) ")
    if (toupper(enter_prg) == "Y"){
      filter_program_type = readline(prompt = "Enter program-type(s) you want to filter, separated by semicolons: ")
      prgs = unlist(strsplit(x = filter_program_type,split = ";"))
      prgs = trimws(prgs,which = "both")
      prgs = intersect(prgs,unique(as.character(filtered_data$program_type)))
      if (length(prgs) == 0){
        warning("Program-types may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(program_type %in% prgs)
      }
    }
    
    # filter by show-name
    enter_show = readline(prompt = "Do you want to filter by any show-name? (y/n) ")
    if (toupper(enter_show) == "Y"){
      filter_show_name = readline(prompt = "Enter show-name(s) you want to filter, separated by semicolons: ")
      shows = unlist(strsplit(x = filter_show_name,split = ";"))
      shows = trimws(shows,which = "both")     
      shows = intersect(shows,unique(as.character(filtered_data$Show_Name)))
      if (length(shows) == 0){
        warning("Show-names may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(Show_Name %in% shows)
      }
    }
    
    # filter by dates
    enter_date = readline(prompt = "Do you want to use partial data based on date/week? (y/n) ")
    if (toupper(enter_date) == "Y"){
      filter_date = readline(prompt = "Enter date/week you want to filter in appropriate format (refer to GitHub guidelines): ")
      tmp = gsub(pattern = "[(]",replacement = "",x = filter_date)
      tmp = gsub(pattern = "[)]",replacement = "",x = tmp)
      tmp = gsub(pattern = ";",replacement = "",x = tmp)
      fltr = paste(unlist(strsplit(x = tmp,split = "[$]")),collapse = " & ")
      filtered_data = filtered_data %>% filter(eval(parse(text = fltr)))
    }
    
    # filter by half-hours
    enter_hh = readline(prompt = "Do you want to filter by any half-hour? (y/n) ")
    if (toupper(enter_hh) == "Y"){
      half_hr_entry = readline(prompt = "Enter half-hour(s) you want to filter, separated by semicolons: ")
      hhrs = unlist(strsplit(x = half_hr_entry,split = ";"))
      hhrs = trimws(hhrs,which = "both")
      filter_half_hr = paste(c("(Half_Hr; isin; '",paste(hhrs,collapse = "','"),"')"),collapse = "")
      hhrs = intersect(unique(as.character(filtered_data$Half_Hr)),hhrs)
      if (length(hhrs) == 0){
        warning("Half-hours may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(Half_Hr %in% hhrs)
      }
    }
  } else {
    
    # filter by sl_nt_daypart
    if (is.na(filter_sl_nt_daypart) == F){
      sl_dps = unlist(strsplit(x = filter_sl_nt_daypart,split = ";"))
      sl_dps = trimws(sl_dps,which = "both")
      sl_dps = intersect(sl_dps,unique(as.character(filtered_data$SL_NT_Daypart)))
      if (length(sl_dps) == 0){
        warning("Dayparts may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(SL_NT_Daypart %in% sl_dps)
      }
    }
    # filter by hh_nt_daypart
    if (is.na(filter_hh_nt_daypart) == F){
      hh_dps = unlist(strsplit(x = filter_hh_nt_daypart,split = ";"))
      hh_dps = trimws(hh_dps,which = "both")
      hh_dps = intersect(hh_dps,unique(as.character(filtered_data$HH_NT_Daypart)))
      if (length(hh_dps) == 0){
        warning("Dayparts may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(HH_NT_Daypart %in% hh_dps)
      }
    }
    
    # filter by program_type
    if (is.na(filter_program_type) == F){
      prgs = unlist(strsplit(x = filter_program_type,split = ";"))
      prgs = trimws(prgs,which = "both")
      prgs = intersect(prgs,unique(as.character(filtered_data$program_type)))
      if (length(prgs) == 0){
        warning("Program-types may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(program_type %in% prgs)
      }
    }
    
    # filter by show-name
    if (is.na(filter_show_name) == F){
      shows = unlist(strsplit(x = filter_show_name,split = ";"))
      shows = trimws(shows,which = "both")
      shows = intersect(shows,unique(as.character(filtered_data$Show_Name)))
      if (length(shows) == 0){
        warning("Show-names may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(Show_Name %in% shows)
      }
    }
    
    # filter by dates
    if (is.na(filter_date) == F){
      tmp = gsub(pattern = "[(]",replacement = "",x = filter_date)
      tmp = gsub(pattern = "[)]",replacement = "",x = tmp)
      tmp = gsub(pattern = ";",replacement = "",x = tmp)
      fltr = paste(unlist(strsplit(x = tmp,split = "[$]")),collapse = " & ")
      filtered_data = filtered_data %>% filter(eval(parse(text = fltr)))
    }
    
    # filter by half-hours
    if (is.na(filter_half_hr) == F){
      half_hr_entry = filter_half_hr
      hhrs = unlist(strsplit(x = half_hr_entry,split = ";"))
      hhrs = trimws(hhrs,which = "both")
      filter_half_hr = paste(c("(Half_Hr; isin; '",paste(hhrs,collapse = "','"),"')"),collapse = "")
      hhrs = intersect(unique(as.character(filtered_data$Half_Hr)),hhrs)
      if (length(hhrs) == 0){
        warning("Half-hours may not have been entered correctly")
      } else {
        filtered_data = filtered_data %>% filter(Half_Hr %in% hhrs)
      }
    }
  }
  
  # creating the impressions and ratings columns using the specifications
  agg_data <- filtered_data %>%
    group_by(.dots = agg_group_by_cols) %>%
    summarize(L_Imps = weighted.mean(L_Imps, L_Dur, na.rm = T),
              L_Rating = weighted.mean(L_Rating, L_Dur, na.rm = T),
              L_Dur = sum(L_Dur, na.rm = T),
              LS_Imps = weighted.mean(LS_Imps, LS_Dur, na.rm = T),
              LS_Rating = weighted.mean(LS_Rating, LS_Dur, na.rm = T),
              LS_Dur = sum(LS_Dur, na.rm = T),
              SC3_Impressions = weighted.mean(SC3_Impressions, SC3_C_Dur, na.rm = T),
              C3_Rating = weighted.mean(C3_Rating, SC3_C_Dur, na.rm = T),
              SC3_C_Dur = sum(SC3_C_Dur, na.rm = T),
              SC7_Impressions = weighted.mean(SC7_Impressions, SC7_C_Dur, na.rm = T),
              C7_Rating = weighted.mean(C7_Rating, SC7_C_Dur, na.rm = T),
              SC7_C_Dur = sum(SC7_C_Dur, na.rm = T) 
    ) %>%
    ungroup()
  
  if (network == 'Broadcast'){
    if (!(substr(unique(agg_data$Network),1,3) %in% c('NBC', 'FOX','ABC','CBS'))) stop("Broadcast should only be used for NBC/FOX/ABC/CBS")
    # add new columns for NBC - broadcast-season number, show-season number, episode number
    agg_data = create_agg_data_BENT_addition(aggregated_data = agg_data)
  }
  
  # sorting the data based on time_id
  if (time_id == "Week"){
    agg_data <- agg_data[order(agg_data$Week),]
  }else if (time_id %in% c("Hour","Date")){
    agg_data <- agg_data[order(agg_data$Date),]
  }else if (time_id == "Half_Hr"){
    agg_data <- agg_data[order(agg_data$Date,agg_data$Half_Hr),] 
  }
  
  # creating a few new columns
  if (network == "Cable"){
    agg_data <- agg_data %>%
      mutate(
        Q1 = ifelse(Cable_Qtr == "1Q",1,0),
        Q2 = ifelse(Cable_Qtr == "2Q",1,0),
        Q3 = ifelse(Cable_Qtr == "3Q",1,0),
        Q4 = ifelse(Cable_Qtr == "4Q",1,0)
      )
  } else if (network == "Broadcast"){
    agg_data <- agg_data %>%
      mutate(
        Q1 = ifelse(Broadcast_Qtr == "1Q",1,0),
        Q2 = ifelse(Broadcast_Qtr == "2Q",1,0),
        Q3 = ifelse(Broadcast_Qtr == "3Q",1,0),
        Q4 = ifelse(Broadcast_Qtr == "4Q",1,0),
        L_LS_Ratio = LS_Rating/L_Rating,
        L_C3_Ratio = C3_Rating/L_Rating,
        L_C7_Ratio = C7_Rating/L_Rating
      )
  }
  
  # creating the model-details-output columns
  mdt = data.frame(time_id,raw_data$data_prep_details,filter_sl_nt_daypart,filter_hh_nt_daypart,filter_program_type,
                   filter_show_name,filter_date,filter_half_hr,filter_cols,extra_agg_group_by_cols,remove_agg_group_by_cols)
  
  return(list(aggregated_data = agg_data,
              data_prep_details = mdt))
}



#:::::::::::::::::::::::::: Function to Prepare model details table 

create_model_details <- function(champion,
                                 interactive_input = TRUE,
                                 n = 1, 
                                 data_prep_details,
                                 model_type = NA, 
                                 model_version = NA, 
                                 model_previous_version = NA, 
                                 model_hierarchy = NA, 
                                 forecast_rule = NA){
  
  #' @description this function outputs model details table for the top n models. The inputs are :
  #' 
  #' @param champion output of the find_champion_arima/LM function
  #' @param interactive_input if inputs will be provided interactively, default TRUE, if FALSE enter arguments carefully
  #' @param n number of top model to push to the model details table, default is 1
  #' @param data_prep_details output of the corresponding create_aggregated_data() function
  #' @param model_type input for the model-type of this set of models
  #' @param model_version input for the model-version of this set of models
  #' @param model_previous_version input for the previous version of the model for this data
  #' @param model_hierarchy input for the model-hierarchy of this set of models
  #' @param forecast_rule input for the forecast_rule of this set of models (if any subset of forecast to be used)
  
  # all column names for all three setups
  dev_cols = c('network', 'demo', 'rejected_in_staging', 'production', 'current_prod', 'push_to_staging','run_data_layers','data_version', 'model_type', 'date_added', 'datetime',
               'cycle', 'cycle_year', 'refresh', 'model_version', 'model_number','model_previous_version', 'model_mlflow_id', 'current_cycle', 'current_cycle_year', 
               'model_hierarchy', 'forecast_data', 'forecast_start_date', 'forecast_end_date', 'stream', 'time_id', 'extra_keep_cols', 'extra_group_by_cols',
               'remove_group_by_cols', 'filter_sl_nt_daypart', 'filter_hh_nt_daypart', 'filter_program_type', 'filter_show_name', 'filter_date','filter_half_hr', 
               'filter_cols', 'extra_agg_group_by_cols', 'remove_agg_group_by_cols', 'create_additional_cols', 'method', 'log_transformation','boxcox', 
               'arima_p', 'arima_d', 'arima_q', 'seasonal_p', 'seasonal_d', 'seasonal_q', 'arima_period', 'changepoints', 'regressors',
               'forecast_rule', 'library_version', 'additional_features', 'latest_base_data', 'model_detail_id','rejected_date')
  
  # stg_cols = c('network', 'demo', 'rejected_in_staging', 'production', 'current_prod', 'push_to_prod','data_version', 'model_type', 'date_added', 'datetime',
  #              'cycle', 'cycle_year', 'refresh', 'model_version', 'model_number','model_final_version','model_previous_version', 'model_mlflow_id', 'current_cycle', 'current_cycle_year', 
  #              'model_hierarchy', 'forecast_data', 'forecast_start_date', 'forecast_end_date', 'stream', 'time_id', 'extra_keep_cols', 'extra_group_by_cols',
  #              'remove_group_by_cols', 'filter_sl_nt_daypart', 'filter_hh_nt_daypart', 'filter_program_type', 'filter_show_name', 'filter_date','filter_half_hr', 
  #              'filter_cols', 'extra_agg_group_by_cols', 'remove_agg_group_by_cols', 'create_additional_cols', 'method', 'log_transformation','boxcox', 
  #              'arima_p', 'arima_d', 'arima_q', 'seasonal_p', 'seasonal_d', 'seasonal_q', 'arima_period', 'changepoints', 'regressors',
  #              'forecast_rule', 'library_version', 'additional_features', 'latest_base_data', 'model_detail_id','rejected_date')
  # 
  # prod_cols = c('network', 'demo', 'production', 'current_prod', 'data_version', 'model_type', 'date_added', 'datetime','cycle', 'cycle_year', 'refresh', 
  #               'model_version', 'model_number','model_final_version', 'model_previous_version', 'model_mlflow_id', 'current_cycle', 'current_cycle_year', 
  #               'model_hierarchy', 'forecast_data', 'forecast_start_date', 'forecast_end_date', 'stream', 'time_id', 'extra_keep_cols', 'extra_group_by_cols',
  #               'remove_group_by_cols', 'filter_sl_nt_daypart', 'filter_hh_nt_daypart', 'filter_program_type', 'filter_show_name', 'filter_date','filter_half_hr', 
  #               'filter_cols', 'extra_agg_group_by_cols', 'remove_agg_group_by_cols', 'create_additional_cols', 'method', 'log_transformation','boxcox', 
  #               'arima_p', 'arima_d', 'arima_q', 'seasonal_p', 'seasonal_d', 'seasonal_q', 'arima_period', 'changepoints', 'regressors','forecast_rule', 
  #               'library_version', 'additional_features', 'latest_base_data', 'model_detail_id')
  
  # initialize the model details table
  details <- data.frame(matrix(ncol = length(dev_cols),nrow = n))
  colnames(details) = dev_cols
  
  # set up columns that should never be created by user
  details$rejected_in_staging = 0
  details$production = 0
  details$current_prod = 0
  details$push_to_staging = 0
  details$run_data_layers = 1
  
  # set columns related to network, date, stream 
  details$network = unique(champion$champion_result$Network) 
  details$demo = unique(champion$champion_result$Demo) 
  details$date_added = Sys.Date()
  details$datetime = format(Sys.time(),"%m%d%Y_%H%M")
  details$stream = unique(champion$champion_result$Stream)
  
  # set columns related to data preparation
  details[,names(data_prep_details)] = data_prep_details
  
  # set columns related to model specifications
  details$method = champion$method
  best_models <- champion$all_model_details[order(champion$all_model_details$total_mape),][1:n,]
  modeling_cols = intersect(dev_cols,names(best_models))
  details[,modeling_cols] = best_models[,modeling_cols]
  
  # other columns
  details$model_number = 1:n
  if (interactive_input == T){
    
    enter_model_type = readline(prompt = "Do you want to enter model_type? (y/n) ")
    if (toupper(enter_model_type) == "Y") details$model_type = readline(prompt = "Enter model_type for this set of models: ")
    
    enter_model_version = readline(prompt = "Do you want to enter model_version? (y/n) ")
    if (toupper(enter_model_version) == "Y") details$model_version = readline(prompt = "Enter model_version for this set of models: ")
    
    enter_model_prev_version = readline(prompt = "Do you want to enter model_previous_version? (y/n) ")
    if (toupper(enter_model_prev_version) == "Y") details$model_previous_version = readline(prompt = "Enter model_previous_version for this set of models: ")
    
    enter_model_hierarchy = readline(prompt = "Do you want to enter model_hierarchy? (y/n) ")
    if (toupper(enter_model_hierarchy) == "Y") details$model_hierarchy = toupper(readline(prompt = "Enter model_hierarchy for this set of models: "))
    
    enter_forecast_rule = readline(prompt = "Do you want to use parts of the generated forecasts? (y/n) ")
    if (toupper(enter_forecast_rule) == "Y") details$forecast_rule = readline(prompt = "Enter forecast_rule for this set of models: ")
    
  } else {
    details$model_type = model_type
    details$model_previous_version = model_previous_version
    details$model_version = model_version
    details$model_hierarchy = toupper(model_hierarchy)
    details$forecast_rule = forecast_rule
  }
  
  # things to be added later: forecast_data, forecast_start_date, forecast_end_date, create_additional_cols
  
  return(details)
}

#::::::: Function to update MDT row with new model
update_mdt <- function(row, champion, model_version = '', n = 1){
  #' @description unction to update MDT row with new model:
  #' 
  #' @param row Old row for MDT
  #' @param champion Output of find champion ARIMA
  #' @param model_version model_version for new model (ex: P6 OR W6)
  #' @param n Number of top models to create rows
  
  dev_cols = c('network', 'demo', 'rejected_in_staging', 'production', 'current_prod', 'push_to_staging','run_data_layers','data_version', 'model_type', 'date_added', 'datetime',
               'cycle', 'cycle_year', 'refresh', 'model_version', 'model_number','model_previous_version', 'model_mlflow_id', 'current_cycle', 'current_cycle_year', 
               'model_hierarchy', 'forecast_data', 'forecast_start_date', 'forecast_end_date', 'stream', 'time_id', 'extra_keep_cols', 'extra_group_by_cols',
               'remove_group_by_cols', 'filter_sl_nt_daypart', 'filter_hh_nt_daypart', 'filter_program_type', 'filter_show_name', 'filter_date','filter_half_hr', 
               'filter_cols', 'extra_agg_group_by_cols', 'remove_agg_group_by_cols', 'create_additional_cols', 'method', 'log_transformation','boxcox', 
               'arima_p', 'arima_d', 'arima_q', 'seasonal_p', 'seasonal_d', 'seasonal_q', 'arima_period', 'changepoints', 'regressors',
               'forecast_rule', 'library_version', 'additional_features', 'latest_base_data', 'model_detail_id','rejected_date')
  
  new  <- data.frame(matrix(ncol = length(dev_cols),nrow = n))
  addcols <- intersect(dev_cols, names(row))
  new[, addcols] <- row[, addcols] 
  
  new$rejected_in_staging = 0
  new$production = 0
  new$current_prod = 0
  new$push_to_staging = 0
  new$run_data_layers = 1
  
  new$model_previous_version = row$model_final_version
  new$model_final_version    = ''
  new$model_mlflow_id = ''
  
  new$date_added = Sys.Date()
  new$datetime = format(Sys.time(),"%m%d%Y_%H%M")
  
  new$method = champion$method
  
  best_models <- champion$all_model_details[order(champion$all_model_details$total_mape),][1:n,]
  modeling_cols = intersect(dev_cols,names(best_models))
  new[,modeling_cols] = best_models[,modeling_cols]
  
  
  new$model_detail_id = ''
  new$rejected_date   = ''
  #new$model_number    = row$model_number
  new$model_version   = model_version
  
  new[is.na(new)] <- ''
  
  new <- new[, dev_cols]
  return(new)
}







#::::::::::::::::::::::: Function to create additional columns for BENT (synced-up with data layer process)

create_agg_data_BENT_addition <- function(aggregated_data){
  
  #' @description adds a few new columns for NBC, used inside the previous `create_agg_data` function
  #' @param aggregated_data dataframe created inside the previous `create_agg_data` function
  
  # assign show-dummy columns, genre-dummy columns and premiere-dummy columns
  genrelist <- read.csv(here::here("src","common_codes","BENT_genre_list.csv"))
  ALTshows = unique(subset(genrelist,GENRE == "ALT")$SHOW)
  COMshows = unique(subset(genrelist,GENRE == "COM")$SHOW)
  DRMshows = unique(subset(genrelist,GENRE == "DRM")$SHOW)
  NWSshows = unique(subset(genrelist,GENRE == "NWS")$SHOW)
  
  aggregated_data = aggregated_data %>%
    mutate(
      Genre = ifelse(rep("Genre",nrow(aggregated_data)) %in% names(aggregated_data),Genre,""),
      Show_Name = ifelse(rep("Show_Name",nrow(aggregated_data)) %in% names(aggregated_data),Show_Name,""),
      AGT = ifelse(Show_Name %in% c("AGT","AGT OTHER","AMERICAS GOT TALENT"),1,0),
      AlmaAwards = ifelse(Show_Name %in% c("ALMA AWARDS"),1,0),
      AmericanComedyAward = ifelse(Show_Name %in% c("AMER COMEDY AWARD"),1,0),
      AmericanGivingAward = ifelse(Show_Name %in% c("AMER GVNG AWDS"),1,0),
      ANW = ifelse(Show_Name %in% c("ANW"),1,0),
      APBio = ifelse(Show_Name %in% c("AP BIO"),1,0),
      BMA = ifelse(Show_Name %in% c("BMA"),1,0),
      BWRock = ifelse(Show_Name %in% c("B WILIAMS ROCKEFELER"),1,0),
      Blacklist = ifelse(Show_Name %in% c("BLACKLIST"),1,0),
      Blindspot = ifelse(Show_Name %in% c("BLINDSPOT"),1,0),
      Brave = ifelse(Show_Name %in% c("BRAVE"),1,0),
      BreedersCup = ifelse(Show_Name %in% c("BREEDERS CUP","BREEDERS CUP CHLLGE"),1,0),
      ChicagoFire = ifelse(Show_Name == 'CHICAGO FIRE',1,0),
      ChicagoMed = ifelse(Show_Name == 'CHICAGO MED',1,0),
      ChicagoPD = ifelse(Show_Name == 'CHICAGO PD',1,0),
      ChicagoSeries = ifelse(Show_Name %in% c("CHICAGO FIRE","CHICAGO JUSTICE","CHICAGO MED","CHICAGO PD"),1,0),
      ChristmasRock = ifelse(Show_Name %in% c("CMAS ROCKEFELLER"),1,0),
      Convention = ifelse(Show_Name %in% c("DECISION 12 CVNNTSN","DECISION 16 CNVNTNS"),1,0),
      Dateline = ifelse(Show_Name %in% c("DATELINE","DATELINE MYSTERY SPC","DATELINE OTHER"),1,0),
      Debate = ifelse(Show_Name %in% c("DECISION 12 DEB","DECISION 16 DEB","DEM DEBATE"),1,0),
      DebateAnalysis = ifelse(Show_Name %in% c("DECISION 12 DBT POST","DECISION 16 DBT POST","DEM RESPONSE ANLYSIS"),1,0),
      Election = ifelse(Show_Name %in% c("DECISION 12 ELEC","ELECTION","ELECTION NIGHT"),1,0),
      Emmy = ifelse(Show_Name %in% c("EMMY AWARDS"),1,0),
      FNIA = ifelse(Show_Name %in% c("FNIA","FNIA 3","FNIA WEATHR DELY SPL"),1,0),
      GoldenGlobe = ifelse(Show_Name %in% c("GOLD GLOBE AWARDS"),1,0),
      GoodGirls = ifelse(Show_Name == 'GOOD GIRLS',1,0),
      GoodPlace = ifelse(Show_Name %in% c("GOOD PLACE"),1,0),
      HollywoodGameNight = ifelse(Show_Name %in% c("HOLLYWOOD GAME NIGHT"),1,0),
      LawOrder = ifelse(Show_Name %in% c("LAW ORDER SVU","LAW ORDER TRUE CRIME"),1,0),
      LittleBigShots = ifelse(Show_Name == 'LITTLE BIG SHOTS',1,0),
      Macys4Jul = ifelse(Show_Name %in% c("MACYS 4TH JULY"),1,0),
      MacysParade = ifelse(Show_Name %in% c("MACYS THNKS PARADE"),1,0),
      Manifest = ifelse(Show_Name %in% c("MANIFEST"),1,0),
      Movies = ifelse(Show_Name %in% c("MOVIE"),1,0),
      News = ifelse(Show_Name %in% c("NBC NEWS","NBC NEWS SPC","NBC NIGHTLY NEWS"),1,0),
      NFL = ifelse(Show_Name %in% c("NFL"),1,0),
      NFLHonors = ifelse(Show_Name %in% c("NFL HONORS"),1,0),
      NFLOpenKO = ifelse(Show_Name %in% c("NFL OPEN KICKOFF SHW"),1,0),
      NFLPlayoff = ifelse(Show_Name %in% c("NFL PLAYOFF"),1,0),
      NFLPrePost = ifelse(Show_Name %in% c("NFL PRE KICK", "NFL PLAYOFF POST"),1,0),
      NFLPreSeason = ifelse(Show_Name %in% c("NFL PRESEASON"),1,0),
      NFLProBowl = ifelse(Show_Name %in% c("NFL PRO BOWL POST", "NFL PRO BOWL"),1,0),
      NHL = ifelse(Show_Name %in% c("NHL","NHL PLAYOFFS","NHL PRE GAME"),1,0),
      NotredameFtbl = ifelse(Show_Name %in% c("NOTRE DAME FOOTBALL"),1,0),
      OlympicsTrials = ifelse(Show_Name %in% c("OLY TRIALS"),1,0),
      OlympicsSpecial = ifelse(Show_Name %in% c("OLYMPICS SPC"),1,0),
      Olympics = ifelse(Show_Name %in% c("OLYMPICS","SUMMER OLY","SUMMER OLY SUS"),1,0),
      OtherSports = ifelse(Show_Name %in% c("US OPEN GOLF","US GYMNASTICS CHAMP","US FIGURE SKATING"),1,0),
      Reverie = ifelse(Show_Name == 'REVERIE',1,0),
      Rise = ifelse(Show_Name == 'RISE',1,0),
      SNL = ifelse(Show_Name %in% c("SNL","SNL SPC","SNL WKND UPDT"),1,0),
      StanleyCup = ifelse(Show_Name %in% c("STANLEY CUP LIVE","NHL STANLEY CUP LIVE"),1,0),
      StateOfAffairs = ifelse(Show_Name %in% c("STATE OF AFFAIRS"),1,0),
      StateOfUnion = ifelse(Show_Name %in% c("STATE OF UNION","STATE UNION ANALYSIS"),1,0),
      Superbowl = ifelse(Show_Name %in% c("SUPER BOWL","SB POST"),1,0),
      Superstore = ifelse(Show_Name == 'SUPERSTORE',1,0),
      Taken = ifelse(Show_Name == 'TAKEN',1,0),
      ThisIsUs = ifelse(Show_Name %in% c('THIS IS US','THIS IS US SPC'),1,0),
      TNF = ifelse(Show_Name %in% c("TNF","TNF PRE GM2"),1,0),
      TrialAndError = ifelse(Show_Name == 'TRIAL AND ERROR',1,0),
      Voice = ifelse(Show_Name == 'VOICE',1,0),
      WillAndGrace = ifelse(Show_Name == 'WILL AND GRACE',1,0),
      WinterOlympics = ifelse(Show_Name %in% c("WINTER OLY","WINTER OLY SUS"),1,0),
      WorldOfDance = ifelse(Show_Name == 'WORLD OF DANCE',1,0),
      WWE = ifelse(Show_Name %in% c("WWE TRIBUTE TROOPS","WRESTLEMANIA"),1,0),
      ALT = ifelse(Show_Name %in% ALTshows,1,0),
      COM = ifelse(Show_Name %in% COMshows,1,0),
      DRM = ifelse(Show_Name %in% DRMshows,1,0),
      NWS = ifelse(Show_Name %in% NWSshows,1,0),
      SPORTS = ifelse(is.na(Genre) == F & substr(Genre,1,1) == 'S',1,0)
    )
  
  # add shows' previous season and finale ratings
  aggregated_data = aggregated_data %>%
    mutate(
      Prev_season_L_rating = 0,
      Prev_finale_L_rating = 0
    )
  
  if ("Show_Season" %in% names(aggregated_data)){
    show_season_rtg = aggregated_data %>%
      filter(FirstAiring == 1) %>%
      group_by(Show_Name, Broadcast_Season, Show_Season, FirstAiring) %>%
      summarize(
        This_season_L_rating = weighted.mean(L_Rating, L_Dur, na.rm = T),
        This_finale_L_rating = weighted.mean(L_Rating[Date == max(Date)], L_Dur[Date == max(Date)], na.rm = T)
      ) 
    
    aggregated_data = aggregated_data %>% left_join(show_season_rtg,by = c("Show_Name", "Broadcast_Season", "Show_Season", "FirstAiring"))
    
    allshownames = unique(show_season_rtg$Show_Name)
    for (i in 1:length(allshownames)){
      tmp_idx = which(show_season_rtg$Show_Name == allshownames[i])
      if (max(show_season_rtg$Show_Season[tmp_idx]) > 1){
        idx = which(aggregated_data$Show_Name == allshownames[i] & aggregated_data$FirstAiring == 1)
        idx2 = as.numeric(lapply(aggregated_data$Broadcast_Season[idx],
                                 FUN = function(x) which(show_season_rtg$Broadcast_Season == (x-1) & show_season_rtg$Show_Name == allshownames[i])))
        aggregated_data$Prev_season_L_rating[idx] = show_season_rtg$This_season_L_rating[idx2]
        aggregated_data$Prev_finale_L_rating[idx] = show_season_rtg$This_finale_L_rating[idx2]
      }
    }
    
    # change NA values to 0 for the new columns
    aggregated_data$Prev_season_L_rating[is.na(aggregated_data$Prev_season_L_rating)] = 0
    aggregated_data$Prev_finale_L_rating[is.na(aggregated_data$Prev_finale_L_rating)] = 0
    aggregated_data$This_season_L_rating[is.na(aggregated_data$This_season_L_rating)] = 0
    aggregated_data$This_finale_L_rating[is.na(aggregated_data$This_finale_L_rating)] = 0
  }
  
  return(aggregated_data)
}




#::::::::::::::::::::::: Function to create additional columns for BENT:::: EARLIER VERSION

create_agg_data_BENT_addition2 <- function(agg_data){
  
  #' @description adds a bunch of new columns for NBC, used inside the previous `create_raw_data` function
  #' @param raw_data dataframe created inside the previous `create_raw_data` function
  
  # assign genre and performance-group information for specific shows 
  genrelist <- readxl::read_xlsx(here::here("src","common_codes","BENT_Show_Genre_List.xlsx"))
  list_of_shows = c("AGT", "ANW", "AP BIO", "BETTR LATE THAN NEVR", "BLACKLIST", "BLINDSPOT", "BLUFF CITY LAW", "BRAVE",
                    "CHICAGO FIRE", "CHICAGO MED", "CHICAGO PD", "DATELINE", "GENIUS JR", "GOOD GIRLS",
                    "GOOD PLACE", "GREAT NEWS", "LAW ORDER SVU", "LAW ORDER TRUE CRIME", "LITTLE BIG SHOTS", "MAKING IT",
                    "MANIFEST", "MARLON", "NEW AMSTERDAM", "PERFECT HARMONY", "REVERIE", "RISE", "RUNNING WILD B GRLLS", 
                    "SHADES OF BLUE", "SMALL FORTUNE", "SONGLAND", "SUNNYSIDE", "SUPERSTORE", "THATS MY JAM",
                    "TAKEN", "THIS IS US", "TIMELESS", "TRIAL AND ERROR", "VOICE", "WILL AND GRACE", "WORLD OF DANCE")
  baseline_grouping = c(4,1,2,2,2,2,1,2,
                        4,1,4,1,1,4,
                        1,1,1,2,4,1,
                        1,2,1,1,3,3,2,
                        2,1,1,1,4,1,
                        3,4,2,3,4,4,4)
  
  # assign the group column to the data
  agg_data$Group = 0
  for (i in 1:length(list_of_shows)){
    idx = which(agg_data$Show_Name == list_of_shows[i])
    agg_data$Group[idx] = baseline_grouping[i]
  }
  
  # assign show-dummy columns, genre-dummy columns and premiere-dummy columns
  NFLshows = c("NFL","NFL WEATHER DELAY")
  NHLshows = c("NHL","NHL PLAYOFFS","NHL PRE GAME")
  ALTshows = unique(subset(genrelist,GENRE == "ALT")$SHOW)
  COMshows = unique(subset(genrelist,GENRE == "COM")$SHOW)
  DRMshows = unique(subset(genrelist,GENRE == "DRM")$SHOW)
  NWSshows = unique(subset(genrelist,GENRE == "NWS")$SHOW)
  
  agg_data = agg_data %>%
    mutate(
      ALT = ifelse(Show_Name %in% ALTshows,1,0),
      COM = ifelse(Show_Name %in% COMshows,1,0),
      DRM = ifelse(Show_Name %in% DRMshows,1,0),
      NWS = ifelse(Show_Name %in% NWSshows,1,0),
      SPORTS = ifelse(is.na(Genre) == F & substr(Genre,1,1) == 'S',1,0),
      Oly14dates = ifelse(Date >= as.Date('2014-02-06') & Date <= as.Date('2014-02-23'),1,0),
      Oly16dates = ifelse(Date >= as.Date('2016-08-05') & Date <= as.Date('2016-08-21'),1,0),
      Oly18dates = ifelse(Date >= as.Date('2018-02-04') & Date <= as.Date('2018-02-25'),1,0),
      LS_SummerOly = Oly16dates,
      LS_WinterOly = Oly14dates + Oly18dates,
      AGT = ifelse(Show_Name %in% c("AGT","AGT OTHER","AMERICAS GOT TALENT"),1,0),
      AlmaAwards = ifelse(Show_Name %in% c("ALMA AWARDS"),1,0),
      AmericanComedyAward = ifelse(Show_Name %in% c("AMER COMEDY AWARD"),1,0),
      AmericanGivingAward = ifelse(Show_Name %in% c("AMER GVNG AWDS"),1,0),
      ANW = ifelse(Show_Name %in% c("ANW"),1,0),
      APBio = ifelse(Show_Name %in% c("AP BIO"),1,0),
      BMA = ifelse(Show_Name %in% c("BMA"),1,0),
      BWRock = ifelse(Show_Name %in% c("B WILIAMS ROCKEFELER"),1,0),
      Blacklist = ifelse(Show_Name %in% c("BLACKLIST"),1,0),
      Blindspot = ifelse(Show_Name %in% c("BLINDSPOT"),1,0),
      Brave = ifelse(Show_Name %in% c("BRAVE"),1,0),
      BreedersCup = ifelse(Show_Name %in% c("BREEDERS CUP","BREEDERS CUP CHLLGE"),1,0),
      ChicagoFire = ifelse(Show_Name == 'CHICAGO FIRE',1,0),
      ChicagoMed = ifelse(Show_Name == 'CHICAGO MED',1,0),
      ChicagoPD = ifelse(Show_Name == 'CHICAGO PD',1,0),
      ChicagoSeries = ifelse(Show_Name %in% c("CHICAGO FIRE","CHICAGO JUSTICE","CHICAGO MED","CHICAGO PD"),1,0),
      ChristmasRock = ifelse(Show_Name %in% c("CMAS ROCKEFELLER"),1,0),
      Convention = ifelse(Show_Name %in% c("DECISION 12 CVNNTSN","DECISION 16 CNVNTNS"),1,0),
      Dateline = ifelse(Show_Name %in% c("DATELINE","DATELINE MYSTERY SPC","DATELINE OTHER"),1,0),
      Debate = ifelse(Show_Name %in% c("DECISION 12 DEB","DECISION 16 DEB","DEM DEBATE"),1,0),
      DebateAnalysis = ifelse(Show_Name %in% c("DECISION 12 DBT POST","DECISION 16 DBT POST","DEM RESPONSE ANLYSIS"),1,0),
      Election = ifelse(Show_Name %in% c("DECISION 12 ELEC","ELECTION","ELECTION NIGHT"),1,0),
      Emmy = ifelse(Show_Name %in% c("EMMY AWARDS"),1,0),
      FNIA = ifelse(Show_Name %in% c("FNIA","FNIA 3","FNIA WEATHR DELY SPL"),1,0),
      GoldenGlobe = ifelse(Show_Name %in% c("GOLD GLOBE AWARDS"),1,0),
      GoodGirls = ifelse(Show_Name == 'GOOD GIRLS',1,0),
      GoodPlace = ifelse(Show_Name %in% c("GOOD PLACE"),1,0),
      HollywoodGameNight = ifelse(Show_Name %in% c("HOLLYWOOD GAME NIGHT"),1,0),
      LawOrder = ifelse(Show_Name %in% c("LAW ORDER SVU","LAW ORDER TRUE CRIME"),1,0),
      LittleBigShots = ifelse(Show_Name == 'LITTLE BIG SHOTS',1,0),
      Macys4Jul = ifelse(Show_Name %in% c("MACYS 4TH JULY"),1,0),
      MacysParade = ifelse(Show_Name %in% c("MACYS THNKS PARADE"),1,0),
      Manifest = ifelse(Show_Name %in% c("MANIFEST"),1,0),
      Movies = ifelse(Show_Name %in% c("MOVIE"),1,0),
      News = ifelse(Show_Name %in% c("NBC NEWS","NBC NEWS SPC","NBC NIGHTLY NEWS"),1,0),
      NFL = ifelse(Show_Name %in% c("NFL PRO BOWL POST", "NFL PRO BOWL"),1,0),
      NFLHonors = ifelse(Show_Name %in% c("NFL HONORS"),1,0),
      NFLOpenKO = ifelse(Show_Name %in% c("NFL OPEN KICKOFF SHW"),1,0),
      NFLPlayoff = ifelse(Show_Name %in% c("NFL PLAYOFF"),1,0),
      NFLPrePost = ifelse(Show_Name %in% c("NFL PRE KICK", "NFL PLAYOFF POST"),1,0),
      NFLPreSeason = ifelse(Show_Name %in% c("NFL PRESEASON"),1,0),
      NFLProBowl = ifelse(Show_Name %in% c("NFL","NFL WEATHER DELAY"),1,0),
      NHL = ifelse(Show_Name %in% NHLshows,1,0),
      NotredameFtbl = ifelse(Show_Name %in% c("NOTRE DAME FOOTBALL"),1,0),
      OlympicsTrials = ifelse(Show_Name %in% c("OLY TRIALS"),1,0),
      OlympicsSpecial = ifelse(Show_Name %in% c("OLYMPICS SPC"),1,0),
      Olympics = ifelse(Show_Name %in% c("OLYMPICS","SUMMER OLY","SUMMER OLY SUS"),1,0),
      Reverie = ifelse(Show_Name == 'REVERIE',1,0),
      Rise = ifelse(Show_Name == 'RISE',1,0),
      SNL = ifelse(Show_Name %in% c("SNL","SNL SPC","SNL WKND UPDT"),1,0),
      OtherSports = ifelse(Show_Name %in% c("US OPEN GOLF","US GYMNASTICS CHAMP","US FIGURE SKATING"),1,0),
      StanleyCup = ifelse(Show_Name %in% c("STANLEY CUP LIVE","NHL STANLEY CUP LIVE"),1,0),
      StateOfAffairs = ifelse(Show_Name %in% c("STATE OF AFFAIRS"),1,0),
      StateOfUnion = ifelse(Show_Name %in% c("STATE OF UNION","STATE UNION ANALYSIS"),1,0),
      Superbowl = ifelse(Show_Name %in% c("SUPER BOWL","SB POST"),1,0),
      Superstore = ifelse(Show_Name == 'SUPERSTORE',1,0),
      Taken = ifelse(Show_Name == 'TAKEN',1,0),
      ThisIsUs = ifelse(Show_Name == 'THIS IS US',1,0),
      TNF = ifelse(Show_Name %in% c("TNF","TNF PRE GM2"),1,0),
      TrialAndError = ifelse(Show_Name == 'TRIAL AND ERROR',1,0),
      Voice = ifelse(Show_Name == 'VOICE',1,0),
      WillAndGrace = ifelse(Show_Name == 'WILL AND GRACE',1,0),
      WinterOlympics = ifelse(Show_Name %in% c("WINTER OLY","WINTER OLY SUS"),1,0),
      WorldOfDance = ifelse(Show_Name == 'WORLD OF DANCE',1,0),
      WWE = ifelse(Show_Name %in% c("WWE TRIBUTE TROOPS","WRESTLEMANIA"),1,0),
      Jan_Prm = Jan*Premiere,
      Feb_Prm = Feb*Premiere,
      Mar_Prm = Mar*Premiere,
      Apr_Prm = Apr*Premiere,
      May_Prm = May*Premiere,
      Jun_Prm = Jun*Premiere,
      Jul_Prm = Jul*Premiere,
      Aug_Prm = Aug*Premiere,
      Sep_Prm = Sep*Premiere,
      Oct_Prm = Oct*Premiere,
      Nov_Prm = Nov*Premiere,
      Dec_Prm = Dec*Premiere,
      Sun_Prm = Sun*Premiere,
      Mon_Prm = Mon*Premiere,
      Tue_Prm = Tue*Premiere,
      Wed_Prm = Wed*Premiere,
      Thu_Prm = Thu*Premiere,
      Fri_Prm = Fri*Premiere,
      Sat_Prm = Sat*Premiere,
      Enc_Prm = (1-FirstAiring)*Premiere,
      average_performer = ifelse(Group == 1,1,0),
      slight_underperformer = ifelse(Group == 2,1,0),
      underperformer = ifelse(Group == 3,1,0),
      overperformer = ifelse(Group == 4,1,0)
    )
  
  # add broadcast-season number, show-season number, shows' previous season and finale ratings
  agg_data = agg_data %>%
    mutate(
      Prev_season_L_rating = 0,
      Prev_finale_L_rating = 0
    )
  
  show_season_rtg = agg_data %>%
    filter(FirstAiring == 1) %>%
    group_by(Show_Name, Broadcast_Season, Show_Season, FirstAiring) %>%
    summarize(
      This_season_L_rating = mean(L_Rating),
      This_finale_L_rating = L_Rating[length(L_Rating)]
    ) 
  
  agg_data = agg_data %>% left_join(show_season_rtg,by = c("Show_Name", "Broadcast_Season", "Show_Season", "FirstAiring"))
  
  allshownames = unique(show_season_rtg$Show_Name)
  for (i in 1:length(allshownames)){
    tmp_idx = which(show_season_rtg$Show_Name == allshownames[i])
    if (max(show_season_rtg$Show_Season[tmp_idx]) > 1){
      idx = which(agg_data$Show_Name == allshownames[i] & agg_data$FirstAiring == 1)
      idx2 = as.numeric(lapply(agg_data$Broadcast_Season[idx],
                               FUN = function(x) which(show_season_rtg$Broadcast_Season == (x-1) & show_season_rtg$Show_Name == allshownames[i])))
      agg_data$Prev_season_L_rating[idx] = show_season_rtg$This_season_L_rating[idx2]
      agg_data$Prev_finale_L_rating[idx] = show_season_rtg$This_finale_L_rating[idx2]
    }
  }
  
  # change NA values to 0 for the new columns
  agg_data$Prev_season_L_rating[is.na(agg_data$Prev_season_L_rating)] = 0
  agg_data$Prev_finale_L_rating[is.na(agg_data$Prev_finale_L_rating)] = 0
  agg_data$This_season_L_rating[is.na(agg_data$This_season_L_rating)] = 0
  agg_data$This_finale_L_rating[is.na(agg_data$This_finale_L_rating)] = 0
  
  return(agg_data)
}


#:::::::::::::::::::::::::: Function to Translate create_additional_columns syntax into R syntax

generate_R_syntax <- function(additional_columns){
  
  #' @description This function takes in the syntax from model details column 'create_additional_cols' and outputs the R
  #' code in a string to actually create this new column in the data. The details of the model detials column syntax can be  
  #' found here: https://github.com/nbcu-ds/r_forecasting_exp/blob/prod/guidelines/model_details_guidelines.md
  #' 
  #' @param additional_columns This is the model details create_additional_cols string i.e. "(LS_Jan_2017:NONE: (Week; >= ; '2017-01-01'))"
  
  # Separate by delimeter '$' and store in a list
  shifts <- as.list(strsplit(additional_columns,'$', fixed=TRUE)[[1]])
  finalStatementVector = c()
  # Iterate through each conditional statement
  for(shift in shifts){
    statement = ""
    temp = str_replace_all(shift, c("\\(" = "", "\\)" = ""," " = ""))
    statementVector <- c()
    # split conditions, determine OR || or AND &&
    parts <- as.list(strsplit(temp,':', fixed=TRUE)[[1]])
    name = parts[[1]] # LS_4Q17
    type = parts[[2]] # AND
    conditions <- as.list(strsplit(parts[[3]],'%', fixed=TRUE)[[1]]) # (Week; >; '2017-11-30') % (Week; < ; '2018-02-01'))
    statementStart <- paste(c(name," = ifelse("), collapse = "")
    # For each condition create the necessary statement
    for(condition in conditions) {
      # Split codition by delimeter ';'
      inner <- as.list(strsplit(condition,';', fixed=TRUE)[[1]])
      column = inner[[1]]
      relation = inner[[2]]
      value = inner[[3]]
      if (column == "Week" || column == "Date") {
        value = str_replace_all(value, "'","")
        value = paste("as.Date('",value,"')", collapse="")
        value = str_replace_all(value, " ","")
      }
      if(relation == "=") {
        relation = "=="
      }
      if(type == "AND"){
        type = "&&"
      }else if(type == "OR"){
        type = "||" 
      }
      tempCondition = paste(c(column, relation, value), collapse = "")
      if (relation %in% c("isin", "notin")) {
        values = as.list(strsplit(inner[[3]], ",", fixed="TRUE")[[1]])
        for(i in 1:length(values)){
          values[i] = str_replace_all(values[i], "'","")
          values[i] = paste("as.Date('",values[i],"')", collapse="")
          values[i] = str_replace_all(values[i], " ","")
        }
        value = paste(values, collapse=", ")
      }
      if (relation %in% c("isin", "notin", "contains", "not_contains", "starts_with", "not_starts_with")) {
        tempCondition = switch(relation,
                               "isin" = paste(column, "%in% c(", value, ")", collapse = ""),
                               "notin" = paste("!", column, "%in% c(", value, ")", collapse = ""), # and add ! in front of whole expression
                               "contains" = paste(column, "%>% str_extract(", value, ")", collapse = ""),
                               "not_contains" = paste("!", column, "%>% str_extract(", value, ")", collapse = ""),
                               "starts_with" = paste(column, "%>% startsWith(", value,")", collapse =""),
                               "not_starts_with" = paste("!", column, "%>% startsWith(", value,")", collapse ="")
        )
      }
      statementVector = append(statementVector, tempCondition)
    }
    statement = paste(statementStart,unlist(paste(statementVector, collapse=paste(type))),",1,0)", sep="")
    if(type == "MATH"){
      inner <- as.list(strsplit(condition,';', fixed=TRUE)[[1]])
      column = inner[[1]]
      relation = inner[[2]]
      value = inner[[3]]
      # add_c, add_f, sub_c, sub_f, mult_c, mult_f, div_c and div_f
      operation = substr(relation,1,3)
      operation = switch(operation,
                         "add" = "+",
                         "sub" = "-",
                         "mul" = "*",
                         "div" = "/"
      )
      statement = paste(name, "=",column, operation, value, collapse = "" )
    }
    finalStatementVector = append(finalStatementVector, statement)
  }
  finalStatement = paste("data = mutate( data,",paste(finalStatementVector, collapse=", "), ")",collapse="")
  return (finalStatement)
}


#:::::::::::::::::::::::::: Function to check mdt syntax for create_additional_columns and apply to case_data

test_additional_cols <- function(case_data, new_columns){
  
  #' @description This function is used during the modeling process and takes in case_data and new_columns to create new columns
  #' and regressors. This function returns the case_data with the new colums created in the data and the new_column string added to 
  #' the data_prep_details under create_additional_cols (like model details column will accept).
  #' 
  #' @param case_data This is the case_data object used during the modeling process
  #' @param new_columns This is the proposed string the be added as a create_additional_cols string to data_prep_details
  #' 
  #' @note this function can be improved in the future but throwing specific errors bases off mistakes in the mdt syntax:
  #' 1) Throw an error if the syntax has AND instead of NONE or an incorrect statement type (currently no error)
  #' 2) Throw more specific errors if semicolons, parenthesis, etc are missing (currently generic error)
  
  # Get the r syntax from generate_R_syntax function
  data = case_data$aggregated_data
  mutate_string = generate_R_syntax(new_columns)
  
  # use the eval function to run the string returned as code to create the new column/s
  eval(parse(text=mutate_string))
  print("R code generated to create additional column: ")
  print(mutate_string)
  # Evaluate the string, if no errors occur then the new columns syntax works and will be added to model details
  case_data$aggregated_data = data
  if(is.na(case_data$data_prep_details$create_additional_cols)){
    case_data$data_prep_details$create_additional_cols <- new_columns
  } else {
    case_data$data_prep_details$create_additional_cols <- paste(case_data$data_prep_details$create_additional_cols, new_columns, sep = "$")
  }
  return (case_data)
}