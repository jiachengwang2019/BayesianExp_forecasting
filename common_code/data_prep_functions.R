#  #######################################################################
#       File-Name:      data_prep_functions.R
#       Version:        R 3.4.4
#       Date:           Feb 13, 2019
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        contains all functions related to data preparation for
#                       model inputs
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

#::::::::::::::::::::::: Function to read the whole data and prepare based on given rules

create_raw_data <- function(inputFileName,
                            inputFilePath,
                            keep_cols,
                            group_by_cols,
                            additional_filter = NULL){
  
  #' @description this function reads data from the S3 file and prepares half-hour level data
  #' corresponding to the columns provided by the user
  #' 
  #' @param inputFileName name to be assigned to the file read from S3
  #' @param inputFilePath S3 path from where the data should be read, of the form: "/mnt/nbcu-ds-linear-forecasting/..."
  #' @param keep_cols list of columns needed to be kept in the data (provided usually in the `parameters.R` file)
  #' @param group_by_cols list of columns to be used as grouping variables (provided usually in the `parameters.R` file)
  #' @param additional_filter any additional condition to be used while preparing the data
  #' 
  #' @return R dataframe with half-hour level data according to the path and conditions provided
  
  # set up the spark connection
  SparkR::sparkR.session()
  sc <- spark_connect(method = "databricks")
  
  # read the file from S3
  df_source <- spark_read_csv(sc, name = inputFileName, path = inputFilePath)
  inputFile <- collect(df_source)
  inputFile$Week <- as.Date(inputFile$Week,format = "%Y-%m-%d")
  inputFile$Date <- as.Date(inputFile$Date,format = "%Y-%m-%d")
  
  if (length(additional_filter)>0){
    filter_txt = paste(additional_filter,collapse = " & ")
    raw_data <- inputFile %>%
      type_convert(col_types = cols(
        Start_Time = col_character(),
        End_Time = col_character(),
        Half_Hr = col_character())
      ) %>%
      filter(eval(parse(text = filter_txt)))
  } else {
    raw_data <- inputFile %>%
      type_convert(col_types = cols(
        Start_Time = col_character(),
        End_Time = col_character(),
        Half_Hr = col_character())
      )
  }
  
  # summarize the data as needed
  raw_data <- raw_data %>%
    dplyr::select(keep_cols) %>%
    group_by(.dots = group_by_cols) %>%
    summarize(LS_Imps = weighted.mean(LS_Imps, LS_Dur),
              LS_Rating = weighted.mean(Nielsen_LS_Rating, LS_Dur),
              LS_Dur = sum(LS_Dur),
              SC3_Impressions = weighted.mean(SC3_Impressions, SC3_C_Dur),
              C3_Rating = weighted.mean(Nielsen_SC3_Rating, SC3_C_Dur),
              SC3_C_Dur = sum(SC3_C_Dur)
    ) %>%
    ungroup()
  
  raw_data = raw_data[order(raw_data$Date,raw_data$Half_Hr),]
  
  return(raw_data)
}


#:::::::::::::::::::::::::: Function to Prepare aggregated data 

create_aggregated_data <- function(raw_data,
                                   filter_text = NULL,
                                   network = "Cable",
                                   agg_timescale = "Week",
                                   agg_group_by_cols){
  
  #' @description this function converts half-hour level data to aggregated data, 
  #' based on the aggregation rules provided by the user
  #' 
  #' @param raw_data R dataframe returned from the `create_raw_data()` function
  #' @param filter_text conditions to be used for subsetting the raw_data
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param agg_timescale time-frame to aggregate the data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param additional_filter any additional condition to be used while preparing the data
  #' 
  #' @return R dataframe with aggregated data according to the path and conditions provided
  
  if (!network %in% c('Broadcast','Cable')){
    stop("network has to be either 'Broadcast' or 'Cable'")
  }
  if (!agg_timescale %in% c('Week','Date','Hour','Half_Hr')){
    stop("agg_timescale is not a valid choice")
  }
  
  if (length(filter_text)>0){
    raw_data <- raw_data %>% filter(eval(parse(text = filter_text)))
  }
  agg_data <- raw_data %>%
    group_by(.dots = agg_group_by_cols) %>%
    summarize(LS_Imps = weighted.mean(LS_Imps, LS_Dur),
              LS_Rating = weighted.mean(LS_Rating, LS_Dur),
              LS_Dur = sum(LS_Dur),
              SC3_Impressions = weighted.mean(SC3_Impressions, SC3_C_Dur, na.rm = T),
              C3_Rating = weighted.mean(C3_Rating, SC3_C_Dur, na.rm = T),
              SC3_C_Dur = sum(SC3_C_Dur)
    ) %>%
    ungroup()
  if (agg_timescale == "Week"){
    agg_data <- agg_data[order(agg_data$Week),]
  }else if (agg_timescale %in% c("Hour","Date")){
    agg_data <- agg_data %>%
      mutate(Day_of_Week = weekdays(Date),
             Sun = ifelse(Day_of_Week == "Sunday",1,0),
             Mon = ifelse(Day_of_Week == "Monday",1,0),
             Tue = ifelse(Day_of_Week == "Tuesday",1,0),
             Wed = ifelse(Day_of_Week == "Wednesday",1,0),
             Thu = ifelse(Day_of_Week == "Thursday",1,0),
             Fri = ifelse(Day_of_Week == "Friday",1,0),
             Sat = ifelse(Day_of_Week == "Saturday",1,0)
      )
    agg_data <- agg_data[order(agg_data$Date),]
  }else if (agg_timescale == "Half_Hr"){
    agg_data <- agg_data %>%
      mutate(Day_of_Week = weekdays(Date),
             Sun = ifelse(Day_of_Week == "Sunday",1,0),
             Mon = ifelse(Day_of_Week == "Monday",1,0),
             Tue = ifelse(Day_of_Week == "Tuesday",1,0),
             Wed = ifelse(Day_of_Week == "Wednesday",1,0),
             Thu = ifelse(Day_of_Week == "Thursday",1,0),
             Fri = ifelse(Day_of_Week == "Friday",1,0),
             Sat = ifelse(Day_of_Week == "Saturday",1,0)
      )
    agg_data <- agg_data[order(agg_data$Date,agg_data$Half_Hr),] 
  }
  
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
        Q4 = ifelse(Broadcast_Qtr == "4Q",1,0)
      )
  }
  
  return(agg_data)
}



#:::::::::::::::::::::::::: Function to Prepare model details table 

create_model_details <- function(champion,
                                 data_path,
                                 params, 
                                 filter_list, 
                                 n = 1, 
                                 prev_details = NULL){
  
  #' @description this function outputs model details table for the top n models. The inputs are :
  #' 
  #' @param champion: Output of the find_champion_arima/LM function
  #' @param params: Named list with the following parameters:
  #' data_version, cycle, cycle_year, refresh, model_version, model_hierarchy, model_type, forecast_data,
  #' data_version, forecast_start_date, forecast_end_date, stream, time_id,  model_version, model_hierarchy,
  #' model_type, extra_keep_cols, remove_group_by_cols, extra_group_by_cols, create_additional_cols, method, additional_features, 
  #' forecast_rule, extra_agg_group_by_cols, remove_agg_group_by_cols, model_version , model_previous_version
  #' @param n: Number of top model to push to the model details table (top n from find_champion_arima)
  #' @param filter_list: Named list with all the columns to filter. Ex:
  #' filter_list <- list(HH_NT_Daypart = 'Weekday Late;Business Day', Half_Hr ="01:00:00;01:30:00;02:00:00", 
  #' Date = '(Date; >=; "2019-01-02")',filter_cols = "(FirstAiring;=;0)$(Show_Name;starts_with;'DEAL')" )
  #' See (https://github.com/nbcu-ds/r_forecasting_exp/blob/prod/guidelines/model_details_guidelines.md) for details
  
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
  # details$current_cycle = ''
  # details$current_cycle_year = ''
  # details$latest_base_data = ''
  
  # get information from data_path
  parts1 = unlist(strsplit(x = data_path,split = "/"))
  if ("pacing" %in% parts1){
    details$refresh = 'Pacing'
    details$data_version = parts1[(which(parts1 == 'pacing')+1)]
  }else if ("cycles" %in% parts1){
    details$refresh = 'Planning'
    details$data_version = parts1[(which(parts1 == 'cycles')+2)]
    details$cycle = substr(details$data_version,3,3)
    details$cycle_year = paste(c("20",substr(details$data_version,1,2)),collapse = "")
  }else if ("adhoc" %in% parts1){
    details$refresh = 'Adhoc'
    details$data_version = parts1[(which(parts1 == 'adhoc')+1)]
  }
  
  # set columns related to network, date, stream 
  details$network = unique(champion$champion_result$Network) 
  details$demo = unique(champion$champion_result$Demo) 
  details$date_added = Sys.Date()
  details$datetime = format(Sys.time(),"%m%d%Y_%H%M")
  details$stream = unique(champion$champion_result$Stream)
  
  # set columns related to model specifications
  models <- champion$all_model_details[order(champion$all_model_details), ][1:n, ] 
  details$method = champion$method
  
  col_list = c("data_version", "cycle", "cycle_year", "refresh", "model_version", "model_hierarchy", "model_type", "forecast_data",
               "data_version", "forecast_start_date", "forecast_end_date", 'stream', 'time_id',  "model_version", "model_hierarchy",
               "model_type", 'extra_keep_cols', 'remove_group_by_cols', 'extra_group_by_cols', 'create_additional_cols', 'method', "additional_features", 
               "forecast_rule", 'extra_agg_group_by_cols', 'remove_agg_group_by_cols', "model_version" , "model_previous_version")
  
  for (col in col_list){
    details[[col]] <- ifelse(col %in% names(params), params[[col]], stop(paste('Incorrect/undefined ', col, ' option', sep ="")))
  }
  
  
  
  details$filter_sl_nt_daypart = ifelse(("SL_NT_Daypart" %in% names(filter_list)), filter_list$SL_NT_Daypart, '')
  details$filter_hh_nt_daypart = ifelse(("HH_NT_Daypart" %in% names(filter_list)), filter_list$HH_NT_Daypart, '')
  details$filter_program_type  = ifelse(("program_type" %in% names(filter_list)), filter_list$program_type, '')
  details$filter_show_name     = ifelse('Show_Name' %in% names(filter_list), filter_list$Show_Name, '')
  details$filter_date          = ifelse('Date' %in% names(filter_list), filter_list$Date, '')
  
  details$filter_half_hr      = ifelse('Half_Hr' %in% names(filter_list), 
                                       paste('(Half_Hr;isin;', gsub(";",",",toString(filter_list$Half_Hr)),')', sep='') , '')
  details$filter_cols = ifelse('filter_cols' %in% names(filter_list), filter_list$filter_cols, '')
  
  details$boxcox  = '' 
  
  
  
  models <- models[, c("log_transformation","arima_p", "arima_d", "arima_q", 
                       "seasonal_P", "seasonal_D", "seasonal_Q", "arima_period", "changepoints", "regressors")]
  
  details = cbind(rbind(details, details[rep(1, n-1), ]), models)
  names(details ) <- tolower(names(details))
  
  details$ID <- seq.int(nrow(details))
  details$model_number  = paste(details$model_version, ".", details$ID , sep="")
  
  details$model_detail_id = ''
  details$library_version = ''
  details$model_mlflow_id = ''
  
  details = details[,c('network', 'demo', 'production', 'current_prod', 'data_version', 'model_type', 'model_number',  'datetime', 'cycle', 'cycle_year',
                       'refresh', 'model_version', 'model_previous_version', 'model_mlflow_id', 'current_cycle', 'current_cycle_year', 'model_hierarchy', 
                       'forecast_data', 'forecast_start_date', 'forecast_end_date', 'filter_half_hr', 'stream', 'time_id', 'extra_keep_cols', 'extra_group_by_cols',
                       'remove_group_by_cols', 'filter_sl_nt_daypart', 'filter_hh_nt_daypart', 'filter_program_type', 'filter_show_name', 'filter_date',
                       'filter_cols', 'extra_agg_group_by_cols', 'remove_agg_group_by_cols', 'create_additional_cols', 'method', 'log_transformation',
                       'boxcox', 'arima_p', 'arima_d', 'arima_q', 'seasonal_p', 'seasonal_d', 'seasonal_q', 'arima_period', 'changepoints', 'regressors',
                       'additional_features', 'forecast_rule', 'latest_base_data', 'date_added', 'model_detail_id', 'library_version', 'run_data_layers')]
  return(details)
}