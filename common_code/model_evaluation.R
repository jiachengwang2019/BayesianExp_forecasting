#  #######################################################################
#       File-Name:      model_evaluation.R
#       Version:        R 3.4.4
#       Date:           Mar 26, 2019
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        contains all functions that can be used to check MAPE and bias values
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   NONE
#       Required by:    NONE
#       Status:         APPROVED
#       Machine:        NBCU laptop
#  #######################################################################

library(ggplot2)
library(tidyverse)
library(scales)

#::::::::::::::::::: Function to calculate insample and out-of-sample quarterly forecasts

find_quarterly_forecast <- function(full_data_champion,
                                    show_variable = "SC3_Impressions",
                                    weight_variable = "SC3_C_Dur",
                                    network = "Cable",
                                    timescale = "Week",
                                    OOS_start,
                                    Last_Actual_Date = as.Date("2099-12-31")){
  
  #' @description this function creates quarterly forecast, actuals and errors for both in-sample and out-of-sample
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param weight_variable denotes which weighting variable to be used to calculate means, default is `SC3_C_Dur`
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param timescale denotes the time-scale of the model - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #'        default is to include all days
  #' @return list of two R dataframes - one for in-sample and one for out-of-sample
  #'         both dataframes have columns quarter, actual, forecast, difference and mape 
  
  if (timescale == "Week"){
    text1 <- "Week<OOS_start"
    text2 <- "Week>=OOS_start"
    text3 <- "Week<=Last_Actual_Date"
  } else{
    text1 <- "Date<OOS_start"
    text2 <- "Date>=OOS_start"
    text3 <- "Date<=Last_Actual_Date"
  }
  
  if (network == "Cable"){
    names(full_data_champion)[which(names(full_data_champion)=="Cable_Qtr")] = "qtr"
  } else {
    names(full_data_champion)[which(names(full_data_champion)=="Broadcast_Qtr")] = "qtr"
  }
  names(full_data_champion)[which(names(full_data_champion)==show_variable)] = "show_variable"
  names(full_data_champion)[which(names(full_data_champion)==weight_variable)] = "weight_variable"
  
  quarterly_forecast <- list()
  quarterly_forecast[[1]] <- full_data_champion %>% 
    filter(eval(parse(text = text1))) %>% 
    group_by(Broadcast_Year, qtr) %>%
    summarize(forecast = mean(Predict, na.rm = T),
              actuals = weighted.mean(show_variable, weight_variable),
              difference = actuals - forecast,
              mape = round(abs((forecast-actuals)/actuals)*100,2)
    ) %>%
    mutate(quarter = paste0(Broadcast_Year, "-", qtr)
    ) %>% 
    subset(select = c("quarter","actuals","forecast","difference","mape")
    ) %>% data.frame()
  
  quarterly_forecast[[2]] <- full_data_champion %>% 
    filter(eval(parse(text = text2))) %>%
    filter(eval(parse(text = text3))) %>%
    group_by(Broadcast_Year, qtr) %>%
    summarize(forecast = mean(Predict, na.rm = T),
              actuals = weighted.mean(show_variable, weight_variable),
              difference = actuals - forecast,
              mape = round(abs((forecast-actuals)/actuals)*100,2)
    ) %>%
    mutate(quarter = paste0(Broadcast_Year, "-", qtr)
    ) %>% 
    subset(select = c("quarter","actuals","forecast","difference","mape")
    ) %>% data.frame()
  
  names(quarterly_forecast) = c("in_sample","out_of_sample")
  return(quarterly_forecast)
}


#:::::::::::::::::: Function to calculate insample and out-of-sample MAPE for the forecast

find_MAPE <- function(full_data_champion,
                      show_variable = "SC3_Impressions",
                      weight_variable = "SC3_C_Dur",
                      network = "Cable",
                      timescale = "Date",
                      OOS_start,
                      Last_Actual_Date = as.Date("2099-12-31")){
  
  #' @description this function computes model-specific and quarterly MAPE, for both in-sample and out-of-sample
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param weight_variable denotes which weighting variable to be used to calculate means, default is `SC3_C_Dur`
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param timescale denotes the time-scale of the model - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #'        default is to include all days
  #' @return A 2-by-2 R dataframe with model-wise and quarterly MAPE for in-sample and out-of-sample data
  
  quarterly_forecast <- find_quarterly_forecast(full_data_champion,show_variable,weight_variable,
                                                network,timescale,OOS_start,Last_Actual_Date)
  
  if (timescale == "Week"){
    text1 <- "Week<OOS_start"
    text2 <- "Week>=OOS_start"
    text3 <- "Week<=Last_Actual_Date"
  } else{
    text1 <- "Date<OOS_start"
    text2 <- "Date>=OOS_start"
    text3 <- "Date<=Last_Actual_Date"
  }
  
  trainset <- full_data_champion %>% filter(eval(parse(text = text1))) 
  testset <- full_data_champion %>% filter(eval(parse(text = text2))) %>% filter(eval(parse(text = text3)))
  
  MAPE = matrix(0,nrow = 2,ncol = 2)
  MAPE[1,1] = mape(actual = trainset[,show_variable],pred = trainset$Predict)
  MAPE[2,1] = mape(actual = testset[,show_variable],pred = testset$Predict)
  MAPE[,2] = as.numeric(lapply(quarterly_forecast,function(x) mape(x$actuals,x$forecast)))
  MAPE = data.frame(MAPE)
  colnames(MAPE) = c("model_wise","quarterly")
  rownames(MAPE) = c("train set","test set")
  
  return(MAPE)
}

#::::::::::::::::::: Function to calculate insample and out-of-sample BIAS for the forecast

find_bias <- function(full_data_champion,
                      show_variable = "SC3_Impressions",
                      weight_variable = "SC3_C_Dur",
                      network = "Cable",
                      timescale = "Date",
                      OOS_start,
                      Last_Actual_Date = as.Date("2099-12-31")){
  
  #' @description this function computes model-specific and quarterly bias, for both in-sample and out-of-sample
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param weight_variable denotes which weighting variable to be used to calculate means, default is `SC3_C_Dur`
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param timescale denotes the time-scale of the model - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #'        default is to include all days
  #' @return A 2-by-2 R dataframe with model-wise and quarterly bias for in-sample and out-of-sample data
  
  quarterly_forecast <- find_quarterly_forecast(full_data_champion,show_variable,weight_variable,
                                                network,timescale,OOS_start,Last_Actual_Date)
  
  if (timescale == "Week"){
    text1 <- "Week<OOS_start"
    text2 <- "Week>=OOS_start"
    text3 <- "Week<=Last_Actual_Date"
  } else{
    text1 <- "Date<OOS_start"
    text2 <- "Date>=OOS_start"
    text3 <- "Date<=Last_Actual_Date"
  }
  
  trainset <- full_data_champion %>% filter(eval(parse(text = text1))) 
  testset <- full_data_champion %>% filter(eval(parse(text = text2))) %>% filter(eval(parse(text = text3)))
  
  bias = matrix(0,nrow = 2,ncol = 2)
  bias[1,1] = mpe(actual = trainset[,show_variable],pred = trainset$Predict)
  bias[2,1] = mpe(actual = testset[,show_variable],pred = testset$Predict)
  bias[,2] = as.numeric(lapply(quarterly_forecast,function(x) mpe(x$actuals,x$forecast)))
  bias = data.frame(bias)
  colnames(bias) = c("model_wise","quarterly")
  rownames(bias) = c("train set","test set")
  
  return(bias)
}


#::::::::::::::::::::::::: Function to create group-wise error

find_groupwise_OOS_error <- function(full_data_champion,
                                     filter_rule = NULL,
                                     group_variable,
                                     show_variable = "SC3_Impressions",
                                     weight_variable = "SC3_C_Dur"){
  
  #' @description this function computes errors for aggregated data depending on grouping variables
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param filter_rule condition to be passed on the full_data_champion to consider for the analysis
  #' @param group_variable vector of variables according to which the groups are to be formed
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param weight_variable denotes which weighting variable to be used to calculate means, default is `SC3_C_Dur`
  #' 
  #' @return R dataframe with groupwise errors, along with counts, actuals and forecasts for out-of-sample
  
  names(full_data_champion)[which(names(full_data_champion)==show_variable)] = "show_variable"
  names(full_data_champion)[which(names(full_data_champion)==weight_variable)] = "weight_variable"
  
  if(is.null(filter_rule) == F){
    full_data_champion = full_data_champion %>% filter(eval(parse(text = filter_rule)))
  }
  
  full_data_champion <- setNames(full_data_champion, tolower(names(full_data_champion)))
  group_variable <- tolower(group_variable)
  
  out <- full_data_champion %>%
    group_by(.dots = group_variable) %>%
    summarize(N = n(),
              Forecast = mean(predict, na.rm = T),
              Actuals = weighted.mean(show_variable, weight_variable,na.rm = T),
              Difference = Actuals - Forecast,
              MAPE = round(abs((Forecast-Actuals)/Actuals)*100,2),
              SMAPE = round((Actuals-Forecast)/(abs(Actuals)+abs(Forecast))*100,2)
    ) %>%
    ungroup() 
  
  outcols = toupper(c(group_variable,"N","Actuals","Forecast","Difference","MAPE","SMAPE"))
  out <- setNames(out, toupper(names(out)))
  return(data.frame(out[,outcols]))
}

#::::::::::::::::::::: Dependent functions

# function to evaluate Symmetric MAPE 
smape <- function(actual,pred){
  smape <- mean(abs(actual - pred)/(abs(actual) + abs(pred)),na.rm = T)*100
  return (smape)
}

# function to evaluate MAPE (used in the functions above)
mape <- function(actual,pred){
  mape <- mean(abs((actual - pred)/actual),na.rm = T)*100
  return (mape)
}

# function to evaluate MPE (used in the functions above)
mpe <- function(actual,pred){
  mpe <- mean((pred - actual)/actual,na.rm = T)*100
  return (mpe)
}