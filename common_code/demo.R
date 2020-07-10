# #######################################################################
#       File-Name:      TSForecast_demo_example.R
#       Version:        R 3.6.2
#       Date:           June 19, 2020 
#       Author:         Jiacheng Wang <Jiacheng.Wang@nbcuni.com>
#       Purpose:        Examples for TSForecast package 
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   NONE
#       Required by:    NONE
#       Status:         In Progress
#       Machine:        NBCU laptop
#  #######################################################################
library(here)

source(here::here('common_code', 'assumption_checking_functions.R'))
source(here::here('common_code', 'dependent_functions.R'))

# Pay attention to the data type, if Broadcast, change this to the 'parameter_BENT.R' function
source(here::here("common_code","parameters_CENT.R"))

# Use data_prep_functions_v2.R instead of data_prep_functions.R!
source(here::here("common_code","data_prep_functions_v2.R"))
source(here::here("common_code","forecasting_functions.R"))
source(here::here("common_code","bayesian_forecasting_functions_v2.R"))
source(here::here("common_code","model_evaluation.R"))
source(here::here("common_code","plotting_functions.R"))

options(warn = -1,digits = 3,verbose = F,error = NULL)


# :::::::::::::::::: PART0 - PREPARE THE DATA
# Setup the parameters 
# Notice: 
# please make sure the net_type is consistent with parameters_CENT/BENT.R function
net_type = 'Cable'
net_ = 'OXYG'

inputpath = paste0('s3://nbcu-ds-linear-forecasting/processed/pacing/20W030/', net_ ,'_AFL_0420_200424_20W030.csv')
raw_data_AFL <- create_raw_data(inputFilePath = inputpath,
                                network = net_type,
                                group_by_cols = 'default')

# In this example, I filter SL_daypart and HH_daypart both using SalesPrime.
aggdata <- create_aggregated_data (raw_data_AFL ,
                                   interactive_input = T,
                                   network = net_type,
                                   time_id =  'Week',
                                   agg_group_by_cols = 'default')

case_data <- aggdata$aggregated_data
case_data = case_data[order(case_data$Week),]

# :::::::::::::::::: PART1 - CHECK ARIMA MODEL ASSUMPTION
# STATIONARY CHECK
# This functions works as a pre-model-selection procedure and it does the following three things:
# 1. Perform 3 hypothesis testing on interested predictor variable, in this example, the interested variable is 'SC3_Impressions'
# 2. Give a suggestion differencing_order to remove the trend in the interested variable
# 3. Provide the time series plot for interested variable
#
# Remark:
# In the test_results, KPSS is a more conservative testing strategy compared to the other two tests, ADF and PP.
# For detailed introduction to stationary test, please refer to the following wiki page:
# ADF: https://en.wikipedia.org/wiki/Augmented_Dickey%E2%80%93Fuller_test
# KPSS: https://en.wikipedia.org/wiki/KPSS_test
# PP: https://en.wikipedia.org/wiki/Phillips%E2%80%93Perron_test

st_check <- check_stationary(data = case_data, log_transformation = 1, variable = 'SC3_Impressions')
st_check

# :::::::::::::::::: PART2 - FIND CHAMPION ARIMA MODEL

# Set up possible sarima model parameters candidates 
# Notice: 
# The parameter d in the argument max_arima_order(p,d,q) should take the advice from 'differencing_order' results of `check_stationary()`` function
weeklylookup <- create_arima_lookuptable(max_arima_order = c(3,1,3),
                                         max_seasonal_order = c(1,0,1),
                                         periods = c(52))

OOS_start = as.Date("2019-03-30")

wkly_regressors = c(
  "intercept","Jan","Feb", "Mar", "Apr", "May", 
  "Jun","Jul", "Aug", "Oct", "Nov", "Dec",
  "Easter_week_ind", "Memorial_Day_week_ind", "Independence_Day_week_ind", 
  "Halloween_week_ind", "Thanksgiving_week_ind", "Thanksgiving_week_aft_ind", 
  "Christmas_week_bfr_ind","Christmas_week_ind","New_Years_Eve_week_ind","trend"
)

keep_reg = c("Jan","Feb", "Mar", "Apr", "May", "Jun","Jul", "Aug", "Oct", "Nov", "Dec")

# Remark:
# 1. 'stream' is our interest predicted variable, i.e., 'SC3_Impression' in this example
# 2. Make sure 'agg_timescale' is consistent with time_id parameter in ' create_aggregated_data()' function
champion = find_champion_arima(data = case_data,
                               stream = 'SC3_Impressions',
                               agg_timescale = "Week",
                               log_transformation = 1,
                               OOS_start = OOS_start,
                               regressors = wkly_regressors,
                               keep_regressors = keep_reg,
                               max_changepoints = 2, # later also experiment with 3 changepoints
                               lookuptable = weeklylookup)

champion_bayesian = find_champion_bayesian(data = case_data,
                                           stream = "SC3_Impressions",
                                           agg_timescale = "Week",
                                           log_transformation = 1,
                                           OOS_start = OOS_start,
                                           regressors = wkly_regressors,
                                           max_changepoints = 1)


# ::::::::::::::: PART3 - CHECK ASSUMPTIONS FOR SELECTED CHAMPION ARIMA MODEL
# ::::::::::::::: Part 3.1 check normality residuals
# This functions works as a post-model selection arima model assumption check and it does the following three things:
# 1. Perform 3 hypothesis testing on champion model residuals (residual = interested_variable_value - predicted_value)
# 2. Give a histogram for the residual to get first look on the distribution of residual
# 3. Provide qqplot plot for residual to check normality assumption
# 
# Remark:
# Pay attention to skewness check result since `find_chamption_arima()` function has applied boxcox transformation

normal_check <- check_residual_normality(data = champion$champion_result)
normal_check

normal_check_bayesian <- check_residual_normality(data = champion_bayesian$champion_result)
normal_check_bayesian

# ::::::::::::::: Part 3.2 check normality residuals
# This functions works as a post-model selection arima model assumption check and it does the following three things:
# 1. Pick out the outlier for the residuals using 1.5IQR rule
# 2. Pick out the extremes for the residuals
# 3. Provide residual plot to check if there are any obvious patterns that violates the assumption for residuals

outlier_check <- check_residual_outlier(data = champion$champion_result)
outlier_check

outlier_check_bayesian <- check_residual_outlier(data = champion_bayesian$champion_result)
outlier_check_bayesian

# ::::::::::::::: Part 3.3 check dependency
# First create a candidate lookup table for arima model parameters, this should be a little bit larger than the
# lookup table used in `find_champion_arima()` function
lookup <- create_arima_lookuptable(max_arima_order = c(7,1,7),
                                   max_seasonal_order = c(5,0,5),
                                   periods = c(52))

# This functions works as a post-model selection arima model assumption check and it does the following four things:
# 1. find out the significant lags of the current model and provide suggestion for possible model parameter choice
# 2. Provide the ACF plot for residuals
# 3. Provide the PACF plot for residuals
# 4. Given the extra arima model candidates lookup table (which provides paramter choice for model with better fit)

dependency_check <- check_acf_pacf_arima(data = champion$champion_result, 
                                         champion_model = champion$champion_model,
                                         existing_lookup = lookup) 
dependency_check


# ::::::::::::::: PART4 - CALCULATE ACCURACY METRICS
# The following three functions provides MAPE, bias and quarterly prediction summary for training data and testing data.

find_quarterly_forecast(full_data_champion = champion$champion_result,
                        show_variable = "SC3_Impressions",
                        weight_variable = "SC3_C_Dur",
                        network = "Cable",
                        timescale = "Week",
                        OOS_start = OOS_start)

find_quarterly_forecast_bayesian(full_data_champion = champion_bayesian$champion_result,
                                 show_variable  = "SC3_Impressions",
                                 weight_variable = "SC3_C_Dur",
                                 network = "Cable",
                                 timescale = "Week",
                                 OOS_start = OOS_start)

find_MAPE(full_data_champion = champion$champion_result,
          show_variable = "SC3_Impressions",
          weight_variable = "SC3_C_Dur",
          network = "Cable",
          timescale = "Week",
          OOS_start = OOS_start)

find_MAPE(full_data_champion = champion_bayesian$champion_result,
          show_variable = "SC3_Impressions",
          weight_variable = "SC3_C_Dur",
          network = "Cable",
          timescale = "Week",
          OOS_start = OOS_start)

find_bias(full_data_champion = champion$champion_result,
          show_variable = "SC3_Impressions",
          weight_variable = "SC3_C_Dur",
          network = "Cable",
          timescale = "Week",
          OOS_start = OOS_start)

find_bias(full_data_champion = champion_bayesian$champion_result,
          show_variable = "SC3_Impressions",
          weight_variable = "SC3_C_Dur",
          network = "Cable",
          timescale = "Week",
          OOS_start = OOS_start)


# ::::::::::::::: PART5 - CHECK THE PLOT
# This works as a visualization tool to compare the actural and predict
# This example aggregate the data in weekly level, thus we use weekly_forecast_plot
# If daily model is fitted, please make sure to use the daily_forecast_plot
weekly_forecast_plot(full_data_champion = champion$champion_result,
                     show_variable = "SC3_Impressions",
                     OOS_start = OOS_start)

weekly_forecast_plot(full_data_champion = champion_bayesian$champion_result,
                     show_variable = "SC3_Impressions",
                     OOS_start = OOS_start)


quarterly_forecast_plot(full_data_champion = champion$champion_result,
                        show_variable = "SC3_Impressions",
                        weight_variable = "SC3_C_Dur",
                        network = "Cable",
                        timescale = "Week",
                        OOS_start = OOS_start)

quarterly_forecast_plot(full_data_champion = champion_bayesian$champion_result,
                        show_variable = "SC3_Impressions",
                        weight_variable = "SC3_C_Dur",
                        network = "Cable",
                        timescale = "Week",
                        OOS_start = OOS_start)

# ::::::::::::::: PART6 - FIT CHAMPION MODEL
# After the model detail table finished, we can simply implement the champion model parameters to get the fitted model

# ARIMA_order & ARIMA_seasonal can be found in model detail table
regset <- unlist(strsplit(champion$regressors, split = ','))
fit_champion_arima(data = case_data,
                   stream = 'SC3_Impressions',
                   agg_timescale = 'Week',
                   OOS_start = OOS_start,
                   regressors = regset,
                   ARIMA_order = c(2,0,3),
                   ARIMA_seasonal = list(order = c(1,0,1), period = 52))

# If variance change, yearly_seasonality not added to the model, the only parameter we need to figure out for bayesian model is exponential decay parameter alpha.
# If weekly model fitted, remember to change agg_timescale as 'Week' and seasonality to be 'Year'!
regset_bayesian <- unlist(strsplit(champion_bayesian$regressors, split = ','))
fit_champion_bayesian (data = case_data,
                       stream = 'SC3_Impressions', 
                       agg_timescale = 'Week', 
                       OOS_start = OOS_start,
                       regressors = regset_bayesian,
                       alpha = 2,
                       seasonality = 'Year')