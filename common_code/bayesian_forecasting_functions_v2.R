#  #######################################################################
#       File-Name:      bayesian_forecasting_functions_v2.R
#       Version:        R 3.5.2
#       Date:           Jul 17, 2019
#       Author:         Jiacheng Wang <Jiacheng.Wang@nbcuni.com>
#       Purpose:        contains all functions related to bayesian model fitting
#                       This version choose the optimal model based on total mape/smape.
#       Input Files:    NONE
#       Output Files:   NONE
#       Data Output:    NONE
#       Previous files: NONE
#       Dependencies:   model_evaluation.r

#       Required by:    NONE
#       Status:         NOT APPROVED
#       Machine:        NBCU laptop
#  #######################################################################

library(tidyverse)
library(changepoint)
require(dplyr)
library(forecast)
library(MASS)
library(invgamma)

#:::::::::::::::::::::::::::: Function to find champion bayesian model

find_champion_bayesian <- function(data,
                                   stream = "SC3_Impressions",
                                   agg_timescale = "Date",
                                   log_transformation = 1,
                                   OOS_start,
                                   regressors,
                                   keep_regressors = NA,
                                   variance_change = FALSE,
                                   weekly_seasonality = FALSE,
                                   yearly_seasonality = FALSE,
                                   max_changepoints = 0,
                                   changepoint_dates = NA,
                                   changepoints_minseglen = 1,
                                   acceptable_MAPE = 15){
  #' @description finds best bayesian forecast model for the given data according to out-of-sample date 
  #' and conditions provided on regressors
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used, include `trend` in this vector if you want to use drift/trend in the model
  #' @param keep_regressors vector of all regressors to be forced to use in the model, default is all regressors 
  #' @param variance_change denotes whether the variance is treated as constant or changing every two-year, default is `FALSE`
  #' @param weekly_seasonality denotes whether adding weekly seasonality structure into daily data covariance structure, default is `FALSE`
  #' @param yearly_seasonality denotes whether adding yearly seasonality structure into weekly data covariance structure, default is `FALSE`
  #' @param max_changepoints no of changepoints to be used in piecewise linear trend, default is 0 (not to use piecewise linear trend)
  #' @param changepoint_dates dates where trend behavior changes, default is `NA` (changepoints to be found using statistical means)
  #' @param changepoint_minseglen integer giving the minimum segment length (no. of observations between changes) for the changepoint function, default is 1
  #' @param acceptable_MAPE MAPE (in %) cutoff to determine which models are acceptable, default is 15
  
  #' @return list containing the following:
  #' - bayesian optimal model parameter choice
  #' - new data adding prediction and error based on the optimal model
  #' - R dataframe with all details and model fit and forecasts for train/test data
  #' - vector of regressors eventually used in the model
  #' - changepoint dates for piecewise linear function
  
  # check if max_changepoints correspond to changepoint_dates
  if (sum(is.na(changepoint_dates))==0 & length(changepoint_dates)>max_changepoints){
    stop("no of changepoint-dates is more than allowed no of changepoints")
  }
  # check if "trend" in the regressors
  if (!"trend" %in% regressors & max_changepoints > 0){
    stop("'trend' has to be included in regressors to allow for piecewise linear trend")
  }
  # check if "intercept" in the regressors
  if (!"intercept" %in% regressors & max_changepoints > 0){
    warning("'intercept' should be included in regressors to get better prediction")
  }
  # check if yearly seasonality is included for daily data or weekly seasonality is included for weekly data
  if (agg_timescale == "Week"){
    if (weekly_seasonality == TRUE){
      warning("Adding weekly seasonality into weekly data is unnecessary")
    }
    # if (length(unique(data$Week)) != nrow(data)){
    #   stop("Make Sure only one observation each week for weekly data!")
    # }
  }else{
    if(yearly_seasonality == TRUE & weekly_seasonality == TRUE){
      stop("Adding weekly and yearly seasonality into daily data together makes algorithm not converge!")
    }
    # if (length(unique(data$Date)) != nrow(data)) {
    #   stop("Make Sure only one observation each day for daily data!")
    # }
  }
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    train <- data %>% filter(Week < OOS_start)
    test <- data %>% filter(Week >= OOS_start)
    cp_timescale <- "Week"
    date_lag =  date_diff(data$Week[which(data$Week< OOS_start)])
    date_lag_test = predict_date_diff(data$Week[which(data$Week< OOS_start)],data$Week[which(data$Week>=OOS_start)])
  } else{
    train <- data %>% filter(Date < OOS_start)
    test <- data %>% filter(Date >= OOS_start)
    cp_timescale <- "Date"
    date_lag = date_diff(data$Date[which(data$Date< OOS_start)])
    date_lag_test = predict_date_diff(data$Date[which(data$Date< OOS_start)],data$Date[which(data$Date>=OOS_start)])
  }
  
  # find train/test length and data
  train_periods = nrow(train)
  test_periods = nrow(test)
  
  # prepare data based on log-transformation
  if (log_transformation == 1){
    train_series = log(as.numeric(unlist(train[,stream])) + 0.01)
    test_series = log(as.numeric(unlist(test[,stream])) + 0.01)
  } else{
    train_series = as.numeric(unlist(train[,stream]))
    test_series = as.numeric(unlist(test[,stream]))
  }
  
  # set cross-validation range
  cv_range = c(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,0.9,1,2,3,4,5,10,11,12,20)
  cv_no = length(cv_range)
  
  # select appropriate regressor subset
  if (sum(is.na(keep_regressors)) == 1) keep_regressors = regressors
  full_formula <- paste(stream,paste(regressors[! regressors %in% c("trend","intercept")],collapse = "+"),sep = "~")
  reduced_formula <- paste(stream,paste(keep_regressors[! keep_regressors %in% c("trend","intercept")],collapse = "+"),sep = "~")
  full_model <- lm(formula(full_formula),data = train)
  lower_model <- lm(formula(reduced_formula),data = train)
  reduced_model <- step(full_model,scope = list(lower = lower_model,upper = full_model),trace = 0)
  regressors_subset <- names(reduced_model$coefficients)[-1]
  
  # prepare xreg matrices for train/test for the arima models
  reg_train <- train[,regressors_subset]
  reg_test <- test[,regressors_subset]
  if ("intercept" %in% regressors){
    Intercept <- rep(1,train_periods)
    reg_train <- cbind(Intercept,reg_train)
    colnames(reg_train)[1] = "intercept"
    Intercept <- rep(1,test_periods)
    reg_test <- cbind(Intercept,reg_test)
    colnames(reg_test)[1] = "intercept"
  }
  if ("trend" %in% regressors){
    x1 = c(1:train_periods)
    reg_train <- cbind(reg_train,x1)
    colnames(reg_train)[ncol(reg_train)] = "trend"
    x2 = seq(train_periods+1,train_periods+test_periods,1)
    reg_test <- cbind(reg_test,x2)
    colnames(reg_test)[ncol(reg_test)] = "trend"
  }
  
  # adjust xreg matrices as needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  regset <- names(reg_train)
  
  # prepare piecewise linear regression if adding any change points
  cp_date_out = NA
  cpt_no = 0
  knotpoints = as.list(rep(NA,max_changepoints))
  cpt_add = ifelse(max_changepoints > 0,1,0)
  cpt_add2 = ifelse(max_changepoints > 0,max_changepoints,1)
  if (max_changepoints > 0){
    cpt_no = c(rep(0,max_changepoints))
    cp_date_out = as.list(rep(NA,max_changepoints))
    if (is.na(changepoint_dates)==T){
      # experiment with at most the assumed no of "max_changepoints"
      for (i in 1:max_changepoints){
        options(warn=-1)
        changepoints <- cpt.mean(as.numeric(unlist(train[,stream])),
                                 method = "BinSeg",
                                 pen.value = "2*log(n)",
                                 Q = i,
                                 minseglen = changepoints_minseglen)
        options(warn=0)
        knotpoints[[i]] = cpts(changepoints)
        cpt_no[i] = length(knotpoints[[i]])
      }
    } else{
      knotpoints = which(unlist(train[,cp_timescale]) %in% changepoint_dates)
      cpt_no = length(knotpoints)
    }
    train_cpt = as.list(rep(NA,max_changepoints))
    for (i in 1:max_changepoints){
      if (cpt_no[i]>0){
        train_cpt[[i]] = as.list(rep(NA, cpt_no[i]+1))
        for (j in 1:(cpt_no[i]+1)){
          if(j !=1 & j!=(cpt_no[i]+1)){
            train_cpt[[i]][[j]] = (knotpoints[[i]][j-1]+1):knotpoints[[i]][j]
          }else{
            train_cpt[[i]][[1]] = 1:knotpoints[[i]][1]
            train_cpt[[i]][[cpt_no[i]+1]] = (knotpoints[[i]][cpt_no[i]]+1):train_periods
          }
        }
        changepoint_dates = as.data.frame(train[knotpoints[[i]],cp_timescale])
        cp_date_out[[i]] = paste(as.character(changepoint_dates[,cp_timescale]),collapse = ",")
      }
    }
  }
  
  # prepare for two-year-based variance change
  if (variance_change == TRUE){
    variance_cv_range = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)
    variance_cv_no = length(variance_cv_range)
    year_range =  unique(format(do.call("c",data[,cp_timescale]),"%Y"))
    var_period = ceiling((as.numeric(max(year_range))-as.numeric(min(year_range)))/2)
    var_length = rep(NA,var_period)
    for (i in 1:var_period){
      var_length[i] = length(which(format(do.call("c",data[,cp_timescale]),"%Y") %in% c(year_range[2*i-1],year_range[2*i])))
    }
  }else{
    variance_cv_range = 1
    variance_cv_no = length(variance_cv_range)
  }
  
  # prepare for weekly seasonality (daily data) and yearly seasonality (weekly data)
  if (yearly_seasonality == FALSE & weekly_seasonality == FALSE){
    cv_range_s = cv_range
    if (variance_change == FALSE){
      all_model = expand.grid(cv_range_s)
      all_model_margin = expand.grid(c(min(cv_range_s),max(cv_range_s)))
      colnames(all_model) = c("alpha")
      colnames(all_model_margin) = c("alpha")
      all_model_type = 1
    }else{
      all_model = expand.grid(cv_range_s,variance_cv_range)
      all_model_margin = expand.grid(c(min(cv_range_s),max(cv_range_s)),c(min(variance_cv_range),max(variance_cv_range)))
      colnames(all_model) = c("alpha","variance")
      colnames(all_model_margin) = c("alpha","variance")
      all_model_type = 2
    }
  }else{
    cv_range_s = cv_range
    cv_range_l = cv_range
    if (variance_change == FALSE){
      all_model = expand.grid(cv_range_s,cv_range_l)
      all_model_margin = expand.grid(c(min(cv_range_s),max(cv_range_s)),c(min(cv_range_l),max(cv_range_l)))
      colnames(all_model) = c("alpha","alpha_season")
      colnames(all_model_margin) = c("alpha","alpha_season")
      all_model_type = 3
    }else {
      all_model = expand.grid(cv_range_s,cv_range_l,variance_cv_range)
      all_model_margin = expand.grid(c(min(cv_range_s),max(cv_range_s)),c(min(cv_range_l),max(cv_range_l)),c(min(variance_cv_range),max(variance_cv_range)))
      colnames(all_model) = c("alpha","alpha_season","variance")
      colnames(all_model_margin) = c("alpha","alpha_season","variance")
      all_model_type = 4
    }
  }
  all_model_no = dim(all_model)[1]
  
  # set initial set of error and time vectors
  MAPE_train = c(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  SMAPE_train = c(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  MAPE_test = c(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  SMAPE_test = c(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  beta_total = as.list(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  sigma_total = c(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  time_total = c(rep(NA,all_model_no*(1-cpt_add)+all_model_no*max_changepoints*cpt_add))
  
  # provide the prior information for beta and sigma^2
  sigma_shape = 2
  sigma_scale = 2
  sigma_old = sigma_scale/(sigma_shape-1)
  beta_mean = rep(0, dim(reg_train)[2])
  beta_covariance = diag(dim(reg_train)[2])
  beta_old = mvrnorm(1,beta_mean,beta_covariance)
  beta_covariance_inv = solve(beta_covariance)
  beta_tmp = beta_covariance_inv%*%beta_mean
  
  for (i in 1:all_model_no){
    if (all_model_type == 1){
      alpha1 = all_model[i,]
      variance_matrix_train = diag(train_periods)
      variance_matrix_test = diag(test_periods)
      ed_inv = solve(variance_matrix_train%*% (exponential_decay(date_lag,alpha1) + diag(dim(reg_train)[1])))
      predict_cov_train = variance_matrix_train%*%exponential_decay(date_lag,alpha1) %*% variance_matrix_train
      predict_date_cov = variance_matrix_test%*%exponential_decay(date_lag_test,alpha1) %*% variance_matrix_train
    }
    if (all_model_type == 2){
      alpha1 = all_model[i,1]
      variance_para = all_model[i,2]
      variance_matrix = diag(rep(variance_para^(1:var_period),var_length))
      variance_matrix_train = variance_matrix[1:train_periods,1:train_periods]
      variance_matrix_test = variance_matrix[(1+train_periods):(train_periods+test_periods),(1+train_periods):(train_periods+test_periods)]
      ed_inv = solve(variance_matrix_train%*% (exponential_decay(date_lag,alpha1) + diag(dim(reg_train)[1])))
      predict_cov_train = variance_matrix_train%*%exponential_decay(date_lag,alpha1) %*% variance_matrix_train
      predict_date_cov = variance_matrix_test%*%exponential_decay(date_lag_test,alpha1) %*% variance_matrix_train
    }
    if (all_model_type == 3){
      alpha1 = all_model[i,1]
      alpha2 = all_model[i,2]
      variance_matrix_train = diag(train_periods)
      variance_matrix_test = diag(test_periods)
      if (agg_timescale == "Week"){
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_year(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_year(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_year(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }else{
        if (weekly_seasonality == TRUE){
          ed_inv = solve(variance_matrix_train%*% (exponential_decay_week(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
          predict_cov_train = variance_matrix_train%*%exponential_decay_week(date_lag,alpha1,alpha2)%*% variance_matrix_train
          predict_date_cov = variance_matrix_test%*%exponential_decay_week(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
        }else{
          ed_inv = solve(variance_matrix_train%*% (exponential_decay_year_daily(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
          predict_cov_train = variance_matrix_train%*%exponential_decay_year_daily(date_lag,alpha1,alpha2)%*% variance_matrix_train
          predict_date_cov = variance_matrix_test%*%exponential_decay_year_daily(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
        }
      }
    }
    if (all_model_type == 4){
      alpha1 = all_model[i,1]
      alpha2 = all_model[i,2]
      variance_para = all_model[i,3]
      variance_matrix = diag(rep(variance_para^(1:var_period),var_length))
      variance_matrix_train = variance_matrix[1:train_periods,1:train_periods]
      variance_matrix_test = variance_matrix[(1+train_periods):(train_periods+test_periods),(1+train_periods):(train_periods+test_periods)]
      if (agg_timescale == "Week"){
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_year(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_year(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_year(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }else{
        if (weekly_seasonality == TRUE){
          ed_inv = solve(variance_matrix_train%*% (exponential_decay_week(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
          predict_cov_train = variance_matrix_train%*%exponential_decay_week(date_lag,alpha1,alpha2)%*% variance_matrix_train
          predict_date_cov = variance_matrix_test%*%exponential_decay_week(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
        }else{
          ed_inv = solve(variance_matrix_train%*% (exponential_decay_year_daily(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
          predict_cov_train = variance_matrix_train%*%exponential_decay_year_daily(date_lag,alpha1,alpha2)%*% variance_matrix_train
          predict_date_cov = variance_matrix_test%*%exponential_decay_year_daily(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
        }
      }
    }
    cat("All_model_no:", all_model_no); cat("CV_No:",i) ; cat("\n")
    # updates for adding all changepoint
    if (max_changepoints > 0){
      for (j in 1:max_changepoints){
        beg = Sys.time()
        iteration = 0
        beta_update = 0.1
        sigma_update = 0.1
        train_series_cpt = as.list(rep(NA,cpt_no[j]+1))
        train_series_cpt_outside = as.list(rep(NA,cpt_no[j]+1))
        reg_train_cpt = as.list(rep(NA,cpt_no[j]+1))
        reg_train_cpt_outside = as.list(rep(NA,cpt_no[j]+1))
        variance_cpt = as.list(rep(NA,cpt_no[j]+1))
        variance_cpt_outside = as.list(rep(NA,cpt_no[j]+1))
        covariance_interaction = as.list(rep(NA,cpt_no[j]+1))
        beta_cpt_old = as.list(rep(NA,cpt_no[j]+1))
        beta_cpt_outside_old = as.list(rep(NA,cpt_no[j]+1))
        ed1_inv = as.list(rep(NA,cpt_no[j]+1))
        ed2_inv = as.list(rep(NA,cpt_no[j]+1))
        ed3_inv = as.list(rep(NA,cpt_no[j]+1))
        ed4_inv = as.list(rep(NA,cpt_no[j]+1))
        ed5_inv = as.list(rep(NA,cpt_no[j]+1))
        ed6_inv = as.list(rep(NA,cpt_no[j]+1))
        for (k in 1:(cpt_no[j]+1)){
          train_series_cpt[[k]] = train_series[train_cpt[[j]][[k]]]
          train_series_cpt_outside[[k]] = train_series[-train_cpt[[j]][[k]]]
          reg_train_cpt[[k]] = reg_train[train_cpt[[j]][[k]],]
          reg_train_cpt_outside[[k]] = reg_train[-train_cpt[[j]][[k]],]
          variance_cpt[[k]] = ed_inv[c(train_cpt[[j]][[k]]),c(train_cpt[[j]][[k]])]
          variance_cpt_outside[[k]] = ed_inv[-train_cpt[[j]][[k]],-train_cpt[[j]][[k]]]
          covariance_interaction[[k]] = ed_inv[train_cpt[[j]][[k]],-train_cpt[[j]][[k]]]
          beta_cpt_old[[k]] = beta_old
          beta_cpt_outside_old[[k]] = beta_old
          ed1_inv[[k]] = t(reg_train_cpt[[k]])%*% variance_cpt[[k]] %*% as.matrix(reg_train_cpt[[k]])
          ed2_inv[[k]] = t(reg_train_cpt_outside[[k]])%*% variance_cpt_outside[[k]] %*% as.matrix(reg_train_cpt_outside[[k]])
          ed3_inv[[k]] = t(reg_train_cpt[[k]])%*% variance_cpt[[k]] %*% train_series_cpt[[k]]
          ed4_inv[[k]] = t(reg_train_cpt_outside[[k]])%*% variance_cpt_outside[[k]] %*% train_series_cpt_outside[[k]]
          ed5_inv[[k]] = t(reg_train_cpt[[k]])%*%covariance_interaction[[k]]
          if (is.null(dim(covariance_interaction[[k]]))){
            ed6_inv[[k]] = t(reg_train_cpt_outside[[k]])%*%covariance_interaction[[k]]
          }else{
            ed6_inv[[k]] = t(reg_train_cpt_outside[[k]])%*%t(covariance_interaction[[k]])
          }
        }
        while (abs(beta_update) > 1e-5 | abs(sigma_update) > 1e-5){
          beta_cpt_new = as.list(rep(NA,cpt_no[j]+1))
          beta_cpt_outside_new = as.list(rep(NA,cpt_no[j]+1))
          resid_cpt = as.list(rep(NA,cpt_no[j]+1))
          for (l in 1:(cpt_no[j]+1)){
            beta_cpt_new[[l]] = solve(ed1_inv[[l]]/sigma_old+beta_covariance_inv)%*%((ed3_inv[[l]]+ed5_inv[[l]]%*%(train_series_cpt_outside[[l]]-as.matrix(reg_train_cpt_outside[[l]])%*%beta_cpt_outside_old[[l]]))/sigma_old+beta_tmp)
            resid_cpt[[l]] = train_series_cpt[[l]]-as.matrix(reg_train_cpt[[l]])%*% beta_cpt_new[[l]]
            beta_cpt_outside_new[[l]] = solve(ed2_inv[[l]]/sigma_old+beta_covariance_inv)%*%((ed4_inv[[l]]+ed6_inv[[l]]%*%(resid_cpt[[l]]))/sigma_old+beta_tmp)
          }
          sigma_shape_new = dim(reg_train)[1]/2 + sigma_shape
          sigma_scale_new = as.numeric(t(unlist(resid_cpt))%*%ed_inv%*%unlist(resid_cpt)/2) + sigma_scale
          sigma_new = as.numeric(sigma_scale_new/(sigma_shape_new-1))
          beta_diff = as.list(rep(NA,cpt_no[j]+1))
          for (k in 1:(cpt_no[j]+1)){
            beta_diff[[k]] = beta_cpt_new[[k]] - beta_cpt_old[[k]]
          }
          beta_update_new = sum(abs(unlist(beta_diff)))
          sigma_update_new = sigma_new - sigma_old
          beta_cpt_old = beta_cpt_new
          beta_cpt_outside_old = beta_cpt_outside_new
          sigma_old = sigma_new
          if (beta_update_new == beta_update){
            break
          }else if (sigma_update_new == sigma_update){
            break
          }else{
            beta_update = beta_update_new
            sigma_update = sigma_update_new
          }
          iteration = iteration + 1 
          if (iteration > 1e5) stop("The algorithm has reached the maximum iterations, and has not converged")
        }
        end = Sys.time()
        beta_total[[max_changepoints*(i-1)+j]] = beta_cpt_new
        sigma_total[max_changepoints*(i-1)+j] = sigma_new
        time_total[max_changepoints*(i-1)+j] = end-beg
        # Estimate for the train & test
        # train
        resid_train_error = sigma_new*predict_cov_train%*%ed_inv%*%unlist(resid_cpt)
        evaluate_train_tmp = as.list(rep(NA,cpt_no[j]+1))
        for(l in 1:(cpt_no[j]+1)){
          evaluate_train_tmp[[l]] = as.matrix(reg_train_cpt[[l]])%*%beta_cpt_new[[l]]
        }
        evaluate_train = unlist(evaluate_train_tmp) + resid_train_error
        
        # test
        resid_error = sigma_new*predict_date_cov%*%ed_inv%*%unlist(resid_cpt)
        evaluate_test <- as.matrix(reg_test)%*%beta_cpt_new[[cpt_no[j]+1]]+ resid_error
        if (log_transformation == 0){
          MAPE_train[max_changepoints*(i-1)+j] = round(mape(train_series,evaluate_train),4)
          SMAPE_train[max_changepoints*(i-1)+j] =  round(smape(train_series,evaluate_train),4)
          MAPE_test[max_changepoints*(i-1)+j] =  round(mape(test_series,evaluate_test),4)
          SMAPE_test[max_changepoints*(i-1)+j]  =  round(smape(test_series,evaluate_test),4)
        }else{
          MAPE_train[max_changepoints*(i-1)+j] = round(mape(exp(train_series),exp(evaluate_train)),4)
          SMAPE_train[max_changepoints*(i-1)+j] = round(smape(exp(train_series),exp(evaluate_train)),4)
          MAPE_test[max_changepoints*(i-1)+j] = round(mape(exp(test_series),exp(evaluate_test)),4)
          SMAPE_test[max_changepoints*(i-1)+j]  = round(smape(exp(test_series),exp(evaluate_test)),4)
        }
        # MAPE_train[max_changepoints*(i-1)+j] = round(mape(exp(train_series)*log_transformation + train_series*(1-log_transformation),exp(evaluate_train)*log_transformation+evaluate_train*(1-log_transformation)),4)
        # SMAPE_train[max_changepoints*(i-1)+j] =  round(smape(exp(train_series)*log_transformation + train_series*(1-log_transformation),exp(evaluate_train)*log_transformation+evaluate_train*(1-log_transformation)),4)
        # MAPE_test[max_changepoints*(i-1)+j] =  round(mape(exp(test_series)*log_transformation + test_series*(1-log_transformation),exp(evaluate_test)*log_transformation+evaluate_test*(1-log_transformation)),4)
        # SMAPE_test[max_changepoints*(i-1)+j] =  round(smape(exp(test_series)*log_transformation + test_series*(1-log_transformation),exp(evaluate_test)*log_transformation+evaluate_test*(1-log_transformation)),4)
      }
    }else{
      beg = Sys.time()
      ed1_inv = t(as.matrix(reg_train)) %*% ed_inv %*% as.matrix(reg_train)
      ed2_inv = t(as.matrix(reg_train))%*% ed_inv %*% train_series
      iteration = 0
      beta_update = 0.1
      sigma_update = 0.1
      
      while (sum(abs(beta_update)) > 1e-5 | abs(sigma_update) > 1e-5){
        beta_mean_new = solve(beta_covariance_inv+ed1_inv/sigma_old)%*%(ed2_inv/sigma_old+beta_tmp)
        beta_new = beta_mean_new
        resid = train_series - as.matrix(reg_train)%*%beta_new
        sigma_shape_new = dim(reg_train)[1]/2 + sigma_shape
        sigma_scale_new = as.numeric(t(resid)%*%ed_inv%*%resid/2) + sigma_scale
        sigma_new = as.numeric(sigma_scale_new/(sigma_shape_new-1))
        beta_update_new = beta_new - beta_old
        sigma_update_new = sigma_new - sigma_old
        beta_old = beta_new
        sigma_old = sigma_new
        if (sum(beta_update_new != beta_update) == 0){
          break
        }else if (sigma_update_new == sigma_update){
          break
        }else{
          beta_update = beta_update_new
          sigma_update = sigma_update_new
        }
        iteration = iteration + 1 
        if (iteration > 1e5) stop("The algorithm has beyond the maximum iterations! Probably not converge!")
      }
      end = Sys.time()
      beta_total[[i]] = beta_new
      sigma_total[i] = sigma_new
      time_total[i] = end-beg
      # Estimate for the train & test
      # train
      resid = train_series - ts(as.matrix(reg_train)%*%beta_new)
      resid_train_error = sigma_new*predict_cov_train%*%ed_inv%*%resid
      evaluate_train = as.matrix(reg_train)%*%beta_new + resid_train_error
      # test
      resid_error = sigma_new*predict_date_cov%*%ed_inv%*%resid
      evaluate_test <- as.matrix(reg_test)%*%beta_new + resid_error
      if (log_transformation == 0){
        MAPE_train[i] = round(mape(train_series,evaluate_train),4)
        SMAPE_train[i] =  round(smape(train_series,evaluate_train),4)
        MAPE_test[i] =  round(mape(test_series,evaluate_test),4)
        SMAPE_test[i] =  round(smape(test_series,evaluate_test),4)
      }else{
        MAPE_train[i] = round(mape(exp(train_series),exp(evaluate_train)),4)
        SMAPE_train[i] = round(smape(exp(train_series),exp(evaluate_train)),4)
        MAPE_test[i] = round(mape(exp(test_series),exp(evaluate_test)),4)
        SMAPE_test[i] = round(smape(exp(test_series),exp(evaluate_test)),4)
      }
    }
  }
  MAPE_total =  MAPE_train + MAPE_test
  SMAPE_total =  SMAPE_train + SMAPE_test
  cpt_optimal_mape = ifelse(which.min(MAPE_total)%%cpt_add2>0,(which.min(MAPE_total)%%cpt_add2)*cpt_add,max_changepoints)
  cpt_optimal_smape = ifelse(which.min(SMAPE_total)%%cpt_add2>0,(which.min(SMAPE_total)%%cpt_add2)*cpt_add,max_changepoints)
  if (max_changepoints > 0){
    beta_cv_tmp_mape = beta_total[which(unlist(lapply(beta_total,length)) == cpt_optimal_mape+1)]
    beta_sd_mape = apply(matrix(unlist(beta_cv_tmp_mape),nrow = ncol(reg_train)*(cpt_optimal_mape+1)),1,sd) %>% round(6)
    beta_bound_mape = apply(matrix(unlist(beta_cv_tmp_mape),nrow = ncol(reg_train)*(cpt_optimal_mape+1)),1,function(x) quantile(x,probs = c(0.025,0.975))) %>% round(6)
    beta_cv_tmp_smape = beta_total[which(unlist(lapply(beta_total,length)) == cpt_optimal_smape+1)]
    beta_sd_smape = apply(matrix(unlist(beta_cv_tmp_smape),nrow = ncol(reg_train)*(cpt_optimal_smape+1)),1,sd) %>% round(6)
    beta_bound_smape = apply(matrix(unlist(beta_cv_tmp_smape),nrow = ncol(reg_train)*(cpt_optimal_smape+1)),1,function(x) quantile(x,probs = c(0.025,0.975))) %>% round(6)
  }else{
    beta_sd_mape = apply(matrix(unlist(beta_total),nrow = ncol(reg_train)),1,sd) %>% round(6)
    beta_bound_mape = apply(matrix(unlist(beta_total),nrow = ncol(reg_train)),1,function(x) quantile(x,probs = c(0.025,0.975))) %>% round(6)
    beta_sd_smape = apply(matrix(unlist(beta_total),nrow = ncol(reg_train)),1,sd) %>% round(6)
    beta_bound_smape = apply(matrix(unlist(beta_total),nrow = ncol(reg_train)),1,function(x) quantile(x,probs = c(0.025,0.975))) %>% round(6)
  }
  
  model_sd_mape = c(rep("-",ncol(all_model)),beta_sd_mape,sd(sigma_total)%>%round(6))
  model_lower_mape = c(rep("-",ncol(all_model)),beta_bound_mape[1,],quantile(sigma_total,0.025)%>%round(6))
  model_upper_mape = c(rep("-",ncol(all_model)),beta_bound_mape[2,],quantile(sigma_total,0.975)%>%round(6))
  model_sd_smape = c(rep("-",ncol(all_model)),beta_sd_smape,sd(sigma_total)%>%round(6))
  model_lower_smape = c(rep("-",ncol(all_model)),beta_bound_smape[1,],quantile(sigma_total,0.025)%>%round(6))
  model_upper_smape = c(rep("-",ncol(all_model)),beta_bound_smape[2,],quantile(sigma_total,0.975)%>%round(6))
  
  
  model_optimal_mape = all_model[which.min(MAPE_total)*(1-cpt_add)+cpt_add*(ceiling(which.min(MAPE_total)/cpt_add2)),]
  model_optimal_smape = all_model[which.min(SMAPE_total)*(1-cpt_add)+cpt_add*(ceiling(which.min(MAPE_total)/cpt_add2)),]
  beta_optimal_mape = beta_total[[which.min(MAPE_total)]]
  beta_optimal_smape = beta_total[[which.min(SMAPE_total)]]
  sigma_optimal_mape = sigma_total[which.min(MAPE_total)]
  sigma_optimal_smape = sigma_total[which.min(SMAPE_total)]
  if (sum(model_optimal_mape!=model_optimal_smape)!= 0) warning("optimal model for mape and smape is different! Prediction is based on optimal mape!")
  if (sum(apply(matrix(unlist(all_model_margin),ncol = ncol(all_model)),1,function(x) all(x == model_optimal_mape)))==1) warning("Optimal model (mape) uses marginal cv candidate value!")
  if (sum(apply(matrix(unlist(all_model_margin),ncol = ncol(all_model)),1,function(x) all(x == model_optimal_smape)))==1) warning("Optimal model (smape) uses marginal cv candidate value!")
  if (cpt_optimal_mape != cpt_optimal_smape) warning("Optimal model (smape) choose different change point numbers!")
  # give prediction based on the optimal model
  if (all_model_type == 1){
    alpha1 = model_optimal_mape
    ed_inv = solve(variance_matrix_train%*% (exponential_decay(date_lag,alpha1) + diag(dim(reg_train)[1])))
    predict_cov_train = variance_matrix_train%*%exponential_decay(date_lag,alpha1) %*% variance_matrix_train
    predict_date_cov = variance_matrix_test%*%exponential_decay(date_lag_test,alpha1) %*% variance_matrix_train
  }
  if (all_model_type == 2){
    alpha1 = model_optimal_mape[1]$alpha
    variance_para = model_optimal_mape[2]$variance
    variance_matrix = diag(rep(variance_para^(1:var_period),var_length))
    variance_matrix_train = variance_matrix[1:train_periods,1:train_periods]
    variance_matrix_test = variance_matrix[(1+train_periods):(train_periods+test_periods),(1+train_periods):(train_periods+test_periods)]
    ed_inv = solve(variance_matrix_train%*% (exponential_decay(date_lag,alpha1) + diag(dim(reg_train)[1])))
    predict_cov_train = variance_matrix_train%*%exponential_decay(date_lag,alpha1) %*% variance_matrix_train
    predict_date_cov = variance_matrix_test%*%exponential_decay(date_lag_test,alpha1) %*% variance_matrix_train
  }
  if (all_model_type == 3){
    alpha1 = model_optimal_mape[1]$alpha
    alpha2 = model_optimal_mape[2]$alpha_season
    if (agg_timescale == "Week"){
      ed_inv = solve(variance_matrix_train%*% (exponential_decay_year(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
      predict_cov_train = variance_matrix_train%*%exponential_decay_year(date_lag,alpha1,alpha2)%*% variance_matrix_train
      predict_date_cov = variance_matrix_test%*%exponential_decay_year(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
    }else{
      if (weekly_seasonality == TRUE){
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_week(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_week(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_week(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }else{
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_year_daily(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_year_daily(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_year_daily(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }
    }
  }
  if (all_model_type == 4){
    alpha1 = model_optimal_mape[1]$alpha
    alpha2 = model_optimal_mape[2]$alpha_season
    variance_para = model_optimal_mape[3]$variance
    variance_matrix = diag(rep(variance_para^(1:var_period),var_length))
    variance_matrix_train = variance_matrix[1:train_periods,1:train_periods]
    variance_matrix_test = variance_matrix[(1+train_periods):(train_periods+test_periods),(1+train_periods):(train_periods+test_periods)]
    if (agg_timescale == "Week"){
      ed_inv = solve(variance_matrix_train%*% (exponential_decay_year(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
      predict_cov_train = variance_matrix_train%*%exponential_decay_year(date_lag,alpha1,alpha2)%*% variance_matrix_train
      predict_date_cov = variance_matrix_test%*%exponential_decay_year(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
    }else{
      if (weekly_seasonality == TRUE){
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_week(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_week(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_week(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }else{
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_year_daily(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_year_daily(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_year_daily(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }
    }
  }
  
  #
  if (max_changepoints >0){
    resid_opt = as.list(rep(NA,cpt_no[cpt_optimal_mape]+1))
    for (l in 1:(cpt_no[cpt_optimal_mape]+1)){
      resid_opt[[l]] = train_series[train_cpt[[cpt_no[cpt_optimal_mape]]][[l]]]-as.matrix(reg_train[train_cpt[[cpt_no[cpt_optimal_mape]]][[l]],])%*% beta_optimal_mape[[l]]
    }
    resid_train_error = sigma_optimal_mape*predict_cov_train%*%ed_inv%*%unlist(resid_opt)
    evaluate_train_tmp = as.list(rep(NA,cpt_no[cpt_optimal_mape]+1))
    for(l in 1:(cpt_no[cpt_optimal_mape]+1)){
      evaluate_train_tmp[[l]] = as.matrix(reg_train[train_cpt[[cpt_no[cpt_optimal_mape]]][[l]],])%*%beta_optimal_mape[[l]]
    }
    evaluate_train = unlist(evaluate_train_tmp) + resid_train_error
    resid_error = sigma_optimal_mape*predict_date_cov%*%ed_inv%*%unlist(resid_opt)
    evaluate_test <- as.matrix(reg_test)%*%beta_cpt_new[[cpt_no[cpt_optimal_mape]+1]]+ resid_error
  }else{
    resid = train_series - ts(as.matrix(reg_train)%*% beta_optimal_mape)
    resid_train_error = sigma_optimal_mape*predict_cov_train%*%ed_inv%*%resid
    evaluate_train = as.matrix(reg_train)%*%beta_optimal_mape + resid_train_error
    resid_error = sigma_optimal_mape*predict_date_cov%*%ed_inv%*%resid
    evaluate_test <- as.matrix(reg_test)%*%beta_optimal_mape + resid_error 
  }
  
  # champion model
  optimal_model = data.frame(matrix(NA,nrow = 1+length(unlist(model_optimal_mape))+max(length(unlist(beta_optimal_mape)),length(unlist(beta_optimal_smape))),ncol = 8))
  optimal_model[1:length(unlist(model_optimal_mape)),1] = round(unlist(model_optimal_mape),4)
  optimal_model[1:length(unlist(model_optimal_mape)),5] = round(unlist(model_optimal_smape),4)
  length_diff = abs(length(unlist(beta_optimal_mape)) - length(unlist(beta_optimal_smape)))
  if (cpt_optimal_mape > cpt_optimal_smape){
    optimal_model[(length(unlist(model_optimal_mape))+1):(length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape))),1] = round(unlist(beta_optimal_mape),4)
    optimal_model[(length(unlist(model_optimal_mape))+1):(length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape))),5] =c(round(unlist(beta_optimal_smape),4),rep("-",length_diff))
    optimal_model[1+length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape)),1] = round(sigma_optimal_mape,4)
    optimal_model[1+length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape)),5] = round(sigma_optimal_smape,4)
    optimal_model[,2] = model_sd_mape
    optimal_model[,3] = model_lower_mape
    optimal_model[,4] = model_upper_mape
    optimal_model[,6] = c(rep("-",ncol(all_model)),c(beta_sd_smape,rep("-",length_diff)),sd(sigma_total)%>%round(6))
    optimal_model[,7] = c(rep("-",ncol(all_model)),c(beta_bound_smape[1,],rep("-",length_diff)),quantile(sigma_total,0.025)%>%round(6))
    optimal_model[,8] = c(rep("-",ncol(all_model)),c(beta_bound_smape[2,],rep("-",length_diff)),quantile(sigma_total,0.975)%>%round(6))
  }else if (cpt_optimal_mape < cpt_optimal_smape){
    optimal_model[(length(unlist(model_optimal_mape))+1):(length(unlist(model_optimal_mape))+length(unlist(beta_optimal_smape))),1] = c(round(unlist(beta_optimal_mape),4),rep("-",length_diff))
    optimal_model[(length(unlist(model_optimal_mape))+1):(length(unlist(model_optimal_mape))+length(unlist(beta_optimal_smape))),5] = round(unlist(beta_optimal_smape),4)
    optimal_model[,2] = c(rep("-",ncol(all_model)),c(beta_sd_mape,rep("-",length_diff)),sd(sigma_total)%>%round(6))
    optimal_model[,3] = c(rep("-",ncol(all_model)),c(beta_bound_mape[1,],rep("-",length_diff)),quantile(sigma_total,0.025)%>%round(6))
    optimal_model[,4] = c(rep("-",ncol(all_model)),c(beta_bound_mape[2,],rep("-",length_diff)),quantile(sigma_total,0.975)%>%round(6))
    optimal_model[,6] = model_sd_smape
    optimal_model[,7] = model_lower_smape
    optimal_model[,8] = model_upper_smape
    optimal_model[1+length(unlist(model_optimal_mape))+length(unlist(beta_optimal_smape)),1] = round(sigma_optimal_mape,4)
    optimal_model[1+length(unlist(model_optimal_mape))+length(unlist(beta_optimal_smape)),5] = round(sigma_optimal_smape,4)
  }else{
    optimal_model[(length(unlist(model_optimal_mape))+1):(length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape))),1] = round(unlist(beta_optimal_mape),4)
    optimal_model[(length(unlist(model_optimal_mape))+1):(length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape))),5] = round(unlist(beta_optimal_smape),4)
    optimal_model[1+length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape)),1] = round(sigma_optimal_mape,4)
    optimal_model[1+length(unlist(model_optimal_mape))+length(unlist(beta_optimal_mape)),5] = round(sigma_optimal_smape,4)
    optimal_model[,2] = model_sd_mape
    optimal_model[,3] = model_lower_mape
    optimal_model[,4] = model_upper_mape
    optimal_model[,6] = model_sd_smape
    optimal_model[,7] = model_lower_smape
    optimal_model[,8] = model_upper_smape
  }
  
  colnames(optimal_model) = c("MAPE optimal", "SD", "LowerBound(95%)", "UpperBound(95%)","SMAPE optimal","SD", "LowerBound(95%)", "UpperBound(95%)")
  if (max_changepoints >0){
    names_changepoint = c(rep(NA,length(colnames(reg_train))*(cpt_no[max(cpt_optimal_mape,cpt_optimal_smape)]+1)))
    for (i in 1:(cpt_no[max(cpt_optimal_mape,cpt_optimal_smape)]+1)){
      names_changepoint[(length(colnames(reg_train))*(i-1)+1):(length(colnames(reg_train))*i)] = paste(colnames(reg_train),i)
    }
    names_beta = names_changepoint
  }else{
    names_beta = colnames(reg_train)
  }
  rownames(optimal_model) = c(colnames(all_model),names_beta,"sigma^2")
  
  # champion_result
  trainfit_champion = exp(evaluate_train)*log_transformation + evaluate_train*(1-log_transformation)
  testfit_champion = exp(evaluate_test)*log_transformation + evaluate_test*(1-log_transformation)
  combinedfit_champion <- as.data.frame(append(trainfit_champion, testfit_champion))
  names(combinedfit_champion) = "Predict"
  full_data_champion <- as.data.frame(cbind(as.data.frame(data),combinedfit_champion)) 
  full_data_champion$Stream = stream
  full_data_champion$Residual = full_data_champion[,stream] - full_data_champion$Predict
  full_data_champion$APE = abs(((full_data_champion[,stream] - full_data_champion$Predict)/full_data_champion[,stream])*100)
  
  # all model details
  if(max_changepoints > 0){
    all_model_detail = all_model[rep(row.names(all_model),rep(cpt_add2,nrow(all_model))),1:ncol(all_model)] 
  }else{
    all_model_detail = all_model 
  }
  detail_entry = data.frame(all_model_detail,
                            MAPE_train,
                            MAPE_test,
                            MAPE_total,
                            SMAPE_train,
                            SMAPE_test,
                            SMAPE_total,
                            time_total,
                            log_transformation,
                            paste(regset,collapse = ","),
                            paste(row.names(optimal_model),collapse = ","),
                            paste(optimal_model[,1],collapse = ","),
                            unlist(rep(cp_date_out,all_model_no)),
                            stringsAsFactors = T)
  names(detail_entry) = c(colnames(all_model),
                          "train_MAPE","test_MAPE","total_MAPE",
                          "train_SMAPE","test_SMAPE","total_SMAPE",
                          "runtime",
                          "log_transformation",
                          "regressors","parameters","estimates","changepoints")
  # forecast champion model - test set
  detail_entry$champion = 0
  minidx_mape = which.min(detail_entry$total_MAPE)
  minidx_smape = which.min(detail_entry$total_SMAPE)
  if (minidx_mape != minidx_smape){
    detail_entry$champion[minidx_mape] = "MAPE_CHAMPION"
    detail_entry$champion[minidx_smape] = "SMAPE_CHAMPION"
  }else{
    detail_entry$champion[minidx_mape] = "CHAMPION"
  }
  detail_entry$ranking = 'Average'
  detail_entry$ranking[detail_entry$test_MAPE < acceptable_MAPE] = 'Good'
  detail_entry$ranking[detail_entry$test_MAPE > 2*acceptable_MAPE] = 'Bad'
  detail_entry = detail_entry[order(detail_entry$total_MAPE,decreasing = F),]
  
  return(list(champion_model = optimal_model,
              champion_result = full_data_champion,
              all_model_details = detail_entry,
              regressors = as.character(detail_entry$regressors[minidx_mape]),
              changepoints = as.character(detail_entry$changepoints[minidx_mape])))
  
}

# ::::::::::::::::::::::::::::::::: Function to fit champion bayesian model
fit_champion_bayesian <- function(data,
                                  stream = "SC3_Impressions",
                                  agg_timescale = "Date",
                                  log_transformation = 1,
                                  OOS_start,
                                  regressors,
                                  keep_regressors = NA,
                                  alpha,
                                  seasonality = "Week",
                                  alpha_season = NA,
                                  variance_change = NA,
                                  changepoint_dates = NA){
  #' @description fit best bayesian forecast model for the given data according to out-of-sample date 
  #' and conditions provided on regressors
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used, include `trend` in this vector if you want to use drift/trend in the model
  #' @param keep_regressors vector of all regressors to be forced to use in the model, default is all regressors 
  #' @param alpha daily level parameter for exponential decay matrix 
  #' @param seasonality denotes seasonality structure into daily data covariance structure - choices are `Week` and `Year`
  #' @param alpha_season seasonal level parameter for exponential decay matrix
  #' @param variance_change parameter for variance change (2-year-based)
  #' @param changepoint_dates dates where trend behavior changes, default is `NA` (changepoints to be found using statistical means)
  
  #' @return list containing the following:
  #' - bayesian optimal model parameter choice
  #' - new data adding prediction and error based on the optimal model
  #' - changepoint dates for piecewise linear function
  
  # check if "trend" in the regressors
  if (!"trend" %in% regressors){
    stop("'trend' has to be included in regressors to allow for piecewise linear trend")
  }
  # check if "intercept" in the regressors
  if (!"intercept" %in% regressors){
    warning("'intercept' has to be included in regressors to get better prediction")
  }
  
  # check if input contains multiple observations each day/week
  if (agg_timescale == "Week"){
    if(length(unique(data$Week)) != nrow(data)){
      stop("Make Sure only one observation each week for weekly data!")
    }
  }else{
    if(length(unique(data$Date)) != nrow(data)){
      stop("Make Sure only one observation each day for daily data!")
    }
  }
  # if (sum(is.na(changepoint_dates)==FALSE) != length(changepoint_dates)){
  #   stop("Please enter the correct `as.Date()` form for changepoint_dates!")
  # }
  if (agg_timescale == "Week" & seasonality == "Week"){
    stop("Add yearly seasonality for weekly data!")
  }
  if (! seasonality %in% c("Year","Week") | length(seasonality)!=1 ){
    stop("Give correct format for seasonality!(`Week` or `Year`)")
  }
  # prepare train and test data 
  if (agg_timescale == "Week"){
    train <- data %>% filter(Week < OOS_start)
    test <- data %>% filter(Week >= OOS_start)
    cp_timescale <- "Week"
    date_lag =  date_diff(data$Week[which(data$Week< OOS_start)])
    date_lag_test = predict_date_diff(data$Week[which(data$Week< OOS_start)],data$Week[which(data$Week>=OOS_start)])
  } else{
    train <- data %>% filter(Date < OOS_start)
    test <- data %>% filter(Date >= OOS_start)
    cp_timescale <- "Date"
    date_lag = date_diff(data$Date[which(data$Date< OOS_start)])
    date_lag_test = predict_date_diff(data$Date[which(data$Date< OOS_start)],data$Date[which(data$Date>=OOS_start)])
  } 
  train_periods = nrow(train)
  test_periods = nrow(test)
  
  # prepare data based on log-transformation
  if (log_transformation == 1){
    train_series = log(as.numeric(unlist(train[,stream])) + 0.01)
    test_series = log(as.numeric(unlist(test[,stream])) + 0.01)
  } else{
    train_series = as.numeric(unlist(train[,stream]))
    test_series = as.numeric(unlist(test[,stream]))
  }
  
  # select appropriate regressor subset
  if (sum(is.na(keep_regressors)) == 1) keep_regressors = regressors
  full_formula <- paste(stream,paste(regressors[! regressors %in% c("trend","intercept")],collapse = "+"),sep = "~")
  reduced_formula <- paste(stream,paste(keep_regressors[! keep_regressors %in% c("trend","intercept")],collapse = "+"),sep = "~")
  full_model <- lm(formula(full_formula),data = train)
  lower_model <- lm(formula(reduced_formula),data = train)
  reduced_model <- step(full_model,scope = list(lower = lower_model,upper = full_model),trace = 0)
  regressors_subset <- names(reduced_model$coefficients)[-1]
  
  # prepare xreg matrices for train/test for the arima models
  reg_train <- train[,regressors_subset]
  reg_test <- test[,regressors_subset]
  if ("intercept" %in% regressors){
    Intercept <- rep(1,train_periods)
    reg_train <- cbind(Intercept,reg_train)
    Intercept <- rep(1,test_periods)
    reg_test <- cbind(Intercept,reg_test)
  }
  if ("trend" %in% regressors){
    x1 = c(1:train_periods)
    reg_train <- cbind(reg_train,x1)
    colnames(reg_train)[ncol(reg_train)] = "trend"
    x2 = seq(train_periods+1,train_periods+test_periods,1)
    reg_test <- cbind(reg_test,x2)
    colnames(reg_test)[ncol(reg_test)] = "trend"
  }
  
  # prepare piecewise linear regression if adding any change points
  cpt_no = 0
  cpt_add = ifelse(length(changepoint_dates)>0,1,0)
  cpt_add2 = ifelse(length(changepoint_dates)>0,length(changepoint_dates),1)
  if (sum(is.na(changepoint_dates)==FALSE) != 0){
    knotpoints = c(rep(NA,length(changepoint_dates)))
    for (i in 1:length(changepoint_dates)){
      if (agg_timescale == "Week"){
        knotpoints[i] = which(data$Week == changepoint_dates[i])
      }else{
        knotpoints[i] = which(data$Date == changepoint_dates[i])
      }
    }
    cpt_no = length(knotpoints)
    train_cpt = as.list(rep(NA, cpt_no+1))
    for (i in 1:(cpt_no+1)){
      if(i !=1 & i!=(cpt_no+1)){
        train_cpt[[i]] = (knotpoints[i-1]+1):knotpoints[i]
      }else{
        train_cpt[[1]] = 1:knotpoints[1]
        train_cpt[[cpt_no+1]] = (knotpoints[cpt_no]+1):train_periods
      }
    }
  }
  
  # prepare for two-year-based variance change
  if (is.na(variance_change) == FALSE){
    year_range =  unique(format(do.call("c",data[,cp_timescale]),"%Y"))
    var_period = ceiling((as.numeric(max(year_range))-as.numeric(min(year_range)))/2)
    var_length = rep(NA,var_period)
    for (i in 1:var_period){
      var_length[i] = length(which(format(do.call("c",data[,cp_timescale]),"%Y") %in% c(year_range[2*i-1],year_range[2*i])))
    }
  }
  
  # prepare for weekly seasonality (daily data) and yearly seasonality (weekly data)
  if (is.na(alpha_season) == TRUE){
    if (is.na(variance_change) == TRUE){
      all_model_type = 1
      parameter_no = 1
      parameter_total = c(alpha)
      names(parameter_total) = "alpha"
    }else{
      all_model_type = 2
      parameter_no = 2
      parameter_total = c(alpha,variance_change)
      names(parameter_total) = c("alpha","variance_change")
    }
  }else{
    if (is.na(variance_change) == TRUE){
      all_model_type = 3
      parameter_no = 2
      parameter_total = c(alpha,alpha_season)
      names(parameter_total) = c("alpha","alpha_season")
    }else {
      all_model_type = 4
      parameter_no = 3
      parameter_total = c(alpha,alpha_season,variance_change)
      names(parameter_total) = c("alpha","alpha_season","variance_change")
    }
  }
  
  
  # adjust xreg matrices as needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  regset <- names(reg_train)
  
  # provide the prior information for beta and sigma^2
  sigma_shape = 2
  sigma_scale = 2
  sigma_old = sigma_scale/(sigma_shape-1)
  beta_mean = rep(0, dim(reg_train)[2])
  beta_covariance = diag(dim(reg_train)[2])
  beta_old = mvrnorm(1,beta_mean,beta_covariance)
  beta_covariance_inv = solve(beta_covariance)
  beta_tmp = beta_covariance_inv%*%beta_mean
  
  # prepare basic algorithm matrix calculation
  if (all_model_type == 1){
    alpha1 = alpha
    variance_matrix_train = diag(train_periods)
    variance_matrix_test = diag(test_periods)
    ed_inv = solve(variance_matrix_train%*% (exponential_decay(date_lag,alpha1) + diag(dim(reg_train)[1])))
    predict_cov_train = variance_matrix_train%*%exponential_decay(date_lag,alpha1) %*% variance_matrix_train
    predict_date_cov = variance_matrix_test%*%exponential_decay(date_lag_test,alpha1) %*% variance_matrix_train
  }
  if (all_model_type == 2){
    alpha1 = alpha
    variance_matrix = diag(rep(variance_change^(1:var_period),var_length))
    variance_matrix_train = variance_matrix[1:train_periods,1:train_periods]
    variance_matrix_test = variance_matrix[(1+train_periods):(train_periods+test_periods),(1+train_periods):(train_periods+test_periods)]
    ed_inv = solve(variance_matrix_train%*% (exponential_decay(date_lag,alpha1) + diag(dim(reg_train)[1])))
    predict_cov_train = variance_matrix_train%*%exponential_decay(date_lag,alpha1) %*% variance_matrix_train
    predict_date_cov = variance_matrix_test%*%exponential_decay(date_lag_test,alpha1) %*% variance_matrix_train
  }
  if (all_model_type == 3){
    alpha1 = alpha
    alpha2 = alpha_season
    variance_matrix_train = diag(train_periods)
    variance_matrix_test = diag(test_periods)
    if (agg_timescale == "Week"){
      ed_inv = solve(variance_matrix_train%*% (exponential_decay_year(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
      predict_cov_train = variance_matrix_train%*%exponential_decay_year(date_lag,alpha1,alpha2)%*% variance_matrix_train
      predict_date_cov = variance_matrix_test%*%exponential_decay_year(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
    }else{
      if (seasonality == "Week"){
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_week(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_week(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_week(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }else{
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_year_daily(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_year_daily(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_year_daily(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }
    }
  }
  if (all_model_type == 4){
    alpha1 = alpha
    alpha2 = alpha_season
    variance_matrix = diag(rep(variance_change^(1:var_period),var_length))
    variance_matrix_train = variance_matrix[1:train_periods,1:train_periods]
    variance_matrix_test = variance_matrix[(1+train_periods):(train_periods+test_periods),(1+train_periods):(train_periods+test_periods)]
    if (agg_timescale == "Week"){
      ed_inv = solve(variance_matrix_train%*% (exponential_decay_year(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
      predict_cov_train = variance_matrix_train%*%exponential_decay_year(date_lag,alpha1,alpha2)%*% variance_matrix_train
      predict_date_cov = variance_matrix_test%*%exponential_decay_year(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
    }else{
      if (seasonality == "Week"){
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_week(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_week(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_week(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }else{
        ed_inv = solve(variance_matrix_train%*% (exponential_decay_year_daily(date_lag,alpha1,alpha2) + diag(dim(reg_train)[1])))
        predict_cov_train = variance_matrix_train%*%exponential_decay_year_daily(date_lag,alpha1,alpha2)%*% variance_matrix_train
        predict_date_cov = variance_matrix_test%*%exponential_decay_year_daily(date_lag_test,alpha1,alpha2)%*% variance_matrix_train
      }
    }
  }
  
  iteration = 0
  beta_update = 0.1
  sigma_update = 0.1
  if (sum(is.na(changepoint_dates) == FALSE)!=0){
    train_series_cpt = as.list(rep(NA,cpt_no+1))
    train_series_cpt_outside = as.list(rep(NA,cpt_no+1))
    reg_train_cpt = as.list(rep(NA,cpt_no+1))
    reg_train_cpt_outside = as.list(rep(NA,cpt_no+1))
    variance_cpt = as.list(rep(NA,cpt_no+1))
    variance_cpt_outside = as.list(rep(NA,cpt_no+1))
    covariance_interaction = as.list(rep(NA,cpt_no+1))
    beta_cpt_old = as.list(rep(NA,cpt_no+1))
    beta_cpt_outside_old = as.list(rep(NA,cpt_no+1))
    ed1_inv = as.list(rep(NA,cpt_no+1))
    ed2_inv = as.list(rep(NA,cpt_no+1))
    ed3_inv = as.list(rep(NA,cpt_no+1))
    ed4_inv = as.list(rep(NA,cpt_no+1))
    ed5_inv = as.list(rep(NA,cpt_no+1))
    ed6_inv = as.list(rep(NA,cpt_no+1))
    for (k in 1:(cpt_no+1)){
      train_series_cpt[[k]] = train_series[train_cpt[[k]]]
      train_series_cpt_outside[[k]] = train_series[-train_cpt[[k]]]
      reg_train_cpt[[k]] = reg_train[train_cpt[[k]],]
      reg_train_cpt_outside[[k]] = reg_train[-train_cpt[[k]],]
      variance_cpt[[k]] = ed_inv[c(train_cpt[[k]]),c(train_cpt[[k]])]
      variance_cpt_outside[[k]] = ed_inv[-train_cpt[[k]],-train_cpt[[k]]]
      covariance_interaction[[k]] = ed_inv[train_cpt[[k]],-train_cpt[[k]]]
      beta_cpt_old[[k]] = beta_old
      beta_cpt_outside_old[[k]] = beta_old
      ed1_inv[[k]] = t(reg_train_cpt[[k]])%*% variance_cpt[[k]] %*% as.matrix(reg_train_cpt[[k]])
      ed2_inv[[k]] = t(reg_train_cpt_outside[[k]])%*% variance_cpt_outside[[k]] %*% as.matrix(reg_train_cpt_outside[[k]])
      ed3_inv[[k]] =t(reg_train_cpt[[k]])%*% variance_cpt[[k]] %*% train_series_cpt[[k]]
      ed4_inv[[k]] =t(reg_train_cpt_outside[[k]])%*% variance_cpt_outside[[k]] %*% train_series_cpt_outside[[k]]
      ed5_inv[[k]] = t(reg_train_cpt[[k]])%*%covariance_interaction[[k]]
      if (is.null(dim(covariance_interaction[[k]]))){
        ed6_inv[[k]] = t(reg_train_cpt_outside[[k]])%*%covariance_interaction[[k]]
      }else{
        ed6_inv[[k]] = t(reg_train_cpt_outside[[k]])%*%t(covariance_interaction[[k]])
      }
    }
    while (abs(beta_update) > 1e-5 | abs(sigma_update) > 1e-5){
      beta_cpt_new = as.list(rep(NA,cpt_no+1))
      beta_cpt_outside_new = as.list(rep(NA,cpt_no+1))
      resid_cpt = as.list(rep(NA,cpt_no+1))
      for (l in 1:(cpt_no+1)){
        beta_cpt_new[[l]] = solve(ed1_inv[[l]]/sigma_old+beta_covariance_inv)%*%((ed3_inv[[l]]+ed5_inv[[l]]%*%(train_series_cpt_outside[[l]]-as.matrix(reg_train_cpt_outside[[l]])%*%beta_cpt_outside_old[[l]]))/sigma_old+beta_tmp)
        resid_cpt[[l]] = train_series_cpt[[l]]-as.matrix(reg_train_cpt[[l]])%*% beta_cpt_new[[l]]
        beta_cpt_outside_new[[l]] = solve(ed2_inv[[l]]/sigma_old+beta_covariance_inv)%*%((ed4_inv[[l]]+ed6_inv[[l]]%*%(resid_cpt[[l]]))/sigma_old+beta_tmp)
      }
      sigma_shape_new = dim(reg_train)[1]/2 + sigma_shape
      sigma_scale_new = as.numeric(t(unlist(resid_cpt))%*%ed_inv%*%unlist(resid_cpt)/2) + sigma_scale
      sigma_new = as.numeric(sigma_scale_new/(sigma_shape_new-1))
      beta_diff = as.list(rep(NA,cpt_no+1))
      for (k in 1:(cpt_no+1)){
        beta_diff[[k]] = beta_cpt_new[[k]] - beta_cpt_old[[k]]
      }
      beta_update_new = sum(abs(unlist(beta_diff)))
      sigma_update_new = sigma_new - sigma_old
      beta_cpt_old = beta_cpt_new
      beta_cpt_outside_old = beta_cpt_outside_new
      sigma_old = sigma_new
      if (beta_update_new == beta_update){
        break
      }else if (sigma_update_new == sigma_update){
        break
      }else{
        beta_update = beta_update_new
        sigma_update = sigma_update_new
      }
      iteration = iteration + 1 
      if (iteration > 1e5) stop("The algorithm has beyond the maximum iterations! Probably not converge!")
    }
    
    # Estimate for the train & test
    # train
    resid_train_error = sigma_new*predict_cov_train%*%ed_inv%*%unlist(resid_cpt)
    evaluate_train_tmp = as.list(rep(NA,cpt_no+1))
    for(l in 1:(cpt_no+1)){
      evaluate_train_tmp[[l]] = as.matrix(reg_train_cpt[[l]])%*%beta_cpt_new[[l]]
    }
    evaluate_train = unlist(evaluate_train_tmp) + resid_train_error
    # test
    resid_error = sigma_new*predict_date_cov%*%ed_inv%*%unlist(resid_cpt)
    evaluate_test <- as.matrix(reg_test)%*%beta_cpt_new[[cpt_no+1]]+ resid_error
    beta_new = unlist(beta_cpt_new)
  }else{
    ed1_inv = t(as.matrix(reg_train)) %*% ed_inv %*% as.matrix(reg_train)
    ed2_inv = t(as.matrix(reg_train))%*% ed_inv %*% train_series
    while (sum(abs(beta_update)) > 1e-5 | abs(sigma_update) > 1e-5){
      beta_mean_new = solve(beta_covariance_inv+ed1_inv/sigma_old)%*%(ed2_inv/sigma_old+beta_tmp)
      beta_new = beta_mean_new
      resid = train_series - as.matrix(reg_train)%*%beta_new
      sigma_shape_new = dim(reg_train)[1]/2 + sigma_shape
      sigma_scale_new = as.numeric(t(resid)%*%ed_inv%*%resid/2) + sigma_scale
      sigma_new = as.numeric(sigma_scale_new/(sigma_shape_new-1))
      beta_update_new = beta_new - beta_old
      sigma_update_new = sigma_new - sigma_old
      beta_old = beta_new
      sigma_old = sigma_new
      if (sum(beta_update_new != beta_update) == 0){
        break
      }else if (sigma_update_new == sigma_update){
        break
      }else{
        beta_update = beta_update_new
        sigma_update = sigma_update_new
      }
      iteration = iteration + 1 
      if (iteration > 1e5) stop("The algorithm has beyond the maximum iterations! Probably not converge!")
    }
    resid = train_series - ts(as.matrix(reg_train)%*% beta_new)
    resid_train_error = sigma_new*predict_cov_train%*%ed_inv%*%resid
    evaluate_train = as.matrix(reg_train)%*%beta_new + resid_train_error
    resid_error = sigma_new*predict_date_cov%*%ed_inv%*%resid
    evaluate_test <- as.matrix(reg_test)%*%beta_new + resid_error 
  }
  
  optimal_model = data.frame(matrix(NA,nrow = 1+parameter_no+length(beta_new),ncol = 1))
  optimal_model[1:parameter_no,1] = parameter_total
  optimal_model[(parameter_no+1):(parameter_no+length(unlist(beta_new))),1] = round(beta_new,4)
  optimal_model[1+parameter_no+length(unlist(beta_new)),1] = round(sigma_new,4)
  if (sum(is.na(changepoint_dates) == FALSE)!=0){
    names_changepoint = c(rep(NA,length(colnames(reg_train))*(cpt_no+1)))
    for (i in 1:(cpt_no+1)){
      names_changepoint[(length(colnames(reg_train))*(i-1)+1):(length(colnames(reg_train))*i)] = paste(colnames(reg_train),i)
    }
    names_beta = names_changepoint
  }else{
    names_beta = colnames(reg_train)
  }
  rownames(optimal_model) = c(names(parameter_total),names_beta,"sigma^2")
  colnames(optimal_model) = "Estimate"
  # champion result
  trainfit_champion = exp(evaluate_train)*log_transformation + evaluate_train*(1-log_transformation)
  testfit_champion = exp(evaluate_test)*log_transformation + evaluate_test*(1-log_transformation)
  combinedfit_champion <- as.data.frame(append(trainfit_champion, testfit_champion))
  names(combinedfit_champion) = "Predict"
  full_data_champion <- as.data.frame(cbind(as.data.frame(data),combinedfit_champion)) 
  full_data_champion$Stream = stream
  full_data_champion$Residual = full_data_champion[,stream] - full_data_champion$Predict
  full_data_champion$APE = abs(((full_data_champion[,stream] - full_data_champion$Predict)/full_data_champion[,stream])*100)
  
  return(list(champion_model = optimal_model,
              champion_result = full_data_champion,
              changepoint = paste(changepoint_dates,collapse = ",")))
}

# ::::::::::::::::::::::::::::::::: Function to calculate the quarterly average  mape and smape
find_quarterly_forecast_bayesian <- function(full_data_champion,
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
  #'        
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
              mape = round(abs((forecast-actuals)/actuals)*100,2),
              smape = round(abs(forecast-actuals)/(abs(forecast)+abs(actuals))*100,4)
    ) %>%
    mutate(quarter = paste0(Broadcast_Year, "-", qtr)
    ) %>% 
    subset(select = c("quarter","actuals","forecast","difference","mape","smape")
    ) %>% data.frame()
  
  mape_train_mean = mean(quarterly_forecast[[1]]$mape)
  smape_train_mean = mean(quarterly_forecast[[1]]$smape)
  
  quarterly_forecast[[2]] <- full_data_champion %>% 
    filter(eval(parse(text = text2))) %>%
    filter(eval(parse(text = text3))) %>%
    group_by(Broadcast_Year, qtr) %>%
    summarize(forecast = mean(Predict, na.rm = T),
              actuals = weighted.mean(show_variable, weight_variable),
              difference = actuals - forecast,
              mape = round(abs((forecast-actuals)/actuals)*100,2),
              smape = round(abs(forecast-actuals)/(abs(forecast)+abs(actuals))*100,4)
    ) %>%
    mutate(quarter = paste0(Broadcast_Year, "-", qtr)
    ) %>% 
    subset(select = c("quarter","actuals","forecast","difference","mape","smape")
    ) %>% data.frame()
  
  names(quarterly_forecast) = c("in_sample","out_of_sample")
  
  mape_test_mean = mean(quarterly_forecast[[2]]$mape[!is.nan(quarterly_forecast[[2]]$mape)])
  smape_test_mean = mean(quarterly_forecast[[2]]$smape[!is.nan(quarterly_forecast[[2]]$smape)])
  quarterly_average = c(mape_train_mean,mape_test_mean,smape_train_mean,smape_test_mean)
  names(quarterly_average) = c("MAPE_train", "MAPE_test","SMAPE_train", "SMAPE_test")
  #return(quarterly_forecast)
  return(list(quarterly_forecast = quarterly_forecast,
              quarterly_average = quarterly_average))
}

#:::::::::::::::::::::::::::::::::: Dependent functions
exponential_decay = function(date_diff, alpha){
  nrows = dim(date_diff)[1]
  ncols = dim(date_diff)[2]
  a = matrix(rep(0,nrows*ncols),nrow = nrows)
  a = exp(-alpha*date_diff)
  return(a)
}

date_diff = function(Date){
  nrow = length(Date)
  a = c(rep(0,nrow))
  b = matrix(rep(0,nrow*nrow),nrow=nrow)
  for (i in c(1:nrow)){
    a[i] = as.numeric(Date[i] - Date[1])
  }
  b[1,] = a
  for (j in c(2:nrow)){
    b[j,] = abs(a-a[j])
  }
  return(b)
}

predict_date_diff = function(Date1,Date2){
  nrow = length(Date2)
  ncol = length(Date1)
  a = matrix(rep(NA,nrow*ncol),nrow = nrow)
  for (i in c(1:nrow)){
    a[i,] = Date2[i]-Date1
  }
  return(a)
}

exponential_decay_week = function(date_diff, alpha1, alpha2){
  nrows = dim(date_diff)[1]
  a = matrix(rep(0,nrows*nrows),nrow = nrows)
  a = exp(-alpha1*date_diff)
  value = date_diff[which(date_diff%%7==0,arr.ind = T)]%/%7
  index1 = which(date_diff%%7==0,arr.ind = T)
  index2 = which(value != 0)
  b = a
  for (i in index2){
    b[index1[i,][1],index1[i,][2]] = a[index1[i,][1],index1[i,][2]] + exp(-alpha2*value[i])
  }
  return(b)
}

exponential_decay_year = function(date_diff,alpha2,alpha3){
  nrows = dim(date_diff)[1]
  ncols = dim(date_diff)[2]
  a = matrix(rep(0,nrows*ncols),nrow = nrows)
  value = date_diff[which(date_diff%%7==0,arr.ind = T)]%/%7
  index1 = which(date_diff%%7==0,arr.ind = T)
  index2 = which(value != 0)
  b = a
  for (i in index2){
    b[index1[i,][1],index1[i,][2]] = a[index1[i,][1],index1[i,][2]] + exp(-alpha2*value[i])
  }
  
  value_year = date_diff[which(date_diff%%365==0,arr.ind = T)]%/%365
  index1_year = which(date_diff%%365==0,arr.ind = T)
  index2_year = which(value_year != 0)
  c = b
  for (i in index2_year){
    c[index1_year[i,][1],index1_year[i,][2]] = b[index1_year[i,][1],index1_year[i,][2]] + exp(-alpha3*value_year[i])
  }
  return(c)
}

exponential_decay_year_daily = function(date_diff, alpha1 ,alpha3){
  nrows = dim(date_diff)[1]
  ncols = dim(date_diff)[2]
  a = matrix(rep(0,nrows*ncols),nrow = nrows)
  a = exp(-alpha1*date_diff)
  value_year = date_diff[which(date_diff%%365==0,arr.ind = T)]%/%365
  index1_year = which(date_diff%%365==0,arr.ind = T)
  index2_year = which(value_year != 0)
  c = a
  for (i in index2_year){
    c[index1_year[i,][1],index1_year[i,][2]] = a[index1_year[i,][1],index1_year[i,][2]] + exp(-alpha3*value_year[i])
  }
  return(c)
}
