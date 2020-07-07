#  #######################################################################
#       File-Name:      forecasting_functions.R
#       Version:        R 3.5.2
#       Date:           Apr 15, 2019 (Updated Nov 5, 2019)
#       Author:         Soudeep Deb <Soudeep.Deb@nbcuni.com>
#       Purpose:        contains all functions related to ARIMA and Linear model fitting 
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
library(changepoint)
library(forecast)
library(ecp)
library(marima)

#::::::::::::::::: Function to prepare lookup table for ARIMA modeling

create_arima_lookuptable <- function(max_arima_order,
                                     max_seasonal_order,
                                     periods = 0){
  
  #' @description this function prepares complete list of arima combinations based on
  #' given arima orders, seasonal orders and periods
  #' 
  #' @param max_arima_order of the form (p,d,q) to denote maximum AR order, differencing order and MA order, resp
  #' @param max_seasonal_order of the form (P,D,Q) to denote maximum seasonal AR order, seasonal differencing and seasonal MA order, resp
  #' @param periods denotes the seasonal periods to consider, can be a vector
  #' 
  #' @return R dataframe with 7 columns "p","d","q","P","D","Q","period"
  
  if (length(max_arima_order)!=3 | length(max_seasonal_order)!=3){
    stop("order arguments must have length 3")
  } else {
    ARIMA_p = c(0:max_arima_order[1])
    ARIMA_d = c(0:max_arima_order[2])
    ARIMA_q = c(0:max_arima_order[3])
    Seasonal_P = c(0:max_seasonal_order[1])
    Seasonal_D = c(0:max_seasonal_order[2])
    Seasonal_Q = c(0:max_seasonal_order[3])
    Period = periods
    out = expand.grid(ARIMA_p,ARIMA_d,ARIMA_q,Seasonal_P,Seasonal_D,Seasonal_Q,Period)
    colnames(out) = c("arima_p","arima_d","arima_q","seasonal_p","seasonal_d","seasonal_q","arima_period")
    rownames(out) = c(1:nrow(out))
    return(out)
  }
  
}


#:::::::::::::::::::: FIND CHAMPION MODEL FROM REGULAR UNIVARIATE TIME SERIES DATA

find_champion_arima <- function(data,
                                stream = "SC3_Impressions",
                                agg_timescale = "Date",
                                log_transformation = 1,
                                OOS_start,
                                regressors,
                                keep_regressors = NA,
                                keep_intercept = T,
                                use_boxcox = T,
                                max_changepoints = 0,
                                changepoint_dates = NA,
                                changepoints_minseglen = 1,
                                lookuptable = NULL){
  
  #' @description finds best ARIMA model for the given data according to out-of-sample date 
  #' and conditions provided on regressors and change-points
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used, include `trend` in this vector if you want to use drift/trend in the model
  #' @param keep_regressors vector of regressors to be forced to use in the model, 
  #'                        default is `NA` which would pick appropriate regressors using stepwise selection method,
  #' @param keep_intercept logical to denote if intercept term should be used in the model, default is TRUE
  #' @param max_changepoints no of changepoints to be used in piecewise linear trend, default is 0 (not to use piecewise linear trend)
  #' @param changepoint_dates dates where trend behavior changes, default is `NA` (changepoints to be found using statistical means)
  #' @param changepoint_minseglen integer giving the minimum segment length (no. of observations between changes) for the changepoint function, default is 1
  #' @param lookuptable R dataframe with candidate ARIMA models (typically an output from `create_arima_lookuptable()`)
  #'                    default is NULL (will return best model from `auto.arima()` function)
  #' 
  #' @return list containing the following:
  #' - ARIMA model object
  #' - R dataframe with all details and model fit and forecasts for train/test data
  #' - vector of regressors eventually used in the model
  #' - changepoint dates for piecewise linear function
  
  # check if max_changepoints correspond to changepoint_dates
  if (sum(is.na(changepoint_dates))==0 & length(changepoint_dates)>max_changepoints){
    stop("no of changepoint-dates is more than allowed no of changepoints")
  }
  if (!"trend" %in% regressors & max_changepoints > 0){
    stop("'trend' has to be included in regressors to allow for piecewise linear trend")
  }
  
  # check if keep_regressors is appropriate
  if (sum(is.na(keep_regressors)) == 0 & length(setdiff(keep_regressors,regressors)) > 0){
    stop("keep_regressors has to be a subset of regressors")
  }
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    train <- data %>% filter(Week < OOS_start)
    test <- data %>% filter(Week >= OOS_start)
    cp_timescale <- "Week"
  } else{
    train <- data %>% filter(Date < OOS_start)
    test <- data %>% filter(Date >= OOS_start)
    cp_timescale <- "Date"
  }
  train_periods = nrow(train)
  test_periods = nrow(test)
  
  if (test_periods == 0) stop("no test set found, select appropriate OOS_start")
  
  # prepare data based on log-transformation
  if (log_transformation == 1){
    train_series = log(as.numeric(unlist(train[,stream])) + 0.01)
    test_series = log(as.numeric(unlist(test[,stream])) + 0.01)
  } else{
    train_series = as.numeric(unlist(train[,stream]))
    test_series = as.numeric(unlist(test[,stream]))
  }
  
  # select appropriate set of regressors
  regressors_subset = intersect(regressors,colnames(train))
  
  # initialize xreg matrices for train/test for the linear models
  reg_train <- train[,regressors_subset]
  reg_test <- test[,regressors_subset]
  
  # adjust xreg matrices if needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  
  # adjust for intercept and trend if needed
  if (keep_intercept == T){
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
  
  # build a list of regressor matrices to test the model
  reg_list = list()
  reg_list[[1]] = list(train = reg_train,test = reg_test,cp_date_out = NA)
  case_count = 1
  
  # adjust xreg matrices for piecewise linear trends
  if (max_changepoints > 0){
    if (is.na(changepoint_dates) == F){
      list_of_dates = as.Date(unlist(train[,cp_timescale]))
      knotpoints = as.numeric(lapply(changepoint_dates,FUN = function(x) min(which(list_of_dates >= x))))
      if (length(knotpoints)>0){
        case_count = case_count + 1
        new_train = reg_train
        new_test = reg_test
        for (i in 1:length(knotpoints)){
          new_train <- cbind(new_train,pmax(0,x1-knotpoints[i]))
          newname <- paste("trend_after",knotpoints[i],sep = "_")
          colnames(new_train)[ncol(new_train)] = newname
          new_test <- cbind(new_test,pmax(0,x2-knotpoints[i]))
          colnames(new_test)[ncol(new_test)] = newname
        }
        changepoint_dates = as.data.frame(train[knotpoints,cp_timescale])
        cp_date_out = paste(as.character(changepoint_dates[,cp_timescale]),collapse = ",")
        reg_list[[case_count]] = list(train = new_train,test = new_test,cp_date_out = cp_date_out)
      }
    } else{
      for (i in 1:max_changepoints){
        changepoints <- cpt.mean(as.numeric(unlist(train[,stream])),
                                 method = "BinSeg",
                                 pen.value = "2*log(n)",
                                 Q = i,
                                 minseglen = changepoints_minseglen)
        knotpoints = cpts(changepoints)
        if (length(knotpoints)>0){
          case_count = case_count + 1
          new_train = reg_train
          new_test = reg_test
          for (i in 1:length(knotpoints)){
            new_train <- cbind(new_train,pmax(0,x1-knotpoints[i]))
            newname <- paste("trend_after",knotpoints[i],sep = "_")
            colnames(new_train)[ncol(new_train)] = newname
            new_test <- cbind(new_test,pmax(0,x2-knotpoints[i]))
            colnames(new_test)[ncol(new_test)] = newname
          }
          changepoint_dates = as.data.frame(train[knotpoints,cp_timescale])
          cp_date_out = paste(as.character(changepoint_dates[,cp_timescale]),collapse = ",")
          reg_list[[case_count]] = list(train = new_train,test = new_test,cp_date_out = cp_date_out)
        }
      }
    }
  }
  
  # set up output details dataframe
  col_list = c("type","train_mape","test_mape","total_mape","runtime","log_transformation","boxcox",
               "arima_p","arima_d","arima_q","seasonal_p","seasonal_d","seasonal_q","arima_period",
               "regressors","parameters","estimates","changepoints")
  model_count = 0
  detail_entry = list()
  best_result = 1e99
  best_output = as.data.frame(append(train_series, test_series)) 
  champion_arima = rep(0,7)
  full_lookup = lookuptable
  
  # set up boxcox value
  if (use_boxcox == T) boxcox_term = "auto" else boxcox_term = NULL
  
  # fit all candidate ARIMA models
  for (idx in 1:length(reg_list)){
    
    # set up the train/test data if needed later and fit full LM model
    train_data <- data.frame(cbind(train_series,reg_list[[idx]]$train))
    test_data <- data.frame(cbind(test_series,reg_list[[idx]]$test))
    full_formula <- paste(c("train_series",paste(names(reg_list[[idx]]$train),collapse = "+")),collapse = "~-1+")
    full_model <- lm(formula(full_formula),data = train_data)
    
    # auto-arima out of sample selection
    begin_process = Sys.time()
    run_this <- try(expr = {fit_auto <- auto.arima(train_series,
                                                   max.p = 7,max.P = 7,max.q = 7,max.Q = 7,max.d = 1,max.D = 0,
                                                   seasonal = FALSE,
                                                   trace = FALSE,
                                                   approximation = FALSE,
                                                   allowdrift = FALSE,
                                                   allowmean = FALSE,
                                                   stepwise = TRUE, 
                                                   biasadj = FALSE,
                                                   ic = "aic",
                                                   lambda = boxcox_term,
                                                   xreg = as.matrix(reg_list[[idx]]$train))},silent = T)
    if (sum(class(run_this) == 'try-error') == 0){
      # evaluate the model
      evaluate_auto <- forecast::forecast(fit_auto,h = test_periods,xreg = as.matrix(reg_list[[idx]]$test))
      if (log_transformation == 1){
        trainfit_champion <- exp(evaluate_auto$fitted)
        Predict_champion <- exp(evaluate_auto$mean)
      } else{
        trainfit_champion <- evaluate_auto$fitted
        Predict_champion <- evaluate_auto$mean
      }
      ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = as.numeric(trainfit_champion))
      ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = as.numeric(Predict_champion))
      ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
      if (ModelAccuracy_total < best_result){
        champion_model = fit_auto
        best_result = ModelAccuracy_total
        best_output = as.data.frame(append(trainfit_champion,Predict_champion))
      }
      # find model components
      champion_arima = fit_auto$arma[c(1,6,2,3,7,4,5)]
      parameters = data.frame(fit_auto$coef)
      colnames(parameters) = "Estimates"
      thismodel_reg = intersect(rownames(parameters),names(reg_list[[idx]]$train))
      # name-correction for intercept and trend
      thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
      thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
      thismodel_reg = unique(thismodel_reg)
      end_process = Sys.time()
      runtime = end_process - begin_process
      
      # storing model details components
      model_count = model_count + 1
      detail_entry[[model_count]] = data.frame("auto selection full model",
                                               round(ModelAccuracy_train,2),
                                               round(ModelAccuracy_test,2),
                                               round(ModelAccuracy_total,2),
                                               round(runtime,2),
                                               log_transformation,
                                               round(as.numeric(fit_auto$lambda),2),
                                               champion_arima[1],
                                               champion_arima[2],
                                               champion_arima[3],
                                               champion_arima[4],
                                               champion_arima[5],
                                               champion_arima[6],
                                               champion_arima[7],
                                               paste(as.character(thismodel_reg),collapse = ","),
                                               paste(row.names(parameters),collapse = ","),
                                               paste(round(parameters$Estimates,2),collapse = ","),
                                               reg_list[[idx]]$cp_date_out,
                                               stringsAsFactors = T)
      names(detail_entry[[model_count]]) = col_list
    }
    
    # find the champion model using set of pre-defined lookup table of difference ARMA combinations
    if (is.null(lookuptable) == FALSE){
      tmp <- as.numeric(apply(full_lookup, 1,function(x) return(sum(champion_arima[1:6] < x[1:6]))))
      zeroidx = which(tmp == 0)
      if (length(zeroidx)>0) lookuptable = full_lookup[-zeroidx,]
      total_loop <- nrow(lookuptable)     
      for(i in 1:total_loop){
        begin_process = Sys.time()
        run_this <- try(fit_test <- Arima(train_series,
                                          order = as.numeric(lookuptable[i,c("arima_p","arima_d","arima_q")]),
                                          seasonal = list(order = as.numeric(lookuptable[i,c("seasonal_p","seasonal_d","seasonal_q")]),
                                                          period = as.numeric(lookuptable$arima_period[i])),
                                          include.drift = FALSE, 
                                          include.constant = FALSE,
                                          lambda = boxcox_term,
                                          xreg = as.matrix(reg_list[[idx]]$train),
                                          method = 'CSS'),silent = T)
        if (sum(class(run_this) == "try-error") == 0){
          if (fit_test$code == 0 & sum(is.nan(sqrt(diag(fit_test$var.coef)))) == 0){
            evaluate_test <- forecast::forecast(fit_test,h = test_periods,xreg = as.matrix(reg_list[[idx]]$test))
            if (log_transformation == 1){
              trainfit_champion <- exp(evaluate_test$fitted)
              Predict_champion <- exp(evaluate_test$mean)
            } else{
              trainfit_champion <- evaluate_test$fitted
              Predict_champion <- evaluate_test$mean
            }
            ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = as.numeric(trainfit_champion))
            ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = as.numeric(Predict_champion))
            ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
            if (ModelAccuracy_total < best_result){
              champion_model = fit_test
              best_result = ModelAccuracy_total
              best_output = as.data.frame(append(trainfit_champion,Predict_champion))
            }
            # find model components
            champion_arima = fit_test$arma[c(1,6,2,3,7,4,5)]
            parameters = data.frame(fit_test$coef)
            colnames(parameters) = "Estimates"
            thismodel_reg = intersect(rownames(parameters),names(reg_list[[idx]]$train))
            # name-correction for intercept and trend
            thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
            thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
            thismodel_reg = unique(thismodel_reg)
            end_process = Sys.time()
            runtime = end_process - begin_process
            
            # storing model details components
            model_count = model_count + 1
            detail_entry[[model_count]] = data.frame("lookup with full model",
                                                     round(ModelAccuracy_train,2),
                                                     round(ModelAccuracy_test,2),
                                                     round(ModelAccuracy_total,2),
                                                     round(runtime,2),
                                                     log_transformation,
                                                     round(as.numeric(fit_test$lambda),2),
                                                     champion_arima[1],
                                                     champion_arima[2],
                                                     champion_arima[3],
                                                     champion_arima[4],
                                                     champion_arima[5],
                                                     champion_arima[6],
                                                     champion_arima[7],
                                                     paste(as.character(thismodel_reg),collapse = ","),
                                                     paste(row.names(parameters),collapse = ","),
                                                     paste(round(parameters$Estimates,2),collapse = ","),
                                                     reg_list[[idx]]$cp_date_out,
                                                     stringsAsFactors = T)
            names(detail_entry[[model_count]]) = col_list
          }
        }
      }
    }
    
    # find auto-stepwise selected model
    reduced_model <- step(full_model,trace = 0)
    thismodel_matrix = as.data.frame(model.matrix(reduced_model))
    
    # find the champion model using set of pre-defined lookup table of difference ARMA combinations
    if (is.null(lookuptable) == FALSE){
      total_loop <- nrow(lookuptable)     
      for(i in 1:total_loop){
        begin_process = Sys.time()
        run_this <- try(fit_test <- Arima(train_series,
                                          order = as.numeric(lookuptable[i,c("arima_p","arima_d","arima_q")]),
                                          seasonal = list(order = as.numeric(lookuptable[i,c("seasonal_p","seasonal_d","seasonal_q")]),
                                                          period = as.numeric(lookuptable$arima_period[i])),
                                          include.drift = FALSE, 
                                          include.constant = FALSE,
                                          lambda = boxcox_term,
                                          xreg = as.matrix(thismodel_matrix),
                                          method = 'CSS'),silent = T)
        if (sum(class(run_this) == "try-error") == 0){
          if (fit_test$code == 0 & sum(is.nan(sqrt(diag(fit_test$var.coef)))) == 0){
            evaluate_test <- forecast::forecast(fit_test,h = test_periods,xreg = as.matrix(reg_list[[idx]]$test[,names(thismodel_matrix)]))
            if (log_transformation == 1){
              trainfit_champion <- exp(evaluate_test$fitted)
              Predict_champion <- exp(evaluate_test$mean)
            } else{
              trainfit_champion <- evaluate_test$fitted
              Predict_champion <- evaluate_test$mean
            }
            ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = as.numeric(trainfit_champion))
            ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = as.numeric(Predict_champion))
            ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
            if (ModelAccuracy_total < best_result){
              champion_model = fit_test
              best_result = ModelAccuracy_total
              best_output = as.data.frame(append(trainfit_champion,Predict_champion))
            }
            # find model components
            champion_arima = fit_test$arma[c(1,6,2,3,7,4,5)]
            parameters = data.frame(fit_test$coef)
            colnames(parameters) = "Estimates"
            thismodel_reg = intersect(rownames(parameters),names(reg_list[[idx]]$train))
            # name-correction for intercept and trend
            thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
            thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
            thismodel_reg = unique(thismodel_reg)
            end_process = Sys.time()
            runtime = end_process - begin_process
            
            # storing model details components
            model_count = model_count + 1
            detail_entry[[model_count]] = data.frame("lookup with auto selected model",
                                                     round(ModelAccuracy_train,2),
                                                     round(ModelAccuracy_test,2),
                                                     round(ModelAccuracy_total,2),
                                                     round(runtime,2),
                                                     log_transformation,
                                                     round(as.numeric(fit_test$lambda),2),
                                                     champion_arima[1],
                                                     champion_arima[2],
                                                     champion_arima[3],
                                                     champion_arima[4],
                                                     champion_arima[5],
                                                     champion_arima[6],
                                                     champion_arima[7],
                                                     paste(as.character(thismodel_reg),collapse = ","),
                                                     paste(row.names(parameters),collapse = ","),
                                                     paste(round(parameters$Estimates,2),collapse = ","),
                                                     reg_list[[idx]]$cp_date_out,
                                                     stringsAsFactors = T)
            names(detail_entry[[model_count]]) = col_list
          }
        }
      }
    }
    
    # fit new models based on keep_regressors
    if (sum(is.na(keep_regressors)) == 0) {
      # find reduced model based on defined regressors
      reduced_formula <- paste("train_series",paste(intersect(keep_regressors,names(train_data)),collapse = "+"),sep = "~")
      lower_model <- lm(formula(reduced_formula),data = train_data)
      reduced_model <- step(full_model,scope = list(lower = lower_model,upper = full_model),trace = 0)
      thismodel_matrix = as.data.frame(model.matrix(reduced_model))
      
      # find the champion model using set of pre-defined lookup table of difference ARMA combinations
      if (is.null(lookuptable) == FALSE){
        total_loop <- nrow(lookuptable)     
        for(i in 1:total_loop){
          begin_process = Sys.time()
          run_this <- try(fit_test <- Arima(train_series,
                                            order = as.numeric(lookuptable[i,c("arima_p","arima_d","arima_q")]),
                                            seasonal = list(order = as.numeric(lookuptable[i,c("seasonal_p","seasonal_d","seasonal_q")]),
                                                            period = as.numeric(lookuptable$arima_period[i])),
                                            include.drift = FALSE, 
                                            include.constant = FALSE,
                                            lambda = boxcox_term,
                                            xreg = as.matrix(thismodel_matrix),
                                            method = 'CSS'),silent = T)
          if (sum(class(run_this) == "try-error") == 0){
            if (fit_test$code == 0 & sum(is.nan(sqrt(diag(fit_test$var.coef)))) == 0){
              evaluate_test <- forecast::forecast(fit_test,h = test_periods,xreg = as.matrix(reg_list[[idx]]$test[,names(thismodel_matrix)]))
              if (log_transformation == 1){
                trainfit_champion <- exp(evaluate_test$fitted)
                Predict_champion <- exp(evaluate_test$mean)
              } else{
                trainfit_champion <- evaluate_test$fitted
                Predict_champion <- evaluate_test$mean
              }
              ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = as.numeric(trainfit_champion))
              ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = as.numeric(Predict_champion))
              ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
              if (ModelAccuracy_total < best_result){
                champion_model = fit_test
                best_result = ModelAccuracy_total
                best_output = as.data.frame(append(trainfit_champion,Predict_champion))
              }
              # find model components
              champion_arima = fit_test$arma[c(1,6,2,3,7,4,5)]
              parameters = data.frame(fit_test$coef)
              colnames(parameters) = "Estimates"
              thismodel_reg = intersect(rownames(parameters),names(reg_list[[idx]]$train))
              # name-correction for intercept and trend
              thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
              thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
              thismodel_reg = unique(thismodel_reg)
              end_process = Sys.time()
              runtime = end_process - begin_process
              
              # storing model details components
              model_count = model_count + 1
              detail_entry[[model_count]] = data.frame("lookup with auto selected model",
                                                       round(ModelAccuracy_train,2),
                                                       round(ModelAccuracy_test,2),
                                                       round(ModelAccuracy_total,2),
                                                       round(runtime,2),
                                                       log_transformation,
                                                       round(as.numeric(fit_test$lambda),2),
                                                       champion_arima[1],
                                                       champion_arima[2],
                                                       champion_arima[3],
                                                       champion_arima[4],
                                                       champion_arima[5],
                                                       champion_arima[6],
                                                       champion_arima[7],
                                                       paste(as.character(thismodel_reg),collapse = ","),
                                                       paste(row.names(parameters),collapse = ","),
                                                       paste(round(parameters$Estimates,2),collapse = ","),
                                                       reg_list[[idx]]$cp_date_out,
                                                       stringsAsFactors = T)
              names(detail_entry[[model_count]]) = col_list
            }
          }
        }
      }
    }
  }
  
  # finalize output details table
  all_model_details = do.call(rbind,detail_entry)
  all_model_details$champion = 0
  minidx = which.min(all_model_details$total_mape)
  all_model_details$champion[minidx] = 1
  all_model_details = all_model_details[order(all_model_details$total_mape,decreasing = F),]
  
  # finalize best model output
  names(best_output) = "Predict"
  full_data_champion <- as.data.frame(cbind(as.data.frame(data),best_output)) 
  full_data_champion$Stream = stream
  full_data_champion$Residual = full_data_champion[,stream] - full_data_champion$Predict
  full_data_champion$APE = abs(((full_data_champion[,stream] - full_data_champion$Predict)/full_data_champion[,stream])*100)
  
  return(list(champion_model = champion_model,
              champion_result = full_data_champion,
              all_model_details = all_model_details,
              regressors = as.character(all_model_details$regressors[1]),
              changepoints = as.character(all_model_details$changepoints[1]),
              method = 'ARIMA'))
}


#:::::::::::::::::::: FIND CHAMPION MODEL USING LINEAR REGRESSION

find_champion_lm <- function(data,
                             stream = "SC3_Impressions",
                             agg_timescale = "Date",
                             log_transformation = 0,
                             OOS_start,
                             regressors,
                             keep_regressors = NA,
                             keep_intercept = T,
                             max_changepoints = 0,
                             changepoints_minseglen = 1,
                             changepoint_dates = NA){
  
  #' @description finds best ARIMA model for the given data according to out-of-sample date 
  #' and conditions provided on regressors and change-points
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 0
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used, include `trend` in this vector if you want to use drift/trend in the model
  #' @param keep_regressors vector of regressors to be forced to use in the model, 
  #'                        default is `NA` which would pick appropriate regressors using stepwise selection method,
  #' @param keep_intercept logical to denote if intercept term should be used in the model, default is TRUE
  #' @param max_changepoints maximum no of changepoints to be used in piecewise linear trend, default is 0 (not to use piecewise linear trend)
  #' @param changepoint_dates dates where trend behavior changes, default is `NULL` (changepoints to be found using statistical means)
  #' 
  #' @return list containing the following:
  #' - ARIMA model object
  #' - R dataframe with all details and model fit and forecasts for train/test data
  #' - vector of regressors eventually used in the model
  #' - changepoint dates for piecewise linear function
  
  # check if max_changepoints correspond to changepoint_dates
  if (sum(is.na(changepoint_dates))==0 & length(changepoint_dates)>max_changepoints){
    stop("number of changepoint-dates is more than allowed number of changepoints")
  }
  if (!"trend" %in% regressors & max_changepoints > 0){
    stop("'trend' has to be included in regressors to allow for piecewise linear trend")
  }
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    train <- data %>% filter(Week < OOS_start)
    test <- data %>% filter(Week >= OOS_start)
    cp_timescale <- "Week"
  } else{
    train <- data %>% filter(Date < OOS_start)
    test <- data %>% filter(Date >= OOS_start)
    cp_timescale <- "Date"
  }
  train_periods = nrow(train)
  test_periods = nrow(test)
  
  if (test_periods == 0) stop("no test set found, select appropriate OOS_start")
  
  # prepare data based on log-transformation
  if (log_transformation == 1){
    train_series = log(as.numeric(unlist(train[,stream])) + 0.01)
    test_series = log(as.numeric(unlist(test[,stream])) + 0.01)
  } else{
    train_series = as.numeric(unlist(train[,stream]))
    test_series = as.numeric(unlist(test[,stream]))
  }
  
  # select appropriate set of regressors
  regressors_subset = intersect(regressors,colnames(train))
  
  # initialize xreg matrices for train/test for the linear models
  reg_train <- train[,regressors_subset]
  reg_test <- test[,regressors_subset]
  
  # adjust xreg matrices if needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  
  # adjust for intercept and trend if needed
  if (keep_intercept == T){
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
  
  # build a list of regressor matrices to test the model
  reg_list = list()
  reg_list[[1]] = list(train = reg_train,test = reg_test,cp_date_out = NA)
  case_count = 1
  
  # adjust xreg matrices for piecewise linear trends
  if (max_changepoints > 0){
    if (is.na(changepoint_dates) == F){
      list_of_dates = as.Date(unlist(train[,cp_timescale]))
      knotpoints = as.numeric(lapply(changepoint_dates,FUN = function(x) min(which(list_of_dates >= x))))
      if (length(knotpoints)>0){
        case_count = case_count + 1
        new_train = reg_train
        new_test = reg_test
        for (i in 1:length(knotpoints)){
          new_train <- cbind(new_train,pmax(0,x1-knotpoints[i]))
          newname <- paste("trend_after",knotpoints[i],sep = "_")
          colnames(new_train)[ncol(new_train)] = newname
          new_test <- cbind(new_test,pmax(0,x2-knotpoints[i]))
          colnames(new_test)[ncol(new_test)] = newname
        }
        changepoint_dates = as.data.frame(train[knotpoints,cp_timescale])
        cp_date_out = paste(as.character(changepoint_dates[,cp_timescale]),collapse = ",")
        reg_list[[case_count]] = list(train = new_train,test = new_test,cp_date_out = cp_date_out)
      }
    } else{
      for (i in 1:max_changepoints){
        changepoints <- cpt.mean(as.numeric(unlist(train[,stream])),
                                 method = "BinSeg",
                                 pen.value = "2*log(n)",
                                 Q = i,
                                 minseglen = changepoints_minseglen)
        knotpoints = cpts(changepoints)
        if (length(knotpoints)>0){
          case_count = case_count + 1
          new_train = reg_train
          new_test = reg_test
          for (i in 1:length(knotpoints)){
            new_train <- cbind(new_train,pmax(0,x1-knotpoints[i]))
            newname <- paste("trend_after",knotpoints[i],sep = "_")
            colnames(new_train)[ncol(new_train)] = newname
            new_test <- cbind(new_test,pmax(0,x2-knotpoints[i]))
            colnames(new_test)[ncol(new_test)] = newname
          }
          changepoint_dates = as.data.frame(train[knotpoints,cp_timescale])
          cp_date_out = paste(as.character(changepoint_dates[,cp_timescale]),collapse = ",")
          reg_list[[case_count]] = list(train = new_train,test = new_test,cp_date_out = cp_date_out)
        }
      }
    }
  }
  
  # set up output details dataframe
  col_list = c("type","train_mape","test_mape","total_mape","runtime","log_transformation",
               "regressors","parameters","estimates","changepoints")
  model_count = 0
  detail_entry = list()
  best_result = 1e99
  best_output = as.data.frame(append(train_series, test_series)) 
  
  # fit all candidate linear models
  for (idx in 1:length(reg_list)){
    
    begin_process = Sys.time()
    # set up the linear model data and environment
    train_data <- data.frame(cbind(train_series,reg_list[[idx]]$train))
    test_data <- data.frame(cbind(test_series,reg_list[[idx]]$test))
    full_formula <- paste(c("train_series",paste(names(reg_list[[idx]]$train),collapse = "+")),collapse = "~-1+")
    full_model <- lm(formula(full_formula),data = train_data)
    
    # find set of parameters and regressors
    parameters = data.frame(full_model$coefficients)
    colnames(parameters) = "Estimates"
    thismodel_reg = rownames(parameters)
    # name-correction for intercept and trend
    thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
    thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
    thismodel_reg = unique(thismodel_reg)
    
    # evaluate performance on train and test set
    evaluate_auto <- predict(full_model,newdata = test_data)
    if (log_transformation == 1){
      trainfit_champion <- exp(fitted.values(full_model))
      Predict_champion <- exp(evaluate_auto)
    } else{
      trainfit_champion <- fitted.values(full_model)
      Predict_champion <- evaluate_auto
    }
    ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = trainfit_champion)
    ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = Predict_champion)
    ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
    if (ModelAccuracy_total < best_result){
      champion_model = full_model
      best_result = ModelAccuracy_total
      best_output = as.data.frame(append(trainfit_champion,Predict_champion))
    }
    end_process = Sys.time()
    runtime = end_process - begin_process
    
    # log all details in the table
    model_count = model_count + 1
    detail_entry[[model_count]] = data.frame("full model",
                                             round(ModelAccuracy_train,2),
                                             round(ModelAccuracy_test,2),
                                             round(ModelAccuracy_total,2),
                                             round(runtime,2),
                                             log_transformation,
                                             paste(thismodel_reg,collapse = ","),
                                             paste(row.names(parameters),collapse = ","),
                                             paste(round(parameters$Estimates,2),collapse = ","),
                                             reg_list[[idx]]$cp_date_out,
                                             stringsAsFactors = T)
    names(detail_entry[[model_count]]) = col_list
    
    # find auto-selected best model
    begin_process = Sys.time()
    reduced_model <- step(full_model,trace = 0)
    # find set of parameters and regressors
    parameters = data.frame(reduced_model$coefficients)
    colnames(parameters) = "Estimates"
    thismodel_reg = rownames(parameters)
    # name-correction for intercept and trend
    thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
    thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
    thismodel_reg = unique(thismodel_reg)
    
    # evaluate performance on train and test set
    evaluate_auto <- predict(reduced_model,newdata = test_data)
    if (log_transformation == 1){
      trainfit_champion <- exp(fitted.values(reduced_model))
      Predict_champion <- exp(evaluate_auto)
    } else{
      trainfit_champion <- fitted.values(reduced_model)
      Predict_champion <- evaluate_auto
    }
    ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = trainfit_champion)
    ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = Predict_champion)
    ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
    if (ModelAccuracy_total < best_result){
      champion_model = reduced_model
      best_result = ModelAccuracy_total
      best_output = as.data.frame(append(trainfit_champion,Predict_champion))
    }
    end_process = Sys.time()
    runtime = end_process - begin_process
    
    # log all details in the table
    model_count = model_count + 1
    detail_entry[[model_count]] = data.frame("auto selection",
                                             round(ModelAccuracy_train,2),
                                             round(ModelAccuracy_test,2),
                                             round(ModelAccuracy_total,2),
                                             round(runtime,2),
                                             log_transformation,
                                             paste(thismodel_reg,collapse = ","),
                                             paste(row.names(parameters),collapse = ","),
                                             paste(round(parameters$Estimates,2),collapse = ","),
                                             reg_list[[idx]]$cp_date_out,
                                             stringsAsFactors = T)    
    names(detail_entry[[model_count]]) = col_list
    
    # fit new models based on keep_regressors
    if (sum(is.na(keep_regressors)) == 0) {
      
      begin_process = Sys.time()
      reduced_formula <- paste("train_series",paste(intersect(keep_regressors,names(train_data)),collapse = "+"),sep = "~")
      lower_model <- lm(formula(reduced_formula),data = train_data)
      reduced_model <- step(full_model,scope = list(lower = lower_model,upper = full_model),trace = 0)
      # find set of parameters and regressors
      parameters = data.frame(reduced_model$coefficients)
      colnames(parameters) = "Estimates"
      thismodel_reg = rownames(parameters)
      # name-correction for intercept and trend
      thismodel_reg[thismodel_reg == "Intercept"] = "intercept"
      thismodel_reg[startsWith(thismodel_reg,"trend")] = "trend"
      thismodel_reg = unique(thismodel_reg)
      
      # evaluate performance on train and test set
      evaluate_auto <- predict(reduced_model,newdata = test_data)
      if (log_transformation == 1){
        trainfit_champion <- exp(fitted.values(reduced_model))
        Predict_champion <- exp(evaluate_auto)
      } else{
        trainfit_champion <- fitted.values(reduced_model)
        Predict_champion <- evaluate_auto
      }
      ModelAccuracy_train = mape(actual = as.numeric(unlist(train[,stream])),pred = trainfit_champion)
      ModelAccuracy_test = mape(actual = as.numeric(unlist(test[,stream])),pred = Predict_champion)
      ModelAccuracy_total = ModelAccuracy_train + ModelAccuracy_test
      if (ModelAccuracy_total < best_result){
        champion_model = reduced_model
        best_result = ModelAccuracy_total
        best_output = as.data.frame(append(trainfit_champion,Predict_champion))
      }
      end_process = Sys.time()
      runtime = end_process - begin_process
      
      # log all details in the table
      model_count = model_count + 1
      detail_entry[[model_count]] = data.frame("forced selection",
                                               round(ModelAccuracy_train,2),
                                               round(ModelAccuracy_test,2),
                                               round(ModelAccuracy_total,2),
                                               round(runtime,2),
                                               log_transformation,
                                               paste(thismodel_reg,collapse = ","),
                                               paste(row.names(parameters),collapse = ","),
                                               paste(round(parameters$Estimates,2),collapse = ","),
                                               reg_list[[idx]]$cp_date_out,
                                               stringsAsFactors = T) 
      names(detail_entry[[model_count]]) = col_list
    }
  }
  
  # finalize output details table
  all_model_details = do.call(rbind,detail_entry)
  all_model_details$champion = 0
  minidx = which.min(all_model_details$total_mape)
  all_model_details$champion[minidx] = 1
  all_model_details = all_model_details[order(all_model_details$total_mape,decreasing = F),]
  
  # finalize best model output
  names(best_output) = "Predict"
  full_data_champion <- as.data.frame(cbind(as.data.frame(data),best_output)) 
  full_data_champion$Stream = stream
  full_data_champion$Residual = full_data_champion[,stream] - full_data_champion$Predict
  full_data_champion$APE = abs(((full_data_champion[,stream] - full_data_champion$Predict)/full_data_champion[,stream])*100)
  
  return(list(champion_model = champion_model,
              champion_result = full_data_champion,
              all_model_details = all_model_details,
              regressors = as.character(all_model_details$regressors[1]),
              changepoints = as.character(all_model_details$changepoints[1]),
              method = 'LM'))
}


#:::::::::::::::::::: FIND CHAMPION MODEL FROM MULTIVARIATE DATA

find_champion_arima_BV <- function(data,
                                   show_names,
                                   stream = "SC3_Impressions",
                                   agg_timescale = "Week",
                                   log_transformation = 1,
                                   OOS_start,
                                   regressors,
                                   piecewsise_trend = FALSE,
                                   changepoint_dates = NA,
                                   maximum_AR,
                                   maximum_MA){
  
  #' @description finds best bivariate ARIMA model for the given data according to out-of-sample date 
  #' and conditions provided on regressors and change-points
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param show_names denotes the names of the first and second show, in that order
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week` or `Date`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used in the model
  #' @param piecewise_trend logical; whether to use piecewise linear trend, default is FALSE
  #' @param changepoint_dates dates where trend behavior changes, default is `NULL` (changepoints to be found using statistical means)
  #' @param maximum_AR denotes maximum auto-regressive order to consider, must be greater than 1
  #' @param maximum_MA denotes maximum moving-average order to consider, must be greater than 1
  #' 
  #' @return list containing the following:
  #' - best AR/MA order
  #' - linear model object from the first step for first show
  #' - linear model object from the first step for second show
  #' - multivariate ARIMA model object
  #' - multivariate ARIMA model forecasts
  #' - R dataframe with all details and model fit and forecasts for train/test data for first show
  #' - R dataframe with all details and model fit and forecasts for train/test data for first show
  #' - changepoint dates for piecewise linear function
  
  if (length(show_names) != 2){
    stop("this method works only for two consecutive shows")
  }
  if (min(maximum_AR,maximum_MA) < 2){
    stop("AR/MA orders have to be integers greater than 1")
  }
  
  # create data files for the two shows separately
  agg_dataA <- subset(data,Show_Name == show_names[1])
  agg_dataB <- subset(data,Show_Name == show_names[2])
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    commondates <- intersect(agg_dataA$Week,agg_dataB$Week)
    idxA = which(agg_dataA$Week %in% commondates)
    idxB = which(agg_dataB$Week %in% commondates)
    partA <- agg_dataA[idxA,]
    trainA <- partA %>% filter(Week < OOS_start)
    testA <- partA %>% filter(Week >= OOS_start)
    partB <- agg_dataB[idxB,]
    trainB <- partB %>% filter(Week < OOS_start)
    testB <- partB %>% filter(Week >= OOS_start)
    cp_timescale <- "Week"
  } else{
    commondates <- intersect(agg_dataA$Date,agg_dataB$Date)
    idxA = which(agg_dataA$Date %in% commondates)
    idxB = which(agg_dataB$Date %in% commondates)
    partA <- agg_dataA[idxA,]
    trainA <- partA %>% filter(Date < OOS_start)
    testA <- partA %>% filter(Date >= OOS_start)
    partB <- agg_dataB[idxB,]
    trainB <- partB %>% filter(Date < OOS_start)
    testB <- partB %>% filter(Date >= OOS_start)
    cp_timescale <- "Date"
  }
  
  train_periods <- nrow(trainA)
  test_periods <- nrow(testA)
  
  # get the set of xreg
  regressors_subset = regressors[! regressors %in% c("trend","intercept")]
  
  # prepare xreg matrices for train/test for the arima models
  reg_train <- trainA[,regressors_subset]
  reg_test <- testA[,regressors_subset]
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
  
  # update xreg matrices if needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  final_reg = names(reg_train)
  
  cp_date_out = NA
  if (piecewsise_trend == TRUE){
    if (length(changepoint_dates) > 0){
      knotpoints = which(trainA[,agg_timescale] %in% changepoint_dates)
    } else {
      changepoints <- e.divisive(cbind(c(trainA[,stream]),c(trainB[,stream])),R = 499)
      knotpoints = changepoints$estimates[-c(1,length(changepoints$estimates))]
    }
    if (length(knotpoints)>0){
      for (i in 1:length(knotpoints)){
        reg_train <- cbind(reg_train,pmax(0,x1-knotpoints[i]))
        newname <- paste("trend_after",knotpoints[i],sep = "_")
        colnames(reg_train)[ncol(reg_train)] = newname
        reg_test <- cbind(reg_test,pmax(0,x2-knotpoints[i]))
        colnames(reg_test)[ncol(reg_test)] = newname
      }
      changepoint_dates = trainA[knotpoints,agg_timescale]
      cp_date_out = paste(as.character(changepoint_dates[,cp_timescale]),collapse = ",")
    }
  } 
  
  # use log-transformation if needed
  if (log_transformation == 1){
    trainA_series = log(as.numeric(unlist(trainA[,stream])) + 0.01)
    testA_series = log(as.numeric(unlist(testA[,stream])) + 0.01)
    trainB_series = log(as.numeric(unlist(trainB[,stream])) + 0.01)
    testB_series = log(as.numeric(unlist(testB[,stream])) + 0.01)
  } else{
    trainA_series = as.numeric(unlist(trainA[,stream]))
    testA_series = as.numeric(unlist(testA[,stream]))
    trainB_series = as.numeric(unlist(trainB[,stream]))
    testB_series = as.numeric(unlist(testB[,stream]))
  }
  
  # combine the two shows data into one data.frame
  Y_train <- data.frame(A = trainA_series,B = trainB_series,reg_train)
  Y_test <- data.frame(A = testA_series,B = testB_series,reg_test)
  
  # OLS for the shows, use assigned regressors for A, add A as a regressor for B
  LM_A <- lm(formula(paste("A",paste(names(reg_train),collapse = "+"),sep = "~")),data = Y_train)
  LM_B <- lm(formula(paste("B",paste(c("A",names(reg_train)),collapse = "+"),sep = "~")),data = Y_train)
  
  # fit VARMA for the bivariate vectors for the residuals
  errors <- data.frame(e1 = LM_A$residuals,e2 = LM_B$residuals)
  cutoff = 1e10
  
  # check performances for different AR/MA orders and find the best model
  for (i in 1:maximum_AR){
    for (j in 1:maximum_MA){
      if (i+j>2){
        time_start = Sys.time()
        # fit the MARIMA model to the residuals
        Model1 <- define.model(kvar = ncol(errors), ar = c(0:(i-1)), ma = c(0:(j-1)))
        Marima_fit <- marima(ts(errors),Model1$ar.pattern,Model1$ma.pattern,penalty = 2,Check = F)
        
        # get the forecasts from the marima model
        arima1 <- arima(errors[,1],order = c((i-1),0,(j-1)))
        f1 <- forecast::forecast(arima1,h = test_periods)
        arima2 <- arima(errors[,2],order = c((i-1),0,(j-1)))
        f2 <- forecast::forecast(arima2,h = test_periods)
        Marima_forecast <- arma.forecast(cbind(f1$mean,f2$mean),
                                         marima = Marima_fit,nstart = 0,nstep = test_periods,check = F)
        
        # add back the mean estimates from the previous linear models
        A_forecast = Marima_forecast$forecasts[1,] + predict(LM_A,Y_test)
        Bnewdata = Y_test
        Bnewdata$A = A_forecast
        B_forecast = Marima_forecast$forecasts[2,] + predict(LM_B,Bnewdata)
        A_pred <- rowSums(cbind(fitted(Marima_fit)[1,],fitted(LM_A)),na.rm = T)
        B_pred <- rowSums(cbind(fitted(Marima_fit)[2,],fitted(LM_B)),na.rm = T)
        
        # get the train/test predictions for the original series
        if (log_transformation == 1){
          A_train = mape(actual = exp(Y_train$A),pred = exp(A_pred))
          B_train = mape(actual = exp(Y_train$B),pred = exp(B_pred))
          A_test = mape(actual = exp(Y_test$A),pred = exp(A_forecast))
          B_test = mape(actual = exp(Y_test$B),pred = exp(B_forecast))
        } else {
          A_train = mape(actual = (Y_train$A),pred = (A_pred))
          B_train = mape(actual = (Y_train$B),pred = (B_pred))
          A_test = mape(actual = (Y_test$A),pred = (A_forecast))
          B_test = mape(actual = (Y_test$B),pred = (B_forecast))
        }
        temp = A_train + A_test + B_train + B_test
        
        # check if this beats the current champion
        if(temp < cutoff){
          cutoff = temp
          champion <- data.frame(ARorder = (i-1),MAorder = (j-1))
          if(log_transformation == 1){
            trainfit_champion = exp(A_pred)
            Predict_champion = exp(A_forecast)
            combinedfit_champion <- as.data.frame(append(trainfit_champion, Predict_champion))  
            names(combinedfit_champion) = "Predict"
            
            champA <- as.data.frame(cbind(as.data.frame(partA),combinedfit_champion)) 
            champA$Stream = stream
            champA$Error = ((champA[,stream] - champA$Predict)/champA[,stream])*100
            champA$APE = abs(champA$Error)
            trainfit_champion = exp(B_pred)
            Predict_champion = exp(B_forecast)
            combinedfit_champion <- as.data.frame(append(trainfit_champion, Predict_champion))  
            names(combinedfit_champion) <- "Predict"
            champB <- as.data.frame(cbind(as.data.frame(partB), combinedfit_champion)) 
            champB$Stream = stream
            champB$Error = ((champB[,stream] - champB$Predict)/champB[,stream])*100
            champB$APE = abs(champB$Error)
            champion_model = list(arma_order = champion,
                                  LM_A = LM_A,
                                  LM_B = LM_B,
                                  MARIMA = Marima_fit,
                                  MARIMA_forecast = Marima_forecast)
            champion_result = rbind(champA,champB)
            champion_result = champion_result[order(champion_result$Week),]
          } else{
            trainfit_champion = (A_pred)
            Predict_champion = (A_forecast)
            combinedfit_champion <- as.data.frame(append(trainfit_champion, Predict_champion))  
            names(combinedfit_champion) = "Predict"
            champA <- as.data.frame(cbind(as.data.frame(partA),combinedfit_champion)) 
            champA$Stream = stream
            champA$Error = ((champA[,stream] - champA$Predict)/champA[,stream])*100
            champA$APE = abs(champA$Error)
            trainfit_champion = (B_pred)
            Predict_champion = (B_forecast)
            combinedfit_champion <- as.data.frame(append(trainfit_champion, Predict_champion))  
            names(combinedfit_champion) <- "Predict"
            champB <- as.data.frame(cbind(as.data.frame(partB), combinedfit_champion)) 
            champB$Stream = stream
            champB$Error = ((champB[,stream] - champB$Predict)/champB[,stream])*100
            champB$APE = abs(champB$Error)
            champion_model = list(arma_order = champion,
                                  LM_A = LM_A,
                                  LM_B = LM_B,
                                  MARIMA = Marima_fit,
                                  MARIMA_forecast = Marima_forecast)
            champion_result = rbind(champA,champB)
            champion_result = champion_result[order(champion_result$Week),]
          }
        }
      }
    }
  }
  
  return(list(champion_model = champion_model,
              champion_result = champion_result,
              regressors = paste(final_reg,collapse = ","),
              changepoints = cp_date_out))
}


#:::::::::::::::::::::::::: FUNCTIONS TO FIT THE CHAMPION LINEAR MODEL

fit_champion_lm <- function(data,
                            stream = "SC3_Impressions",
                            agg_timescale = "Date",
                            log_transformation = 1,
                            OOS_start,
                            regressors,
                            changepoint_dates = NA){
  
  #' @description fits best linear regression model for the given data according to out-of-sample date 
  #' and conditions provided on regressors and change-points
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used, include `trend` in this vector if you want to use drift/trend in the model
  #' @param changepoint_dates dates where trend behavior changes, default is `NA` (changepoints to be found using statistical means)
  #' 
  #' @return list containing the following:
  #' - ARIMA model object
  #' - R dataframe with all details and model fit and forecasts for train/test data
  #' - vector of regressors eventually used in the model
  #' - changepoint dates for piecewise linear function
  
  # check if changepoints correspond to regressors
  if (!"trend" %in% regressors & sum(is.na(changepoint_dates))==0){
    regressors = c(regressors,"trend")
  }
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    train <- data %>% filter(Week < OOS_start)
    test <- data %>% filter(Week >= OOS_start)
  } else{
    train <- data %>% filter(Date < OOS_start)
    test <- data %>% filter(Date >= OOS_start)
  }
  train_periods = nrow(train)
  test_periods = nrow(test)
  
  # select appropriate regressor subset
  regressors_subset <- regressors[! regressors %in% c('trend','intercept')]
  
  # prepare xreg matrices for train/test for the arima models
  reg_train <- train[,regressors_subset]
  reg_test <- test[,regressors_subset]
  if ("intercept" %in% regressors){
    Intercept <- rep(1,train_periods)
    reg_train <- cbind(Intercept,reg_train)
    if (test_periods > 0){
      Intercept <- rep(1,test_periods)
      reg_test <- cbind(Intercept,reg_test)
    }
  }
  if ("trend" %in% regressors){
    x1 = c(1:train_periods)
    reg_train <- cbind(reg_train,x1)
    colnames(reg_train)[ncol(reg_train)] = "trend"
    if (test_periods > 0){
      x2 = seq(train_periods+1,train_periods+test_periods,1)
      reg_test <- cbind(reg_test,x2)
      colnames(reg_test)[ncol(reg_test)] = "trend"
    }
  }
  
  # adjust xreg matrices for piecewise linear trends
  if (sum(is.na(changepoint_dates))==0){
    if (agg_timescale == "Week"){
      list_of_dates = as.Date(unlist(train$Week))
    } else{
      list_of_dates = as.Date(unlist(train$Date))
    }
    knotpoints = as.numeric(lapply(changepoint_dates,FUN = function(x) min(which(list_of_dates >= x))))
    for (i in 1:length(knotpoints)){
      reg_train <- cbind(reg_train,pmax(0,x1-knotpoints[i]))
      newname <- paste("trend_after",knotpoints[i],sep = "_")
      colnames(reg_train)[ncol(reg_train)] = newname
      if (test_periods > 0){
        reg_test <- cbind(reg_test,pmax(0,x2-knotpoints[i]))
        colnames(reg_test)[ncol(reg_test)] = newname
      }
    }
  }
  
  # adjust xreg matrices as needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  
  # prepare data based on log-transformation
  if (log_transformation == 1){
    train_series = log(as.numeric(unlist(train[,stream])) + 0.01)
    test_series = log(as.numeric(unlist(test[,stream])) + 0.01)
  } else{
    train_series = as.numeric(unlist(train[,stream]))
    test_series = as.numeric(unlist(test[,stream]))
  }
  
  # set up the linear model data and environment
  train_data <- data.frame(cbind(train_series,reg_train))
  test_data <- data.frame(cbind(test_series,reg_test))
  fm <- paste(c("train_series",paste(names(reg_train),collapse = "+")),collapse = "~-1+")
  
  # fit the linear model
  champion_model <- lm(formula(fm),data = train_data)
  if (test_periods>0){
    evaluate_auto <- stats::predict(champion_model,newdata = test_data)
    if (log_transformation == 1){
      trainfit_champion <- exp(fitted.values(champion_model))
      Predict_champion <- exp(evaluate_auto)
    } else{
      trainfit_champion <- fitted.values(champion_model)
      Predict_champion <- evaluate_auto
    }
    combinedfit_champion <- as.data.frame(append(trainfit_champion, Predict_champion))
  } else {
    if (log_transformation == 1){
      combinedfit_champion <- as.data.frame(exp(fitted.values(champion_model)))
    } else{
      combinedfit_champion <- as.data.frame(fitted.values(champion_model))
    }
  }
  names(combinedfit_champion) = "Predict"
  
  full_data_champion <- as.data.frame(cbind(as.data.frame(data),combinedfit_champion)) 
  full_data_champion$Stream = stream
  full_data_champion$Error = (full_data_champion[,stream] - full_data_champion$Predict)
  full_data_champion$APE = 100*abs(full_data_champion$Error)/full_data_champion[,stream]
  
  return(list(champion_model = champion_model,
              champion_result = full_data_champion,
              changepoints = changepoint_dates))
}


#:::::::::::::::::::::::::: FUNCTIONS TO FIT THE CHAMPION ARIMA MODEL

fit_champion_arima <- function(data,
                               stream = "SC3_Impressions",
                               agg_timescale = "Date",
                               log_transformation = 1,
                               boxcox = NA,
                               OOS_start,
                               regressors,
                               changepoint_dates = NA,
                               ARIMA_order,
                               ARIMA_seasonal){
  
  #' @description fits the best ARIMA model for the given data according to 
  #' conditions provided on regressors, arima orders and change-points
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param boxcox denotes what boxcox transformation to use in ARIMA, default is `NA` (which corresponds to 'auto')
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used in the model
  #' @param changepoint_dates dates where trend behavior changes, default is `NULL` (not to use piecewise linear trend)
  #' @param ARIMA_order of the form (p,d,q) to denote AR order, differencing order and MA order, respectively
  #' @param ARIMA_seasonal list with two compponents `order` [seasonal ARIMA order, of the form (P,D,Q)], and `period`
  #' 
  #' @return list containing the following:
  #' - ARIMA model object
  #' - R dataframe with all details and model fit and forecasts for train/test data
  #' - changepoint dates for piecewise linear function
  
  # check if changepoints correspond to regressors
  if (!"trend" %in% regressors & sum(is.na(changepoint_dates))==0){
    regressors = c(regressors,"trend")
  }
  if (length(ARIMA_order) != 3){
    stop("ARIMA_order should have exactly three components")
  }
  if (sum(is.element(names(ARIMA_seasonal),c("order","period"))) != 2){
    stop("ARIMA_seasonal does not match allowed format")
  }
  if (length(ARIMA_seasonal$order) != 3 | length(ARIMA_seasonal$period) != 1){
    stop("ARIMA_seasonal does not match allowed format")
  }
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    train <- data %>% filter(Week < OOS_start)
    test <- data %>% filter(Week >= OOS_start)
  } else{
    train <- data %>% filter(Date < OOS_start)
    test <- data %>% filter(Date >= OOS_start)
  }
  train_periods = nrow(train)
  test_periods = nrow(test)
  
  # prepare xreg matrices for train/test for the arima models
  regressors_subset = regressors[! regressors %in% c("trend","intercept")]
  reg_train <- train[,regressors_subset]
  reg_test <- test[,regressors_subset]
  if ("intercept" %in% regressors){
    Intercept <- rep(1,train_periods)
    reg_train <- cbind(Intercept,reg_train)
    if (test_periods > 0){
      Intercept <- rep(1,test_periods)
      reg_test <- cbind(Intercept,reg_test)
    }
  }
  if ("trend" %in% regressors){
    x1 = c(1:train_periods)
    reg_train <- cbind(reg_train,x1)
    colnames(reg_train)[ncol(reg_train)] = "trend"
    if (test_periods > 0){
      x2 = seq(train_periods+1,train_periods+test_periods,1)
      reg_test <- cbind(reg_test,x2)
      colnames(reg_test)[ncol(reg_test)] = "trend"
    }
  }
  
  # adjust xreg matrices for piecewise linear trends
  if (sum(is.na(changepoint_dates))==0){
    if (agg_timescale == "Week"){
      list_of_dates = as.Date(unlist(train$Week))
    } else{
      list_of_dates = as.Date(unlist(train$Date))
    }
    knotpoints = as.numeric(lapply(changepoint_dates,FUN = function(x) min(which(list_of_dates >= x))))
    for (i in 1:length(knotpoints)){
      reg_train <- cbind(reg_train,pmax(0,x1-knotpoints[i]))
      newname <- paste("trend_after",knotpoints[i],sep = "_")
      colnames(reg_train)[ncol(reg_train)] = newname
      if (test_periods > 0){
        reg_test <- cbind(reg_test,pmax(0,x2-knotpoints[i]))
        colnames(reg_test)[ncol(reg_test)] = newname
      }
    }
  }
  
  # adjust xreg matrices as needed
  idx = which(colSums(reg_train)==0)
  if (length(idx)>0){
    reg_train <- reg_train[,-idx]
    reg_test <- reg_test[,-idx]
  }
  
  # get the boxcox variable
  if (class(boxcox) != "numeric" | is.na(boxcox) == T) boxcox = 'auto'
  
  # prepare data based on log-transformation
  if (log_transformation == 1){
    our_series = log(as.numeric(unlist(train[,stream])) + 0.01)
  } else{
    our_series = as.numeric(unlist(train[,stream]))
  }
  
  # fit the best Arima model and get forecast
  fit_champion <- Arima(our_series,
                        order = ARIMA_order,
                        seasonal = ARIMA_seasonal,
                        include.drift = FALSE,
                        include.constant = FALSE,
                        lambda = boxcox,
                        xreg = as.matrix(reg_train),
                        method = 'CSS')
  
  # assign the fitted values and forecast
  if (test_periods>0){
    forecasts_champion <- fit_champion %>% forecast::forecast(h = test_periods, xreg = as.matrix(reg_test))
    if (log_transformation == 1){
      trainfit_champion <- exp(fit_champion$fitted)
      Predict_champion <- exp(forecasts_champion$mean)
    } else{
      trainfit_champion <- fit_champion$fitted
      Predict_champion <- forecasts_champion$mean
    }
    combinedfit_champion <- as.data.frame(append(trainfit_champion, Predict_champion))
  } else {
    if (log_transformation == 1){
      combinedfit_champion <- as.data.frame(exp(fit_champion$fitted))
    } else{
      combinedfit_champion <- as.data.frame(fit_champion$fitted)
    }
  }
  names(combinedfit_champion) = "Predict"
  
  full_data_champion <- as.data.frame(cbind(as.data.frame(data),combinedfit_champion)) 
  full_data_champion$Stream = stream
  full_data_champion$Error = (full_data_champion[,stream] - full_data_champion$Predict)
  full_data_champion$APE = 100*abs(full_data_champion$Error)/full_data_champion[,stream]
  
  return(list(champion_model = fit_champion,
              champion_result = full_data_champion,
              changepoints = changepoint_dates)
  )
}


#:::::::::::::::::::::::::: FUNCTIONS TO FIT THE CHAMPION MODEL FOR BIVARIATE ARIMA

fit_champion_arima_BV = function(data,
                                 show_names,
                                 stream = "SC3_Impressions",
                                 agg_timescale = "Week",
                                 log_transformation = 1,
                                 OOS_start,
                                 regressors,
                                 piecewsise_trend = FALSE,
                                 changepoint_dates = NULL,
                                 ARorder,
                                 MAorder){
  
  #' @description fits bivariate ARIMA model for the given data according to 
  #' conditions provided on regressors, parameters and change-points
  #' 
  #' @param data R data-frame, typically an output from `create_aggregated_data()` function (see `data_prep_function.R`)
  #' @param show_names denotes the names of the first and second show, in that order
  #' @param stream denotes which variable to fit the model to, default is `SC3_Impressions`
  #' @param agg_timescale denotes the time-scale of the aggregated data - choices are `Week` or `Date`, default is `Week`
  #' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param regressors vector of all regressors to be used in the model
  #' @param piecewise_trend logical; whether to use piecewise linear trend, default is FALSE
  #' @param changepoint_dates dates where trend behavior changes, default is `NULL` (changepoints to be found using statistical means)
  #' @param ARorder denotes the auto-regressive order to use, must be greater than 1
  #' @param MAorder denotes the moving-average order to use, must be greater than 1
  #' 
  #' @return list containing the following:
  #' - linear model object from the first step for first show
  #' - linear model object from the first step for second show
  #' - multivariate ARIMA model object
  #' - multivariate ARIMA model forecasts
  #' - R dataframe with all details and model fit and forecasts for train/test data for first show
  #' - R dataframe with all details and model fit and forecasts for train/test data for first show
  #' - changepoint dates for piecewise linear function
  
  
  if (length(show_names) != 2){
    stop("this method works only for two consecutive shows")
  }
  if (max(ARorder,MAorder) < 2){
    stop("AR/MA orders have to be integers greater than 1")
  }
  if (piecewsise_trend == T & is.null(changepoint_dates) == T){
    stop("no changepoint date has been provided")
  }
  if (piecewsise_trend == T & ! "trend" %in% regressors){
    regressors = c(regressors,"trend")
  }
  
  # create data files for the two shows separately
  agg_dataA <- subset(data,Show_Name == show_names[1])
  agg_dataB <- subset(data,Show_Name == show_names[2])
  
  # prepare train and test data 
  if (agg_timescale == "Week"){
    commondates <- intersect(agg_dataA$Week,agg_dataB$Week)
    idxA = which(agg_dataA$Week %in% commondates)
    idxB = which(agg_dataB$Week %in% commondates)
    partA <- agg_dataA[idxA,]
    trainA <- partA %>% filter(Week < OOS_start)
    testA <- partA %>% filter(Week >= OOS_start)
    partB <- agg_dataB[idxB,]
    trainB <- partB %>% filter(Week < OOS_start)
    testB <- partB %>% filter(Week >= OOS_start)
    cp_timescale <- "Week"
  } else{
    commondates <- intersect(agg_dataA$Date,agg_dataB$Date)
    idxA = which(agg_dataA$Date %in% commondates)
    idxB = which(agg_dataB$Date %in% commondates)
    partA <- agg_dataA[idxA,]
    trainA <- partA %>% filter(Date < OOS_start)
    testA <- partA %>% filter(Date >= OOS_start)
    partB <- agg_dataB[idxB,]
    trainB <- partB %>% filter(Date < OOS_start)
    testB <- partB %>% filter(Date >= OOS_start)
    cp_timescale <- "Date"
  }
  
  train_periods <- nrow(trainA)
  test_periods <- nrow(testA)
  
  # get the set of xreg
  regressors_subset = regressors[! regressors %in% c("trend","intercept")]
  
  # prepare xreg matrices for train/test for the arima models
  reg_train <- trainA[,regressors_subset]
  reg_test <- testA[,regressors_subset]
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
  
  # adjust xreg matrices for piecewise linear trends
  if (sum(is.na(changepoint_dates)) == 0){
    if (agg_timescale == "Week"){
      list_of_dates = as.Date(unlist(trainA$Week))
    } else{
      list_of_dates = as.Date(unlist(trainA$Date))
    }
    knotpoints = as.numeric(lapply(changepoint_dates,FUN = function(x) min(which(list_of_dates >= x))))
    for (i in 1:length(knotpoints)){
      reg_train <- cbind(reg_train,pmax(0,x1-knotpoints[i]))
      newname <- paste("trend_after",knotpoints[i],sep = "_")
      colnames(reg_train)[ncol(reg_train)] = newname
      if (test_periods > 0){
        reg_test <- cbind(reg_test,pmax(0,x2-knotpoints[i]))
        colnames(reg_test)[ncol(reg_test)] = newname
      }
    }
  }
  
  # use log-transformation if needed
  if (log_transformation == 1){
    trainA_series = log(as.numeric(unlist(trainA[,stream])) + 0.01)
    testA_series = log(as.numeric(unlist(testA[,stream])) + 0.01)
    trainB_series = log(as.numeric(unlist(trainB[,stream])) + 0.01)
    testB_series = log(as.numeric(unlist(testB[,stream])) + 0.01)
  } else{
    trainA_series = as.numeric(unlist(trainA[,stream]))
    testA_series = as.numeric(unlist(testA[,stream]))
    trainB_series = as.numeric(unlist(trainB[,stream]))
    testB_series = as.numeric(unlist(testB[,stream]))
  }
  
  # combine the two shows data into one data.frame
  Y_train = data.frame(A = trainA_series,B = trainB_series,reg_train)
  Y_test = data.frame(A = testA_series,B = testB_series,reg_test)
  
  # OLS for the shows, use assigned regressors for A, add A as a regressor for B
  LM_A = lm(formula(paste("A",paste(names(reg_train),collapse = "+"),sep = "~")),data = Y_train)
  LM_B = lm(formula(paste("B",paste(c("A",names(reg_train)),collapse = "+"),sep = "~")),data = Y_train)
  
  # fit VARMA for the bivariate vectors for the residuals
  errors = data.frame(e1 = LM_A$residuals,e2 = LM_B$residuals)
  
  i = ARorder + 1; j = MAorder + 1
  if (i+j>2){
    # fit the MARIMA model to the residuals
    Model1 = define.model(kvar = ncol(errors), ar = c(0:(i-1)), ma = c(0:(j-1)))
    Marima_fit = marima(ts(errors),Model1$ar.pattern,Model1$ma.pattern,penalty = 2)
    
    # get the forecasts from the marima model
    arima1 = arima(errors[,1],order = c((i-1),0,(j-1)))
    f1 = forecast::forecast(arima1,h = test_periods)
    arima2 = arima(errors[,2],order = c((i-1),0,(j-1)))
    f2 = forecast::forecast(arima2,h = test_periods)
    Marima_forecast = arma.forecast(cbind(f1$mean,f2$mean),
                                    marima = Marima_fit,nstart = 0,nstep = test_periods,check = F)
    
    # add back the mean estimates from the previous linear models
    A_forecast = Marima_forecast$forecasts[1,] + predict(LM_A,Y_test)
    Bnewdata = Y_test
    Bnewdata$A = A_forecast
    B_forecast = Marima_forecast$forecasts[2,] + predict(LM_B,Bnewdata)
    A_pred = rowSums(cbind(fitted(Marima_fit)[1,],fitted(LM_A)),na.rm = T)
    B_pred = rowSums(cbind(fitted(Marima_fit)[2,],fitted(LM_B)),na.rm = T)
    
    if(log_transformation == 1){
      trainfit_championA = exp(A_pred)
      Predict_championA = exp(A_forecast)
      trainfit_championA = exp(B_pred)
      Predict_championA = exp(B_forecast)
    } else{
      trainfit_championA = (A_pred)
      Predict_championA = (A_forecast)
      trainfit_championB = (B_pred)
      Predict_championB = (B_forecast)
    }
    combinedfit_champion = as.data.frame(append(trainfit_champion, Predict_champion))  
    names(combinedfit_champion) = "Predict"
    champA = as.data.frame(cbind(as.data.frame(partA),combinedfit_champion)) 
    champA$Stream = stream
    champA$Error = (champA[,stream] - champA$Predict)
    champA$APE = 100*abs(champA$Error)/champA[,stream]
    combinedfit_champion = as.data.frame(append(trainfit_champion, Predict_champion))  
    names(combinedfit_champion) = "Predict"
    champB = as.data.frame(cbind(as.data.frame(partB), combinedfit_champion)) 
    champB$Stream = stream
    champB$Error = (champB[,stream] - champB$Predict)
    champB$APE = 100*abs(champB$Error)/champB[,stream]
    champion_model = list(LM_A = LM_A,
                          LM_B = LM_B,
                          MARIMA = Marima_fit,
                          MARIMA_forecast = Marima_forecast)
    champion_result = rbind(champA,champB)
    champion_result = champion_result[order(champion_result$Week),]
  }
  
  return(list(champion_model = champion_model,
              champion_result = champion_result,
              changepoints = changepoint_dates))
}