#' check stationary assumption for time series observations
#' @description this function works as a stationarity assumption check for a variable of interest
#'
#' @param data denotes input dataframe, usually the results from \code{\link{find_champion_arima}} function
#' @param variable denotes response variable of interest
#' @param log_transformation denotes whether to use log-transformation to the variable, choices are 0 (no) and 1 (yes), default is 1
#' @param start_date denotes the starting date from which the assumption is going to be tested, format has to be \code{"as.Date()"}, default is to use all data
#' @param threshold denotes threshold p-value for various hypothesis testing, default is 0.05
#'
#' @return
#' \item{test_results}{result table for 3 stationary tests}
#'
#' \item{differencing_order}{suggested differencing order to make data stationarity}
#'
#' \item{plot}{ggplot for the variable}
#' 
#' @import tseries
#' @import ggplot2
#' @export
#' 
#' @examples 
#' 
#' check_stationary(USA_AFL_19W019_aggregated, variable = "SC3_Impressions")

check_stationary <- function(data,
                             variable,
                             log_transformation = 0,
                             start_date = NULL,
                             threshold = 0.05){
  
  # take the subset data without NA value
  if (is.null(start_date) == TRUE) start_date = as.Date(min(data$Week))
  subset = data[data$Week >= start_date & is.na(data[,variable]) == F,]
  our_series = as.numeric(unlist(subset[,variable]))
  if (log_transformation == 1) our_series = log(our_series)
  
  # Plot the variable
  res.p <- ggplot(subset,aes(x = as.Date(subset$Week))) +
    geom_rect(aes(xmin = start_date, xmax = as.Date(max(subset$Date)),ymin = -Inf, ymax = Inf), fill = "grey100", alpha = 0.1) +
    geom_line(size = 1, aes(y = our_series), color = "blue") +
    ggtitle(paste("Data since",start_date))+
    scale_x_date("", date_breaks = "1 year", date_labels = "%Y") +
    scale_y_continuous(variable) +
    theme_minimal() +
    theme(axis.text.x = element_text(size = 10),
          axis.text.y = element_text(size = 10),
          axis.title.y = element_text(size = 10),
          legend.text = element_text(size = 10),
          legend.position = "bottom",
          legend.title = element_blank()
    )
  
  # Statistical Tests
  
  # Test 1: Augmented Dickey-Fuller Test
  st1 = round(tseries::adf.test(our_series)$statistic,4)
  p1 = round(tseries::adf.test(our_series)$p.value,3)
  if(p1 >= 0.05) d1 = "N" else d1 = "Y"
  
  # Test 2: Kwiatkowski–Phillips–Schmidt–Shin (KPSS) tests
  st2 = round(tseries::kpss.test(our_series)$statistic,4)
  p2 = round(tseries::kpss.test(our_series)$p.value,3)
  if(p2 >= 0.05) d2 = "Y" else d2 = "N"
  
  # Test 3: Phillips–Perron test
  st3 = round(tseries::pp.test(our_series)$statistic,4)
  p3 = round(tseries::pp.test(our_series)$p.value,3)
  if(p3 >= 0.05) d3 = "N" else d3 = "Y"
  
  test = c("ADF","KPSS","PP")
  st = c(st1,st2,st3)
  p = c(p1,p2,p3)
  d = c(d1,d2,d3)
  test_table = as.data.frame(cbind(test,st,p,d))
  colnames(test_table) = c("Test", "Statistic", "p-value", "Stationary")
  row.names(test_table) = 1:nrow(test_table)
  
  # determine the order for differencing
  diff_order = 0
  residual_diff = our_series
  d1.new = d1
  d2.new = d2
  d3.new = d3
  while (d1.new != "Y" | d2.new != "Y" |d3.new != "Y"){
    residual_diff = diff(residual_diff,lag = 1)
    p1.new = tseries::adf.test(residual_diff)$p.value
    p2.new = tseries::kpss.test(residual_diff)$p.value
    p3.new = tseries::pp.test(residual_diff)$p.value
    p1 = tseries::adf.test(our_series)$p.value
    if(p1.new >= 0.05) d1.new = "N" else d1.new = "Y"
    if(p2.new >= 0.05) d2.new = "Y" else d2.new = "N"
    if(p3.new >= 0.05) d3.new = "N" else d3.new = "Y"
    d.new = c(d1.new,d2.new,d3.new)
    diff_order = diff_order + 1
  }
  
  return(list(test_results = test_table,
              differencing_order = diff_order,
              plot = res.p))
  
}



#' Check normality assumptions for residuals
#' @description this function works as normality assumption check (mainly for residuals) from a fitted model, including shapriro, skewness and kurtosis test
#'
#' @param data denotes the dataframe, usually the results from \code{\link{find_champion_arima}} function
#' @param variable denotes the response variable of interest and take \code{"Residual"} as default
#' @param start_date denotes the starting date from which the assumption is going to be tested, format has to be \code{as.Date()},
#'                   default is to use all data
#' @param threshold denotes the threshold significance level for hypothesis testing, default value is 0.05
#'
#' @return
#' \item{test_results}{result table for shapriro, skewness and kurtosis test}
#'
#' \item{histogram}{histgram plot for targeted data}
#'
#' \item{qqplot}{qqplot for targeted data}
#' @import tseries
#' @import ggplot2
#' @export
#' @examples
#' 
#' daily_regressors = c(
#'                      "Jan","Feb", "Mar", "Apr", "May", "Jun",
#'                      "Jul", "Aug", "Oct", "Nov", "Dec",
#'                      "Sun","Mon","Tue","Thu","Fri","Sat",
#'                      "Easter_ind", "Memorial_Day_ind", "Independence_Day_ind", "Halloween_ind", 
#'                      "Thanksgiving_ind", "Christmas_ind","New_Years_Eve_ind"
#'                     )
#' 
#' champion = find_champion_bayesian(USA_AFL_19W019_aggregated, 
#'                                   OOS_start = as.Date("2018-1-1"),
#'                                   regressors = c(daily_regressors, "trend","intercept"),
#'                                   max_changepoints = 1)
#' champion$champion_result$Residual = champion$champion_result$SC3_Impressions
#'                                         - champion$champion_result$Predict
#' check_residual_normality(champion$champion_result)
#' 
#' 
check_residual_normality <- function(data,
                                     variable = "Residual",
                                     start_date = NULL,
                                     threshold = 0.05){
  
  
  if (! "Residual" %in% names(data)) stop("Residual has to be a column in the data")
  
  # take the subset data without NA values and create residual vector
  if (is.null(start_date) == TRUE) start_date = as.Date(min(data$Week))
  subset = data[data$Week >= start_date & is.na(data[,variable]) == F,]
  residual = as.numeric(unlist(subset[,variable]))
  
  # shapiro test
  shapiro.t = shapiro.test(residual)
  st1 = round(shapiro.t$statistic,3)
  p1 = round(shapiro.t$p.value,3)
  if(p1 >= threshold) d1 = "Normal" else d1 = "Non-normal"
  
  # skewness and kurtosis test
  skew.t = skew_test(residual)
  st2 = round(skew.t$statistic,3)
  p2 = round(skew.t$p.value,3)
  if(p2 >= threshold) d2 = "Not Skew" else d2 = skew.t$skew
  
  kurtosis.t = kurtosis_test(residual)
  st3 = round(kurtosis.t$statistic,3)
  p3 = round(kurtosis.t$p.value,3)
  if(p3 >= threshold) d3 = "Standard" else d3 = kurtosis.t$kurtosis
  
  # test result table
  test = c("shapiro","skewness","kurtosis")
  st = c(st1,st2,st3)
  p = c(p1,p2,p3)
  d = c(d1,d2,d3)
  sugg_d2 = "-"
  sugg_d3 = "-"
  if(d2 == "left skew") sugg_d2 = "Square/CubicRoot/Log" else if (d2 == "right skew") sugg_d2 = "SquareRoot/CubicRoot/Log"
  if (d3 == "short tail") sugg_d3 = "no transformation" else if (d3 == "long tail") sugg_d3 = "Other distribution/Bootstrap"
  suggestions = c("-", sugg_d2, sugg_d3)
  test_table = as.data.frame(cbind(test,st,p,d,suggestions))
  colnames(test_table) = c("Test", "Statistic", "p-value", "Decision","Transform")
  rownames(test_table) = c("normal", "skewness", "tail")
  
  # plot the histogram and the qq plot
  hist.p = qplot(residual, geom = "blank") +
    geom_histogram(aes(y = ..density..), colour = "black", fill = "white",bins = 30) +
    ggtitle(paste("Histogram of the residuals, start date = ",start_date)) +
    stat_density(geom = "line", aes(colour = "bla")) +
    geom_line(aes(x = residual,y = dnorm(residual,mean = mean(residual),sd = sd(residual)),color = "blabla")) +
    scale_colour_manual(name = "", values = c("red", "green"),
                        breaks = c("bla", "blabla"),
                        labels = c("kernel estimate", "normal curve")) +
    theme_minimal() +
    theme(axis.text.x = element_text(size = 10),
          axis.text.y = element_text(size = 10),
          axis.title.y = element_text(size = 10),
          legend.text = element_text(size = 10),
          legend.position = "bottom",
          legend.title = element_blank()
    )
  
  qq.p = ggplot(subset, aes(sample = residual)) +
    stat_qq() +
    stat_qq_line() +
    ggtitle(paste("QQ plot of the residuals, start date = ",start_date))
  
  
  return(list(test_results = test_table,
              histogram = hist.p,
              qqplot = qq.p))
  
}

#' Check outliers
#' @description this function works on outlier detection for a variable of interest
#'
#' @param data denotes the input dataframe, usually the results from \code{\link{find_champion_arima}} function
#' @param variable denotes the response variable of interest and take \code{"Residual"} as default
#' @param start_date denotes the starting date from which the assumption is going to be tested, format has to be \code{as.Date()}, default is to use all data
#' @param extreme_num denotes the number for top most extreme values in residuals, default is 5
#' @param iqr_threshold denotes the threshold value for IQR to detect outliers, default is 3
#'
#' @return
#' \item{outlier_data}{data corresponding to the index for residuals outside IQR threshold}
#'
#' \item{extreme_data}{data corresponding to the most extreme value for residuals}
#'
#' \item{plot}{plot of residuals vs predicted values and extreme points and outliers are marked}
#' @import tseries
#' @import ggplot2
#' @export
#' @examples 
#' 
#' daily_regressors = c(
#'                      "Jan","Feb", "Mar", "Apr", "May", "Jun",
#'                      "Jul", "Aug", "Oct", "Nov", "Dec",
#'                      "Sun","Mon","Tue","Thu","Fri","Sat",
#'                      "Easter_ind", "Memorial_Day_ind", "Independence_Day_ind", "Halloween_ind", 
#'                      "Thanksgiving_ind", "Christmas_ind","New_Years_Eve_ind"
#'                     )
#' 
#' champion = find_champion_bayesian(USA_AFL_19W019_aggregated, 
#'                                   OOS_start = as.Date("2018-1-1"),
#'                                   regressors = c(daily_regressors, "trend","intercept"),
#'                                   max_changepoints = 1)
#' champion$champion_result$Residual = champion$champion_result$SC3_Impressions 
#'                                        - champion$champion_result$Predict
#' check_residual_outlier(champion$champion_result)

check_residual_outlier <- function(data,
                                   variable = "Residual",
                                   start_date = NULL,
                                   extreme_num = 5,
                                   iqr_threshold = 3){
  
  if (! "Residual" %in% names(data)) stop("Residual has to be a column in the data")
  
  # take the subset data without NA values and create residual vector
  if (is.null(start_date) == TRUE) start_date = as.Date(min(data$Week))
  subset = data[data$Week >= start_date & is.na(data[,variable]) == F,]
  residual = as.numeric(unlist(subset[,variable]))
  
  # find the outliers
  resid.q <- quantile(residual,prob = c(0.25,0.75))
  iqr <- diff(resid.q)
  limits <- resid.q + iqr_threshold*iqr*c(-1,1)
  score <- abs(pmin((residual-limits[1])/iqr,0) + pmax((residual - limits[2])/iqr,0))
  outlier = c(rep("non outlier",length(score)))
  outlier[which(score>0)] = paste("outlier outside", iqr_threshold,"IQR")
  show.residual = sort.list(abs(residual), decreasing = TRUE)[1:extreme_num]
  outlier[show.residual] = "extreme"
  
  # prepare the plot
  resid.p = ggplot(subset, aes(x = subset$Predict, y = residual,col = outlier)) +
    ggtitle("Residuals vs Predict") +
    labs(x = "Predict",y = "Residuals") +
    geom_point() +
    geom_text(aes(label=ifelse(score>0,as.character(subset$Week),'')),hjust = 0,vjust = 0,size = 3) +
    theme_minimal() +
    theme(axis.text.x = element_text(size = 10),
          axis.text.y = element_text(size = 10),
          axis.title.y = element_text(size = 10),
          legend.text = element_text(size = 10),
          legend.position = "bottom",
          legend.title = element_blank()
    )
  
  index_iqr = which(score > 0)
  index_extreme = show.residual
  data_iqr = subset[index_iqr,]
  data_extreme = subset[index_extreme,]
  
  return(list(outlier_data = data_iqr,
              extreme_data = data_extreme,
              plot = resid.p))
}

#' Check dependency
#' @description this function works as autocorreltation assumption check for residuals from ARIMA fitted model
#'
#' @param data denotes the input dataframe, usually the results from \code{\link{find_champion_arima}} function
#' @param champion_model denotes the best candidate model as fitted to the main data, usually result from \code{\link{find_champion_arima}} function
#' @param variable denotes the response variable of interest and take \code{"Residual"} as default
#' @param start_date denotes the start date from which the assumption is going to be tested, format has to be \code{as.Date()}, default is to use all data
#' @param existing_lookup R dataframe with candidate ARIMA models used for \code{\link{find_champion_arima}} function
#'
#' @return
#' \item{arma_spec}{arma lag for non-seasonal and seasonal moving average, autocorrelation which lies outside the 95\% signficance level}
#'
#' \item{acfplot}{ggplot for acf}
#'
#' \item{pacfplot}{ggplot for pacf}
#'
#' \item{lookup_difference}{R dataframe with 7 columns "p","d","q","P","D","Q","period", which provides the arima model candidates except for candidates in \code{existing_lookup}.}
#' @import tseries
#' @import ggplot2
#' @export
#' @examples 
#' 
#' OOS_start = as.Date('2018-1-1')
#' 
#' daily_regressors = c(
#' "Jan","Feb", "Mar", "Apr", "May", "Jun",
#' "Jul", "Aug", "Oct", "Nov", "Dec",
#' "Sun","Mon","Tue","Thu","Fri","Sat",
#' "Easter_ind", "Memorial_Day_ind", "Independence_Day_ind", "Halloween_ind", 
#' "Thanksgiving_ind", "Christmas_ind","New_Years_Eve_ind"
#' )
#' 
#' prg_reg = c("Acquired", "Movies", "Encores", "Premieres")
#' 
#' dailylookup = create_arima_lookuptable(c(1,0,1),c(1,0,1), period = 7)
#' 
#' champion = find_champion_arima(
#' data = USA_AFL_19W019_aggregated,
#' show_variable = "SC3_Impressions",
#' agg_timescale = "Date",
#' log_transformation = 0,
#' OOS_start = OOS_start,
#' regressors = c(daily_regressors,prg_reg,"trend"),
#' max_changepoints = 1,
#' changepoint_dates = NA,
#' lookuptable = dailylookup
#' )
#' 
#' champion$champion_result$Residual = champion$champion_result$SC3_Impressions 
#'                                        - champion$champion_result$Predict
#'                                        
#' check_acf_pacf_arima(champion$champion_result, champion$champion_model, existing_lookup = dailylookup)

check_acf_pacf_arima <- function(data,
                                 champion_model,
                                 variable = "Residual",
                                 start_date = NULL,
                                 existing_lookup = NULL){
  
  # check for existence of lookup table
  if (is.null(existing_lookup) == TRUE){
    stop("please give weekly/daily lookup table for current arima model!")
  }
  
  # check if residual column exists
  if (! "Residual" %in% names(data)) stop("Residual has to be a column in the data")
  
  # take the subset data without NA values and create residual vector
  if (is.null(start_date) == TRUE) start_date = as.Date(min(data$Week))
  subset = data[data$Week >= start_date & is.na(data[,variable]) == F,]
  residual = as.numeric(unlist(subset[,variable]))
  
  # get the lag order for current arima model
  nsma.order = champion_model$arma[2]
  nsar.order = champion_model$arma[1]
  sma.order = champion_model$arma[4]
  sar.order = champion_model$arma[3]
  
  # Plot the residuals
  acf.p = ggAcf(residual, lag.max = NULL, na.action = na.pass, plot = TRUE) +
    ggtitle(paste("Residuals ACF plot, start date = ",start_date))
  pacf.p = ggPacf(residual, lag.max = NULL, na.action = na.pass, plot = TRUE)+
    ggtitle(paste("Residuals PACF plot, start date = ",start_date))
  significance_level = qnorm((1 + 0.95)/2)/sqrt(sum(!is.na(residual)))
  ma.lag = acf.p$data$lag[acf.p$data$Freq > significance_level]
  ar.lag = pacf.p$data$lag[pacf.p$data$Freq > significance_level]
  
  period = unique(existing_lookup$arima_period)[1]
  sma.lag = ma.lag[which(ma.lag%%period == 0)]
  nsma.lag = ma.lag[which(ma.lag%%period != 0)]
  if (length(sma.lag)!=0){
    sma.lag.suggest = max(sma.lag%/%period) + sma.order
  } else{
    sma.lag = "-"
    sma.lag.suggest = sma.order
  }
  if (length(nsma.lag)!=0){
    nsma.lag.suggest = max(unique(nsma.lag%%period))
  } else{
    nsma.lag = "-"
    nsma.lag.suggest = 0
  }
  sar.lag = ar.lag[which(ar.lag%%period ==0)]
  nsar.lag = ar.lag[which(ar.lag%%period !=0)]
  if (length(sar.lag)!=0){
    sar.lag.suggest = max(sar.lag%/%period) + sar.order
  }else{
    sar.lag = "-"
    sar.lag.suggest = sar.order
  }
  if (length(nsar.lag)!=0){
    nsar.lag.suggest = max(unique(nsar.lag%%period))
  }else{
    nsar.lag = "-"
    nsar.lag.suggest = 0
  }
  
  arma_lag = data.frame(matrix(NA,nrow = 4,ncol = 2))
  arma_lag[1,1] = paste(nsma.lag,collapse = ", ")
  arma_lag[2,1] = paste(nsar.lag,collapse = ", ")
  arma_lag[3,1] = paste(sma.lag,collapse = ", ")
  arma_lag[4,1] = paste(sar.lag,collapse = ", ")
  colnames(arma_lag) = c("significant_lags","suggested_lag")
  row.names(arma_lag) = c("non-seasonal-MA","non-seasonal-AR","seasonal-MA","seasonal-AR")
  
  arma_lag$suggested_lag = as.numeric(c(nsma.lag.suggest,nsar.lag.suggest,sma.lag.suggest,sar.lag.suggest))
  
  suggest_lag = c("nsma lag" = nsma.lag.suggest,"nsar lag" = nsar.lag.suggest,
                  "sma lag" = sma.lag.suggest,"sar lag" = sar.lag.suggest)
  
  # find the lookup-table set on which the model can be updated to
  lookupnew <- create_arima_lookuptable(max_arima_order = c(nsar.lag.suggest,max(existing_lookup$arima_d),nsma.lag.suggest),
                                        max_seasonal_order = c(sar.lag.suggest,max(existing_lookup$seasonal_d),sma.lag.suggest),
                                        periods = period)
  if (dim(existing_lookup) != dim(lookupnew)){
    lookup_diff = setdiff(lookupnew,existing_lookup)
  }else{
    lookup_diff = NA
  }
  
  return(list(arma_spec = arma_lag,
              acfplot = acf.p,
              pacfplot = pacf.p,
              lookup_difference = lookup_diff))
}