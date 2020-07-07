# functions for daily data exponential decay covariance structure; used in `find_champion_bayesian` function
exponential_decay = function(date_diff, alpha){
  nrows = dim(date_diff)[1]
  ncols = dim(date_diff)[2]
  a = matrix(rep(0,nrows*ncols),nrow = nrows)
  a = exp(-alpha*date_diff)
  return(a)
}

# functions for calculating data difference; used in `find_champion_bayesian` function
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

# functions to calculate the date difference between train and test data; used in `find_champion_bayesian` function
predict_date_diff = function(Date1,Date2){
  nrow = length(Date2)
  ncol = length(Date1)
  a = matrix(rep(NA,nrow*ncol),nrow = nrow)
  for (i in c(1:nrow)){
    a[i,] = Date2[i]-Date1
  }
  return(a)
}

# functions for adding weekly seasonality into daily data covariance structure; used in `find_champion_bayesian` function
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

# functions for adding yearly seasonality into weekly data covariance structure; used in `find_champion_bayesian` function
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

# functions for adding yearly seasonality into daily data covariance structure; used in `find_champion_bayesian` function

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


# function to test for long/short tail; used in `check_normality` function 

kurtosis_test <- function (x) {
  m4 <- sum((x-mean(x))^4)/length(x)
  s4 <- var(x)^2
  kurt <- (m4/s4) - 3
  sek <- sqrt(24/length(x))
  statistic <- kurt/sek
  p <- pt(statistic,(length(x)-1))
  if (p > 0.5){
    p.value = 2*(1-p)
    kurtosis = "long tail"
  } else{
    p.value = 2*p
    kurtosis = "short tail"
  }
  return(list(statistic = statistic,
              kurtosis = kurtosis,
              p.value = p.value))
}

# function to test for skewness; used in `check_normality` function 

skew_test <- function (x) {
  m3 <- sum((x-mean(x))^3)/length(x)
  s3 <- sqrt(var(x))^3
  skew <- m3/s3
  ses <- sqrt(6/length(x))
  statistic <- skew/ses
  p = pt(statistic,(length(x)-1))
  if (p > 0.5){
    p.value = 2*(1-p)
    skew = "right skew"
  } else {
    p.value = 2*p
    skew = "left skew"
  }
  return(list(statistic = statistic,
              skew = skew,
              p.value = p.value))
  
}