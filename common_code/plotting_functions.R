#  #######################################################################
#       File-Name:      plotting_functions.R
#       Version:        R 3.4.4
#       Date:           Feb 13, 2019
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

#::::::::::::::::::::: Function to create daily forecast plot

daily_forecast_plot <- function(full_data_champion,
                                show_variable = "SC3_Impressions",
                                OOS_start,
                                Last_Actual_Date = as.Date("2099-12-31"),
                                title = F){
  
  #' @description this function creates daily forecast plot, and should be used when daily model is fitted
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #' 
  #' @return A GGplot to show the actuals and forecasts for daily model
  
  names(full_data_champion)[which(names(full_data_champion)==show_variable)] = "show_variable"
  maxval <- max(c(full_data_champion$show_variable,full_data_champion$Forecast),na.rm = T)
  if (title==F ) g_title <- element_blank() else g_title <- title
  
  if (show_variable %in% c("LS_Imps","SC3_Impressions","L_Imps")){
    logbase10 <- ceiling(log(maxval,base = 10))
    scalefactor = 0.5^(logbase10%%2)*10^(4 + ceiling((logbase10 - 4)/2))
    maxval <- maxval%/%scalefactor + 1
    plot1 <- full_data_champion %>% 
      subset(Date <= Last_Actual_Date) %>% 
      ggplot(aes(x = Date)) +
      geom_rect(aes(xmin = OOS_start, xmax = max(Date),ymin = -Inf, ymax = Inf), fill = "grey80", alpha = 0.1) +
      geom_line(size = 0.3, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 0.3, aes(y = Predict, color = "Forecast")) +
      scale_x_date("", date_breaks = "1 year", date_labels = "%Y") +
      scale_y_continuous("Impressions",
                         labels =  unit_format(accuracy = 1,
                                               suffix = "K", 
                                               scale = 1e-3,
                                               trim = FALSE,
                                               big.mark = ""),
                         breaks = c(0:maxval)*scalefactor,
                         limits = c(0,maxval*scalefactor)) +
      scale_color_manual(values = c("Actuals" = "red", "Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20),
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      )
  } else {
    maxval <- ceiling(maxval)
    plot1 <- full_data_champion %>% 
      subset(Date <= Last_Actual_Date) %>% 
      ggplot(aes(x = Date)) +
      geom_rect(aes(xmin = OOS_start, xmax = max(Date), ymin = -Inf, ymax = Inf),fill = "grey80", alpha = 0.1) +
      geom_line(size = 0.3, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 0.3, aes(y = Predict, color = "Forecast")) +
      scale_x_date("", date_breaks = "1 year", date_labels = "%Y") +
      scale_y_continuous("Rating",breaks = c(0:maxval),limits = c(0,maxval)) +
      scale_color_manual(values = c("Actuals" = "red","Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20),
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      )
  }
  
  return(plot1)
}

#:::::::::::::::::::::::: Function to create weekly forecast plot

weekly_forecast_plot <- function(full_data_champion,
                                 show_variable = "SC3_Impressions",
                                 OOS_start,
                                 Last_Actual_Date = as.Date("2099-12-31"), 
                                 title=F){
  
  #' @description this function creates weekly forecast plot, and should be used when weekly model is fitted
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #' 
  #' @return A GGplot to show the actuals and forecasts for weekly model
  
  names(full_data_champion)[which(names(full_data_champion)==show_variable)] = "show_variable"
  maxval <- max(c(full_data_champion$show_variable,full_data_champion$Forecast),na.rm = T)
  
  if (title==F ) g_title <- element_blank() else g_title <- title
  
  if (show_variable %in% c("LS_Imps","SC3_Impressions","L_Imps")){
    logbase10 <- ceiling(log(maxval,base = 10))
    scalefactor = 0.5^(logbase10%%2)*10^(4 + ceiling((logbase10 - 4)/2))
    maxval <- maxval%/%scalefactor + 1
    plot1 <- full_data_champion %>% 
      subset(Week <= Last_Actual_Date) %>% 
      ggplot(aes(x = Week)) +
      geom_rect(aes(xmin = OOS_start, xmax = max(Week), ymin = -Inf, ymax = Inf), fill = "grey80", alpha = 0.1) +
      geom_line(size = 0.3, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 0.3, aes(y = Predict, color = "Forecast")) +
      scale_x_date("", date_breaks = "1 year", date_labels = "%Y") +
      scale_y_continuous("Impressions",
                         labels =  unit_format(accuracy = 1,
                                               suffix = "K", 
                                               scale = 1e-3,
                                               trim = FALSE,
                                               big.mark = ""),
                         breaks = c(0:maxval)*scalefactor,
                         limits = c(0,maxval*scalefactor)) +
      scale_color_manual(values = c("Actuals" = "red", "Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20),
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title =  element_blank()
      )
  } else {
    maxval = ceiling(maxval)
    plot1 <- full_data_champion %>% 
      subset(Week <= Last_Actual_Date) %>% 
      ggplot(aes(x = Week)) +
      geom_rect(aes(xmin = OOS_start, xmax = max(Week), ymin = -Inf, ymax = Inf), fill = "grey80", alpha = 0.1) +
      geom_line(size = 0.3, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 0.3, aes(y = Predict, color = "Forecast")) +
      scale_x_date("", date_breaks = "1 year", date_labels = "%Y") +
      scale_y_continuous("Rating",breaks = c(0:maxval),limits = c(0,maxval)) +
      scale_color_manual(values = c("Actuals" = "red","Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20),
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      )
  }
  
  return(plot1)
}

# function to create quarterly forecast plot
quarterly_forecast_plot <- function(full_data_champion,
                                    show_variable = "SC3_Impressions",
                                    weight_variable = "SC3_C_Dur",
                                    network = "Cable",
                                    timescale = "Week",
                                    OOS_start,
                                    Last_Actual_Date = as.Date("2099-12-31"),
                                    title=F){
  
  #' @description this function creates quarterly forecast plot
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param weight_variable denotes which weighting variable to be used to calculate means, default is `SC3_C_Dur`
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param timescale denotes the time-scale of the model - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #' 
  #' @return A GGplot to show the actuals and forecasts on quarterly level
  
  if (timescale == "Week"){
    text3 <- "Week<=Last_Actual_Date"
  } else{
    text3 <- "Date<=Last_Actual_Date"
  }
  
  if (title==F) g_title <- element_blank() else g_title <- title
  quarterly_forecast <- find_quarterly_forecast(full_data_champion,show_variable,weight_variable,
                                                network,timescale,OOS_start,Last_Actual_Date)
  names(full_data_champion)[which(names(full_data_champion)==show_variable)] = "show_variable"
  names(full_data_champion)[which(names(full_data_champion)==weight_variable)] = "weight_variable"
  
  maxval <- max(c(full_data_champion$show_variable,full_data_champion$Forecast),na.rm = T)
  
  if (show_variable %in% c("LS_Imps","SC3_Impressions","L_Imps")){
    logbase10 <- ceiling(log(maxval,base = 10))
    scalefactor = 0.5^(logbase10%%2)*10^(4 + ceiling((logbase10 - 4)/2))
    maxval <- maxval%/%scalefactor + 1
    plot2 <- full_data_champion %>% 
      filter(eval(parse(text = text3))) %>% 
      group_by(Broadcast_Year, Cable_Qtr) %>%
      summarize(
        Forecast = mean(Predict,na.rm = T),
        show_variable = weighted.mean(show_variable, weight_variable, na.rm = T)
      ) %>%
      mutate(
        year_quarter = paste0(Broadcast_Year, "-", Cable_Qtr)
      ) %>%
      ungroup() %>%
      ggplot(aes(x = year_quarter, group = 1)) +
      geom_rect(aes(xmin = quarterly_forecast$out_of_sample$quarter[1], xmax = max(year_quarter),
                    ymin = -Inf, ymax = Inf),
                fill = "grey80", alpha = 0.1) +
      geom_line(size = 1, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 1, aes(y = Forecast, color = "Forecast")) +
      scale_x_discrete("") +
      scale_y_continuous("Impressions",
                         labels =  unit_format(accuracy = 1,
                                               suffix = "K", 
                                               scale = 1e-3,
                                               trim = FALSE,
                                               big.mark = ""),
                         breaks = c(0:maxval)*scalefactor,
                         limits = c(0,maxval*scalefactor)) +
      scale_color_manual(values = c("Actuals" = "red","Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20, angle=90, hjust=1), 
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      ) 
  } else {
    maxval = ceiling(maxval)
    plot2 <- full_data_champion %>% 
      filter(eval(parse(text = text3))) %>% 
      group_by(Broadcast_Year, Broadcast_Qtr) %>%
      summarize(
        Forecast = mean(Predict,na.rm = T),
        show_variable = weighted.mean(show_variable, weight_variable, na.rm = T)
      ) %>%
      mutate(
        year_quarter = paste0(Broadcast_Year, "-", Broadcast_Qtr)
      ) %>%
      ungroup() %>%
      ggplot(aes(x = year_quarter, group = 1)) +
      geom_rect(aes(xmin = quarterly_forecast$out_of_sample$quarter[1], xmax = max(year_quarter),
                    ymin = -Inf, ymax = Inf),
                fill = "grey80", alpha = 0.1) +
      geom_line(size = 1, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 1, aes(y = Forecast, color = "Forecast")) +
      scale_x_discrete("") +
      scale_y_continuous("Rating",
                         breaks = c(0:maxval),
                         limits = c(0,maxval)) +
      scale_color_manual(values = c("Actuals" = "red", "Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20, angle=90, hjust=1), 
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      ) 
  }
  
  return(plot2)
}


# function to create quarterly forecast plot
quarterly_forecast_plot2 <- function(full_data_champion,
                                     show_variable = "SC3_Impressions",
                                     weight_variable = "SC3_C_Dur",
                                     network = "Cable",
                                     timescale = "Week",
                                     OOS_start,
                                     title=F,
                                     Last_Actual_Date = as.Date("2099-12-31")){
  
  #' @description this function creates quarterly forecast plot
  #' 
  #' @param full_data_champion R dataframe with results from fitted model, must have a column called `Predict`
  #' @param show_variable denotes which variable the model was fitted to, default is `SC3_Impressions`
  #' @param weight_variable denotes which weighting variable to be used to calculate means, default is `SC3_C_Dur`
  #' @param network it has to be either `Broadcast` or `Cable`, default is `Cable`
  #' @param timescale denotes the time-scale of the model - choices are `Week`, `Date`, `Hour` or `Half_Hr`
  #' @param OOS_start the first date for the out-of-sample, format has to be `as.Date()`
  #' @param Last_Actual_Date the date up to which the calculations to be done, format has to be `as.Date()`
  #' 
  #' @return A GGplot to show the actuals and forecasts on quarterly level
  
  if (timescale == "Week"){
    text3 <- "Week<=Last_Actual_Date"
  } else{
    text3 <- "Date<=Last_Actual_Date"
  }
  
  if (title==F) g_title <- element_blank() else g_title <- title
  
  quarterly_forecast <- find_quarterly_forecast(full_data_champion,show_variable,weight_variable,
                                                network,timescale,OOS_start,Last_Actual_Date)
  
  names(full_data_champion)[which(names(full_data_champion)==show_variable)] = "show_variable"
  names(full_data_champion)[which(names(full_data_champion)==weight_variable)] = "weight_variable"
  
  maxval <- max(c(full_data_champion$show_variable,full_data_champion$Forecast),na.rm = T)
  
  if (show_variable %in% c("LS_Imps","SC3_Impressions","L_Imps")){
    logbase10 <- ceiling(log(maxval,base = 10))
    scalefactor = 0.5^(logbase10%%2)*10^(4 + ceiling((logbase10 - 4)/2))
    maxval <- maxval%/%scalefactor + 1
    plot2 <- full_data_champion %>% 
      filter(eval(parse(text = text3))) %>% 
      group_by(Broadcast_Year, Cable_Qtr) %>%
      summarize(
        Forecast = mean(Predict,na.rm = T),
        show_variable = weighted.mean(show_variable, weight_variable, na.rm = T)
      ) %>%
      mutate(
        year_quarter = paste0(Broadcast_Year, "-", Cable_Qtr)
      ) %>%
      ungroup() %>%
      ggplot(aes(x = year_quarter, group = 1)) +
      geom_rect(aes(xmin = quarterly_forecast$out_of_sample$quarter[1], xmax = max(year_quarter),
                    ymin = -Inf, ymax = Inf),
                fill = "grey80", alpha = 0.1) +
      geom_line(size = 1, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 1, aes(y = Forecast, color = "Forecast")) +
      scale_x_discrete("") +
      scale_y_continuous("Impressions",
                         labels =  unit_format(accuracy = 1,
                                               suffix = "K", 
                                               scale = 1e-3,
                                               trim = FALSE,
                                               big.mark = ""),
                         breaks = c(0:maxval)*scalefactor,
                         limits = c(0,maxval*scalefactor)) +
      scale_color_manual(values = c("Actuals" = "red","Forecast" = "blue") ) +
      theme_minimal() +
      ggtitle(g_title) +
      theme(axis.text.x = element_text(size = 20, angle=90, hjust=1), 
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      ) 
  } else {
    #print('here1')
    #research_forecast = as.symbol(paste0('res_', show_variable)) 
    #print(research_forecast)
    maxval = ceiling(maxval)
    plot2 <- full_data_champion %>% 
      filter(eval(parse(text = text3))) %>% 
      group_by(Broadcast_Year, Broadcast_Qtr) %>%
      summarize(
        Forecast = mean(Predict,na.rm = T),
        show_variable = weighted.mean(show_variable, weight_variable, na.rm = T),
        research_forecast = mean(res_LS_Rating, na.rm = T) ) %>% 
      mutate(
        year_quarter = paste0(Broadcast_Year, "-", Broadcast_Qtr)
      ) %>%
      ungroup() %>%
      #print(head(.)) %>%
      ggplot(aes(x = year_quarter, group = 1)) +
      geom_rect(aes(xmin = quarterly_forecast$out_of_sample$quarter[1], xmax = max(year_quarter),
                    ymin = -Inf, ymax = Inf),
                fill = "grey80", alpha = 0.1) +
      geom_line(size = 1, aes(y = show_variable, color = "Actuals")) + 
      geom_line(size = 1, aes(y = Forecast, color = "Forecast")) +
      geom_line(size = 1, aes(y = research_forecast, color= "Res Forecast")) +
      scale_x_discrete("") +
      scale_y_continuous("Rating",
                         breaks = c(0:maxval),
                         limits = c(0,maxval)) +
      scale_color_manual(values = c("Actuals" = "red", "Forecast" = "blue", "Res Forecast" = "orange") ) +
      ggtitle(g_title) +
      theme_minimal() +
      theme(axis.text.x = element_text(size = 20, angle=90, hjust=1), 
            axis.text.y = element_text(size = 20),
            axis.title.y = element_text(size = 20),
            legend.text = element_text(size = 20),
            legend.position = "bottom", 
            legend.title = element_blank()
      ) 
  }
  
  return(plot2)
}