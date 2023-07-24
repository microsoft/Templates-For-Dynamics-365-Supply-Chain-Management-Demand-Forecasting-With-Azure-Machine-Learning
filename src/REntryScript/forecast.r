library(forecast)
library(plyr)
library(zoo)

MAX_DECIMAL <- 79228162514264337593543950335

#Time series models
TIME_SERIES_MODEL_ALL = "ALL"
TIME_SERIES_MODEL_ARIMA = "ARIMA"
TIME_SERIES_MODEL_ETS = "ETS"
TIME_SERIES_MODEL_STL = "STL"
TIME_SERIES_MODEL_ETS_ARIMA = "ETS+ARIMA"
TIME_SERIES_MODEL_ETS_STL = "ETS+STL"

TIME_SERIES_MODELS = c(TIME_SERIES_MODEL_ALL, TIME_SERIES_MODEL_ARIMA, TIME_SERIES_MODEL_ETS, TIME_SERIES_MODEL_STL, TIME_SERIES_MODEL_ETS_ARIMA, TIME_SERIES_MODEL_ETS_STL)

#Missing value substitution options
MISSING_VALUE_MEAN = "MEAN"
MISSING_VALUE_PREVIOUS = "PREVIOUS"
MISSING_VALUE_INTERPOLATION_LINEAR = "INTERPOLATE LINEAR"
MISSING_VALUE_INTERPOLATION_POLYNOMIAL = "INTERPOLATE POLYNOMIAL"

MISSING_VALUE_OPTIONS = c(MISSING_VALUE_MEAN, MISSING_VALUE_PREVIOUS, MISSING_VALUE_INTERPOLATION_LINEAR, MISSING_VALUE_INTERPOLATION_POLYNOMIAL)


#Values for the FORCE_SEASONALITY
FORCE_SEASONALITY_AUTO = "AUTO"
FORCE_SEASONALITY_NONE = "NONE"
FORCE_SEASONALITY_ADDITIVE = "ADDITIVE"
FORCE_SEASONALITY_MULTIPLICATIVE = "MULTIPLICATIVE"

FORCE_SEASONALITY_OPTIONS = c(FORCE_SEASONALITY_AUTO, FORCE_SEASONALITY_NONE, FORCE_SEASONALITY_ADDITIVE, FORCE_SEASONALITY_MULTIPLICATIVE)


#Values for the MISSING_VALUE_SCOPE option
MISSING_VALUE_SCOPE_GLOBAL = "GLOBAL"
MISSING_VALUE_SCOPE_GRANULARITY_ATTRIBUTE = "GRANULARITY_ATTRIBUTE"
MISSING_VALUE_SCOPE_HISTORY_DATE_RANGE = "HISTORY_DATE_RANGE"

MISSING_VALUE_SCOPE_OPTIONS = c(MISSING_VALUE_SCOPE_GLOBAL, MISSING_VALUE_SCOPE_GRANULARITY_ATTRIBUTE, MISSING_VALUE_SCOPE_HISTORY_DATE_RANGE)

order_and_fill_missing_values <- function(data, missingValueSubstitution, min_datekey, max_datekey) {

    #Order data by date key
    data <- data[order(data$DATEKEY),]

    #Get minimum and maximum date keys
    if (is.na(min_datekey)) {
        min_datekey <- head(data$DATEKEY, 1)
    }
    if (is.na(max_datekey)) {
        max_datekey <- tail(data$DATEKEY, 1)
    }

    #Fill records with missing date keys with NaN 
    filled_data <- merge(data.frame(list(DATEKEY = seq(min_datekey, max_datekey))), data, all = T)

    #Replace NaN with a value, so forecast models knows that there was no data.
    if (is.numeric(missingValueSubstitution)) {
        filled_data$TRANSACTIONQTY[is.na(filled_data$TRANSACTIONQTY)] <- missingValueSubstitution
    }
    else if (missingValueSubstitution == MISSING_VALUE_MEAN) {
        filled_data$TRANSACTIONQTY[is.na(filled_data$TRANSACTIONQTY)] <- mean(filled_data$TRANSACTIONQTY, na.rm = TRUE)
    }
    else if (missingValueSubstitution == MISSING_VALUE_PREVIOUS) {
        filled_data$TRANSACTIONQTY <- na.locf(filled_data$TRANSACTIONQTY, na.rm = FALSE)
    }
    else if (missingValueSubstitution == MISSING_VALUE_INTERPOLATION_LINEAR) {
        filled_data$TRANSACTIONQTY <- na.approx(filled_data$TRANSACTIONQTY, na.rm = FALSE)
    }
    else if (missingValueSubstitution == MISSING_VALUE_INTERPOLATION_POLYNOMIAL) {
        filled_data$TRANSACTIONQTY <- na.spline(filled_data$TRANSACTIONQTY)
    }

    return(filled_data)
}

mape <- function(observed, predicted) {

    observedLenght <- length(observed);

    if (observedLenght == 0) {
        return(-1);
    }

    #The same number of predicted and observed elements must be used
    predicted <- head(predicted, n = observedLenght);

    ape <- (abs(observed - predicted)) / observed * 100;
    mape <- mean(ape, na.rm = TRUE)

    if (is.nan(mape)) {
        mape <- -1;
    }

    return(mape);
}

modelArima <- function(historicalData, forecastHorizon, confidenceLevel, forceSeasonality) {
    return(tryCatch({
        force_seasonality_model_map <- c(NA, 1, 1, 0)
        names(force_seasonality_model_map) <- c(FORCE_SEASONALITY_AUTO, FORCE_SEASONALITY_ADDITIVE, FORCE_SEASONALITY_MULTIPLICATIVE, FORCE_SEASONALITY_NONE)

        arima_model <- auto.arima(historicalData, D = force_seasonality_model_map[forceSeasonality])
        fcast <- forecast(arima_model, h = forecastHorizon, level = c(confidenceLevel))

        mean_arima <- as.numeric(fcast$mean)
        sigma_arima <- as.numeric((fcast$upper - fcast$lower) / 2)
        arima_fitted <- as.numeric(fitted(arima_model))

        return(list(mean = mean_arima, sigma = sigma_arima, fitted = arima_fitted))

    }, error = function(err) {
        print(paste("ARIMA model error:  ", err))
        return(NULL)
    }))
}

modelEts <- function(historicalData, forecastHorizon, confidenceLevel, forceSeasonality) {
    return(tryCatch({
        # model = 3 characters: Z means automatically selected
        # 1st letter is error type, 2nd letter is trend type, 3rd letter is season type.
        force_seasonality_model_map <- c("ZZZ", "ZZA", "ZZM", "ZZN")
        names(force_seasonality_model_map) <- c(FORCE_SEASONALITY_AUTO, FORCE_SEASONALITY_ADDITIVE, FORCE_SEASONALITY_MULTIPLICATIVE, FORCE_SEASONALITY_NONE)

        ets_model <- ets(historicalData, model = force_seasonality_model_map[forceSeasonality])
        fcast <- forecast(ets_model, h = forecastHorizon, level = c(confidenceLevel))

        mean_ets <- as.numeric(fcast$mean)
        sigma_ets <- as.numeric((fcast$upper - fcast$lower) / 2)
        ets_fitted <- as.numeric(fitted(ets_model))

        return(list(mean = mean_ets, sigma = sigma_ets, fitted = ets_fitted))
    }, error = function(err) {
        print(paste("ETS model error:  ", err))
        return(NULL)
    }))
}

modelStl <- function(historicalData, forecastHorizon, confidenceLevel, seasonality) {
    if ((seasonality > 1) && (length(historicalData) > 2 * seasonality)) {
        tryCatch({
            fcast <- stlf(historicalData, h = forecastHorizon, level = c(confidenceLevel))

            mean_stl <- as.numeric(fcast$mean)
            sigma_stl <- as.numeric((fcast$upper - fcast$lower) / 2)
            stl_fitted <- as.numeric(fitted(fcast))

            return(list(mean = mean_stl, sigma = sigma_stl, fitted = stl_fitted))
        }, error = function(err) {
            print(paste("STL model error:  ", err))
            return(NULL)
        })
    }
    else {
        return(NULL)
    }
}

modelEtsArima <- function(ets_result, arima_result) {
    if (!is.null(ets_result) && !is.null(arima_result)) {
        mean_ets_arima <- (arima_result$mean + ets_result$mean) / 2.0;
        sigma_ets_arima <- sqrt(0.25 * ets_result$sigma * ets_result$sigma + 0.25 * arima_result$sigma * arima_result$sigma)
        ets_arima_fitted <- (arima_result$fitted + ets_result$fitted) / 2.0;

        return(list(mean = mean_ets_arima, sigma = sigma_ets_arima, fitted = ets_arima_fitted))
    }
    return(NULL)
}

modelEtsStl <- function(ets_result, stl_result) {
    if (!is.null(ets_result) && !is.null(stl_result)) {
        mean_ets_stl <- (stl_result$mean + ets_result$mean) / 2.0;
        sigma_ets_stl <- sqrt(0.25 * ets_result$sigma * ets_result$sigma + 0.25 * stl_result$sigma * stl_result$sigma)

        ets_stl_fitted <- (stl_result$fitted + ets_result$fitted) / 2.0;

        return(list(mean = mean_ets_stl, sigma = sigma_ets_stl, fitted = ets_stl_fitted))
    }
    return(NULL)
}

addToOutput <- function(vars_data_frame, granularity_attribute, date_key, transaction_qty, sigma, error_percentage, forecast_model_name) {
    new_data_frame <- data.frame(GRANULARITYATTRIBUTE = granularity_attribute, DATEKEY = date_key, TRANSACTIONQTY = transaction_qty, SIGMA = sigma, ERRORPERCENTAGE = as.numeric(0), FORECASTMODELNAME = "")

    # Put model parameters only in first line for same granularity attribute   
    new_data_frame$ERRORPERCENTAGE[1] = error_percentage[1]
    new_data_frame$FORECASTMODELNAME[1] = forecast_model_name[1]

    return(rbind(vars_data_frame, new_data_frame));
}

trainModels <- function(timeSeriesModel, historicalData, forecastHorizon, confidenceLevel, seasonality, forceSeasonality) {
    result = list()

    #ARIMA	
    if (timeSeriesModel == TIME_SERIES_MODEL_ARIMA || timeSeriesModel == TIME_SERIES_MODEL_ETS_ARIMA || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
        arimaForecast <- modelArima(historicalData, forecastHorizon, confidenceLevel, forceSeasonality)

        if (timeSeriesModel == TIME_SERIES_MODEL_ARIMA || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
            result[[TIME_SERIES_MODEL_ARIMA]] <- arimaForecast
        }
    }

    #ETS	
    if (timeSeriesModel == TIME_SERIES_MODEL_ETS || timeSeriesModel == TIME_SERIES_MODEL_ETS_ARIMA || timeSeriesModel == TIME_SERIES_MODEL_ETS_STL || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
        etsForecast <- modelEts(historicalData, forecastHorizon, confidenceLevel, forceSeasonality)

        if (timeSeriesModel == TIME_SERIES_MODEL_ETS || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
            result[[TIME_SERIES_MODEL_ETS]] <- etsForecast
        }
    }

    #STL
    if (timeSeriesModel == TIME_SERIES_MODEL_STL || timeSeriesModel == TIME_SERIES_MODEL_ETS_STL || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
        stlForecast <- modelStl(historicalData, forecastHorizon, confidenceLevel, seasonality)

        if (timeSeriesModel == TIME_SERIES_MODEL_STL || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
            result[[TIME_SERIES_MODEL_STL]] <- stlForecast
        }
    }

    #ETS + ARIMA
    if (timeSeriesModel == TIME_SERIES_MODEL_ETS_ARIMA || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
        result[[TIME_SERIES_MODEL_ETS_ARIMA]] <- modelEtsArima(etsForecast, arimaForecast)
    }

    #ETS + STL
    if (timeSeriesModel == TIME_SERIES_MODEL_ETS_STL || timeSeriesModel == TIME_SERIES_MODEL_ALL) {
        result[[TIME_SERIES_MODEL_ETS_STL]] <- modelEtsStl(etsForecast, stlForecast)
    }

    return(result);
}


entry_script = new.env()

# This method will be called on init in each worker process.
entry_script$init <- function() {
}

# This method will be called on each minibatch.
entry_script$run <- function(minibatch) {

    # Map 1-based optional input ports to variables
    datasetCombined <- minibatch # class: data.frame

    ################### PARAMETERS INIT ###################

    #Convert column names to upper
    for (i in 1:length(names(datasetCombined))) {
        colnames(datasetCombined)[i] <- toupper(colnames(datasetCombined)[i])
    }

    #First row should have parameter values
    datasetParameters <- head(datasetCombined, 1)

    data <- datasetCombined

    print(paste("Generating forecast for granularity attribute: ", data$GRANULARITYATTRIBUTE[1], ", length of dataset is: ", length(data$GRANULARITYATTRIBUTE)))

    #Number of forecast predictions
    horizon <- as.numeric(datasetParameters$HORIZON)

    if (is.na(horizon) || horizon <= 0) {
        stop(paste("Parameter HORIZON must be greater than 0."))
    }

    #Seasonality of historical data
    if (is.null(datasetParameters$SEASONALITY)) {
        seasonality <- 1;
    } else {
        seasonality <- as.numeric(datasetParameters$SEASONALITY);
    }

    if (is.na(seasonality) || seasonality <= 0) {
        stop(paste("Parameter SEASONALITY must be greater than 0."))
    }

    #Start date key from which forecast should be generated
    global_max_datekey <- max(data$DATEKEY)
    if (is.null(datasetParameters$FORECAST_START_DATEKEY)) {
        forecast_start_datekey <- global_max_datekey + 1;
    } else {
        forecast_start_datekey <- as.numeric(datasetParameters$FORECAST_START_DATEKEY);

        if (forecast_start_datekey <= global_max_datekey) {
            stop(paste("Parameter FORECAST_START_DATEKEY must be greater than maximum date key of historical data (", global_max_datekey, ")"))
        }
    }

    #Time series model
    if (is.null(datasetParameters$TIME_SERIES_MODEL)) {
        timeSeriesModel <- TIME_SERIES_MODEL_ALL;
    } else {
        timeSeriesModel <- toupper(datasetParameters$TIME_SERIES_MODEL);
    }

    if (is.na(match(timeSeriesModel, TIME_SERIES_MODELS))) {
        stop(paste("Parameter value of TIME_SERIES_MODEL does not represent any known forecasting model."))
    }

    #Confidence level
    if (is.null(datasetParameters$CONFIDENCE_LEVEL)) {
        confidenceLevel <- 95;
    } else {
        confidenceLevel <- as.numeric(datasetParameters$CONFIDENCE_LEVEL);
    }

    if (is.na(confidenceLevel) || confidenceLevel <= 0 || confidenceLevel >= 100) {
        stop(paste("Parameter CONFIDENCE_LEVEL must be greater than 0 and lower than 100."))
    }

    #Size of a test set in a percent of a total historical data size
    if (is.null(datasetParameters$TEST_SET_SIZE_PERCENT)) {
        testSetSizePercent <- 20;
    } else {
        testSetSizePercent <- as.numeric(datasetParameters$TEST_SET_SIZE_PERCENT);
    }

    if (is.na(testSetSizePercent) || testSetSizePercent < 0 || testSetSizePercent >= 100) {
        stop(paste("Parameter TEST_SET_SIZE_PERCENT must be greater than or equal to 0 and lower than 100."))
    }

    #How gaps in historical data are filled
    if (is.null(datasetParameters$MISSING_VALUE_SUBSTITUTION)) {
        missingValueSubstitution <- 0;
    } else {

        if (is.numeric(datasetParameters$MISSING_VALUE_SUBSTITUTION)) {
            missingValueSubstitution <- as.numeric(datasetParameters$MISSING_VALUE_SUBSTITUTION)
        }
        else {
            missingValueSubstitution <- toupper(datasetParameters$MISSING_VALUE_SUBSTITUTION)

            if (is.na(match(missingValueSubstitution, MISSING_VALUE_OPTIONS))) {
                stop(paste("Parameter value of MISSING_VALUE_SUBSTITUTION does not represent any known substitution option."))
            }
        }
    }

    #How are the historical data gaps filled?
    if (is.null(datasetParameters$MISSING_VALUE_SCOPE)) {
        missingValueScope <- MISSING_VALUE_SCOPE_GRANULARITY_ATTRIBUTE;
    } else {
        missingValueScope <- toupper(datasetParameters$MISSING_VALUE_SCOPE);
    }

    if (is.na(match(missingValueScope, MISSING_VALUE_SCOPE_OPTIONS))) {
        stop(paste("Parameter value of MISSING_VALUE_SCOPE does not represent any known option."))
    }

    #History date range period
    global_min_datekey <- min(data$DATEKEY)
    historyDateRangeFrom <- historyDateRangeTo <- NA
    if (missingValueScope == MISSING_VALUE_SCOPE_HISTORY_DATE_RANGE) {
        if (is.null(datasetParameters$HISTORY_DATE_RANGE_FROM)) {
            historyDateRangeFrom <- global_min_datekey
        } else {
            historyDateRangeFrom <- as.numeric(datasetParameters$HISTORY_DATE_RANGE_FROM)
        }
        if (is.null(datasetParameters$HISTORY_DATE_RANGE_TO)) {
            historyDateRangeTo <- global_max_datekey
        } else {
            historyDateRangeTo <- as.numeric(datasetParameters$HISTORY_DATE_RANGE_TO)
        }

        if (historyDateRangeTo < historyDateRangeFrom) {
            stop(paste("Parameter HISTORY_DATE_RANGE_TO must be greater than parameter HISTORY_DATE_RANGE_FROM"))
        }
    }

    #Seasonality
    if (is.null(datasetParameters$FORCE_SEASONALITY)) {
        forceSeasonality <- FORCE_SEASONALITY_AUTO;
    } else {
        forceSeasonality <- toupper(datasetParameters$FORCE_SEASONALITY);
    }

    if (is.na(match(forceSeasonality, FORCE_SEASONALITY_OPTIONS))) {
        stop(paste("Parameter value of FORCE_SEASONALITY does not represent any known seasonality option."))
    }


    ################### END PARAMETERS INIT ###################

    output <- data.frame(
        GRANULARITYATTRIBUTE = character(0),
        DATEKEY = numeric(0),
        TRANSACTIONQTY = numeric(0),
        SIGMA = numeric(0),
        ERRORPERCENTAGE = numeric(0),
        FORECASTMODELNAME = character(0))

    granularityAttributes <- unique(data$GRANULARITYATTRIBUTE);
    granularityAttributes_num <- length(granularityAttributes)


    min_datekey <- max_datekey <- NA
    #Get minimum and maximum date keys
    if (missingValueScope == MISSING_VALUE_SCOPE_GLOBAL) {
        max_datekey <- max(data$DATEKEY)
        min_datekey <- min(data$DATEKEY)
    } else if (missingValueScope == MISSING_VALUE_SCOPE_HISTORY_DATE_RANGE) {
        max_datekey <- historyDateRangeFrom
        min_datekey <- historyDateRangeTo
    }


    for (i in 1:granularityAttributes_num) {

        granularityAttribute_data <- data[which(data$GRANULARITYATTRIBUTE == granularityAttributes[i]),]

        #Prepare training and test data for given granularity attribute
        granularityAttribute_data <- order_and_fill_missing_values(granularityAttribute_data, missingValueSubstitution, min_datekey, max_datekey)

        full_data <- as.numeric(granularityAttribute_data$TRANSACTIONQTY)

        full_data_ts <- ts(full_data, frequency = seasonality)
        full_data_last_datekey <- granularityAttribute_data$DATEKEY[length(granularityAttribute_data$DATEKEY)];
        full_data_horizon_offset <- forecast_start_datekey - full_data_last_datekey + horizon - 1;

        test_size <- floor(length(granularityAttribute_data$TRANSACTIONQTY) * testSetSizePercent / 100);
        resultTimeSeriesModel <- timeSeriesModel

        if (test_size > 0) {
            #When there is data in the test set then the model will be chosen based on how well it predicts the demand in the test set

            test_data <- tail(as.numeric(granularityAttribute_data$TRANSACTIONQTY), n = test_size);
            train_data <- head(granularityAttribute_data$TRANSACTIONQTY, n = -test_size)

            train_data_ts <- ts(train_data, frequency = seasonality);

            result <- trainModels(timeSeriesModel, train_data_ts, forecastHorizon = test_size, confidenceLevel = confidenceLevel, seasonality, forceSeasonality)

            #Calculate accuracy metrics

            #Mape cannot be calculated when there are zeros in the data
            test_data_na <- replace(test_data, test_data == 0, NaN)

            result_mape <- Inf

            for (name in names(result)) {
                #How big is the error between forecasted demand and actual demand
                current_mape <- mape(test_data_na, result[[name]]$mean)

                if (current_mape < result_mape) {
                    resultTimeSeriesModel <- toupper(name)
                    result_mape <- current_mape
                }
            }

            #if for some reason there is still Inf, replace it with -1 that means mape can not be calculated
            result_mape[is.infinite(result_mape)] <- -1

            result <- trainModels(resultTimeSeriesModel, full_data_ts, full_data_horizon_offset, confidenceLevel, seasonality, forceSeasonality)
        }
        else {
            #There is no data in the test set so the model will be chosen based on how well it fits historical data
            result <- trainModels(timeSeriesModel, full_data_ts, full_data_horizon_offset, confidenceLevel, seasonality, forceSeasonality)

            #Mape cannot be calculated when there are zeros in the data
            full_data_na <- replace(full_data, full_data == 0, NaN)

            #MAPE cannot be calculated when the test set is empty
            result_mape <- -1
            min_mape_fitted <- Inf

            for (name in names(result)) {
                #How well the model fits historical data
                current_mape_fitted <- mape(full_data_na, result[[name]]$fitted)

                if (current_mape_fitted < min_mape_fitted) {
                    resultTimeSeriesModel <- toupper(name)
                    min_mape_fitted <- current_mape_fitted
                }
            }
        }

        #Save result to output
        mean_aml <- c()
        sigma_aml <- c()
        mape_aml <- c()

        if (!is.null(result[[resultTimeSeriesModel]])) {
            mean_aml <- result[[resultTimeSeriesModel]]$mean
            sigma_aml <- result[[resultTimeSeriesModel]]$sigma
            mape_aml <- result_mape
        }

        if (length(mean_aml) == 0) {
            paste("Forecast cannot be generated for ", granularityAttributes[i])

            output = addToOutput(
                output,
                granularityAttributes[i],
                -1,
                -1,
                -1,
                -1,
                "")
        }
        else {
            sigma_aml[is.infinite(sigma_aml)] <- MAX_DECIMAL

            #Forecasted values will be sent starting from forecast start date key parameter value
            output = addToOutput(
                output,
                granularityAttributes[i],
                seq(from = forecast_start_datekey, length.out = horizon),
                tail(as.numeric(mean_aml), horizon),
                tail(as.numeric(sigma_aml), horizon),
                mape_aml,
                resultTimeSeriesModel)
        }
    }

    # Each parallel run step minibatch processing returns an array of string.
    # Lines from all the minibatches are combined into 1 output file.
    result <- paste(
        paste('"', output$GRANULARITYATTRIBUTE, '"', sep = ""),
        output$DATEKEY,
        output$TRANSACTIONQTY,
        output$SIGMA,
        output$ERRORPERCENTAGE,
        output$FORECASTMODELNAME,
        sep = ",")
}