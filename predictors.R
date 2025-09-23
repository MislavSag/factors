library(data.table)
library(finfeatures)


# paths
if (interactive()) {
  PATH_PRICES     = file.path("D:/predictors/prices")
  PATH_PREDICTORS = file.path("D:/predictors/daily")
} else {
  PATH_PRICES = file.path("prices")
  PATH_PREDICTORS = file.path("daily")
}

# Create directory if it doesnt exists
if (!dir.exists(PATH_PREDICTORS)) {
  dir.create(PATH_PREDICTORS)
}

# Get index
if (interactive()) {
  i = 1L
} else {
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
}

# Get symbol
symbols = gsub("\\.csv", "", list.files(PATH_PRICES))
symbol_i = symbols[i]

# Import Ohlcv data
ohlcv = fread(file.path(PATH_PRICES, paste0(symbol_i, ".csv")))
head(ohlcv, 20)
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume)],
                  date_col = "date")


# ROLLING PREDICTORS ------------------------------------------------------
# Exuber
path_ =   create_path("exuber")
windows_ = c(windows, 504)
if (max(at) > min(windows)) {
  exuber_init = RollingExuber$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = lag_,
    exuber_lag = 1L
  )
  exuber = exuber_init$get_rolling_features(ohlcv, TRUE)
}

# Backcusum
path_ = create_path("backcusum")
if (max(at) > min(windows)) {
  backcusum_init = RollingBackcusum$new(
    windows = windows_,
    workers = workers,
    at = at,
    lag = lag_,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2))
  backcusum = backcusum_init$get_rolling_features(ohlcv)
}

# Theft r
path_ = create_path("theftr")
if (max(at) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = lag_,
    features_set = c("catch22", "feasts"))
  theft_r = theft_init$get_rolling_features(ohlcv)
  fwrite(theft_r, path_)
}

# Theft py
path_ = create_path("theftpy")
if (max(at) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows,
    workers = 1L,
    at = at,
    lag = lag_,
    features_set = c("tsfel", "tsfresh"))
  theft_py = suppressMessages(theft_init$get_rolling_features(ohlcv))
  fwrite(theft_py, path_)
}

# Forecasts
path_ = create_path("forecasts")
if (max(at) > min(windows)) {
  forecasts_init = RollingForecats$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = lag_,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22)
  forecasts = suppressMessages(forecasts_init$get_rolling_features(ohlcv))
  fwrite(forecasts, path_)
}

# Tsfeatures
path_ = create_path("tsfeatures")
if (max(at) > min(windows)) {
  tsfeatures_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = lag_,
    scale = TRUE)
  tsfeatures = suppressMessages(tsfeatures_init$get_rolling_features(ohlcv))
  fwrite(tsfeatures, path_)
}

# WaveletArima
path_ = create_path("waveletarima")
if (max(at) > min(windows)) {
  waveletarima_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = lag_,
    filter = "haar")
  waveletarima = suppressMessages(waveletarima_init$get_rolling_features(ohlcv))
  fwrite(waveletarima, path_)
}

# FracDiff
path_ = create_path("fracdiff")
if (max(at) > min(windows)) {
  fracdiff_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = lag_,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9))
  fracdiff = suppressMessages(fracdiff_init$get_rolling_features(ohlcv))
  fwrite(fracdiff, path_)
}

# Theft r with returns
path_ = create_path("theftr")
if (max(at) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows,
    workers = workers,
    at = at,
    lag = lag_,
    features_set = c("catch22", "feasts"))
  theft_r = theft_init$get_rolling_features(ohlcv, price_col = "returns")
  fwrite(theft_r, path_)
}

# Theft py with returns
path_ = create_path("theftpy")
if (max(at) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows,
    workers = 1L,
    at = at,
    lag = lag_,
    features_set = c("tsfel", "tsfresh"))
  theft_py = suppressMessages(theft_init$get_rolling_features(ohlcv, price_col = "returns"))
  fwrite(theft_py, path_)
}

# Combine all predictors
Reduce(function(x, y) merge(x, y, by = "date"),
       list(ohlcv, exuber, backcusum))

