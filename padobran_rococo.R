library(data.table)
library(rococo)


# Config
if (interactive()) {
  PATH = "/home/sn/data/equity/us/factors"  
} else {
  PATH = "/home/jmaric/factors"
}

# File to import
files = list.files(file.path(PATH, "daily"), full.names = TRUE)  
tickers = tools::file_path_sans_ext(basename(files))

# Test for interactove
if (interactive()) {
  files = symbols_import[1:5]; tickers = tickers[1:5]
}

# Import sample data in monthly freq
dt = lapply(symbols_import, function(x) {
  # x = symbols_import[1]
  dt_ = fread(x)
  # dt_[, .(symbol, date, open, low, high, close, volume, returns)]
  dt_[, month := data.table::yearmon(date)]
  dt_[, let(returns = NULL)]
  dt_id = dt_[, .(
    open  = data.table::first(open),
    low = min(low, na.rm = TRUE),
    high = max(high, na.rm = TRUE),
    close = data.table::last(close),
    close_raw = data.table::last(close_raw),
    volume = sum(volume, na.rm = TRUE),
    industry = data.table::last(industry),
    sector = data.table::last(sector),
    sid = data.table::last(sid),
    date  = data.table::last(date)
  ), by = .(symbol, month)]
  cols = setdiff(colnames(dt_), colnames(dt_id))
  dt_factors = dt_[, data.table::last(.SD), by = .(symbol, month), .SDcols = cols]
  # dt_factors[, 1:10]
  dt_ = dt_factors[dt_id, on = c("symbol", "month")]
  setorderv(dt_, c("symbol", "date"))
  return(dt_)
})
dt = rbindlist(dt, fill = TRUE)
dt[, target := close / open -1]
dt[, target := shift(target, 1, type = "lead"), by = symbol]

`# Remove columns with many NA values
ncol(dt)
remove_cols = dt[, vapply(.SD, function(x) (sum(is.na(x)) / .N) > 0.5, logical(1L))]
remove_cols = names(remove_cols[remove_cols == TRUE])
dt = dt[, .SD, .SDcols = -remove_cols]
ncol(dt)

# Remove NA values
nrow(dt)
dt = na.omit(dt)
nrow(dt)

# Checks
