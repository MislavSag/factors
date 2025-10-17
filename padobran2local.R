library(data.table)
library(rococo)


# Config
PATH = "/home/sn/data/equity/us/factors"
if (!dir.exists(PATH)) dir.create(PATH)

# Download data from padobran
# scp -r padobran:/home/jmaric/factors/daily/ /home/sn/data/equity/us/factors
# rsync -avz --ignore-existing padobran:/home/jmaric/factors/daily/ /home/sn/data/equity/us/factors

# SP100 Symbols for test
if (interactive()) {
  path_ = "/home/sn/lean/data/equity/usa/universes/etf/splg"
  if (!dir.exists(path_)) stop("Path does not exist")
  files = list.files(path_, full.names = TRUE)
  etf = lapply(files, fread)
  etf = rbindlist(etf)
  setnames(etf, c("symbol", "id", "date", "weight", "n", "mcap"))
  symbols = etf[, unique(symbol)]
}

# File to import
files = list.files(file.path(PATH, "daily"), full.names = TRUE)
tickers = tools::file_path_sans_ext(basename(files))
symbols_import = intersect(tolower(symbols), tickers)
symbols_import = file.path(PATH, "daily", paste0(symbols_import, ".csv"))

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

# Remove columns with many NA values
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
symbols_by_month = dt[, .N, by = month][order(month)]
plot(symbols_by_month)
symbols_by_month[N > 10]
dt[, rococo(returns_10, target), by = month] |>
  _[order(month)]

# Calculate gamma for all factors
predictors = setdiff(
  dt[, names(.SD), .SDcols = !is.character], 
  c("symbol", "date", "month", "sid", "industry", "sector", "target", "open", 
    "low", "high", "close", "close_raw", "volume")
)
dt_long = melt(dt[month > 2016], id.vars = c("month", "target"), measure.vars = predictors)
dt_long = na.omit(dt_long)
dt_long[, .N, by = .(month, variable)]
dt_long[, any(is.na(value))]
dt_long[, any(is.infinite(value))]
dt_long = dt_long[is.finite(value)]
# variables = dt_long[, unique(variable)]
# rococo_l = list()
# for (i in seq_along(variables)) {
#   print(i)
#   var_ = variables[i]
#   dt_long[variable == var_]
#   rococo_l[[i]] = dt_long[variable == var_] |>
#     _[, .(variable = var_, gamma = rococo(value, target)), by = .(month)]
# }
dt_roco = dt_long[, .(gamma = rococo(value, target)), by = .(month, variable)]
dt_roco[gamma > 0.75]
dt_roco[gamma < -0.75]

# Mean gamma rank across all months
cs_gamma = dt_roco[, .(gamma = mean(gamma, na.rm = TRUE)), by = variable]
setorder(cs_gamma, -gamma)
cs_gamma
