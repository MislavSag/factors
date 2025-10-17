library(data.table)
library(janitor)


# paths
if (interactive()) {
  PATH_DAILY   = file.path("D:/strategies/factors/daily")
  PATH_MONTHLY = file.path("D:/strategies/factors/monthly")
} else {
  PATH_DAILY   = file.path("/home/jmaric/factors/daily")
  PATH_MONTHLY = file.path("/home/jmaric/factors/MONTHLY")
}

# Create directory if it doesnt exists
if (!dir.exists(PATH_MONTHLY)) dir.create(PATH_MONTHLY)

# Get index
if (interactive()) {
  i = 1L
} else {
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
}

# Get symbol
files = list.files(PATH_DAILY, full.names = TRUE)
filei = files[i]

# If files already exists cont
filem = file.path(PATH_MONTHLY, basename(filei))
if (!file.exists(filem)) {
  cat(sprintf("Processing: %s\n", filem))
} else {
  cat(sprintf("File already exists: %s\n", filem))
  quit(save = "no", status = 0)
}

# Import data and upsample to monthly data
dt = fread(filei)
dt[, month := data.table::yearmon(date)]
dt[, let(returns = NULL)]
dt_id = dt[, .(
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
cols = setdiff(colnames(dt), colnames(dt_id))
dt[, names(.SD) := lapply(.SD, as.numeric), .SDcols = cols]
dt_factors_last = dt[, data.table::last(.SD), by = .(symbol, month), .SDcols = cols]
dt_factors_mean = dt[, lapply(.SD, mean, na.rm = TRUE), by = .(symbol, month), .SDcols = cols]
setnames(dt_factors_mean, cols, paste0(cols, "_mean"))
# dt_factors_last[, .SD, .SDcols = c(1:5, (ncol(dt_factors_last)-5):ncol(dt_factors_last))][]
# dt_factors_mean[, .SD, .SDcols = c(1:5, (ncol(dt_factors_mean)-5):ncol(dt_factors_mean))][]
dt = dt_factors_last[dt_id, on = c("symbol", "month")]
dt = dt_factors_mean[dt, on = c("symbol", "month")]
setorder(dt, date)

# Remove columns with all Na observations
dt = remove_empty(dt, which = "rows", cutoff = 0.001, quiet = FALSE)
dt = remove_empty(dt, which = "cols", cutoff = 0.001, quiet = FALSE)

# Checks
if (interactive()) {
  dt[, .SD, .SDcols = c(1:9, (ncol(dt)-9):ncol(dt))][]  
  dt[, .(symbol, date, month, returns_1, returns_1_mean)]
}

# Save
fwrite(dt, filem)
