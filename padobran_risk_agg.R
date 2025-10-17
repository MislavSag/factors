library(data.table)
library(rococo)


# Config
PATH = "/home/sn/data/equity/us/factors"
if (!dir.exists(PATH)) dir.create(PATH)

# Download data from padobran
# scp -r padobran:/home/jmaric/factors/daily/ /home/sn/data/equity/us/factors
# rsync -avz --ignore-existing padobran:/home/jmaric/factors/daily/ /home/sn/data/equity/us/factors

# Import all predictors for one symbols
predictors = fread(file.path(PATH, "daily", "aapl.csv"))
dput(tail(predictors, 1))
