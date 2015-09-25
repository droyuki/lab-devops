#!/usr/bin/env Rscript
stime <- Sys.time()
stimefmt <- format(stime, "%Y_%m_%d_%H%M")
setwd("./")
source("ser_helpers.R")

## get command line argument(s)
cmdargs <-  getCommandLineArg()
if ( cmdargs["data_typeid"] == '1' ) {
    sec <- "ser1"
} else if ( cmdargs["data_typeid"] == '2' ) {
    sec <- "ser2"
}

## read config
conf_fname <- "conf.ini"
if ( file.exists(conf_fname) ) {
    config <- readIniConfig(conf_fname)
} else {
    message('Configure file not found. Program aborted.')
    quit(status=1)
}
log_path <- config[[sec]]["log_path"]


## start logging
logfile <- file(file.path(log_path,  "stderr.log"), open='a')
sink(logfile, append=TRUE, type='message')

message('')
message("Job starting time: ", stime)
message("Rscript run on ", system("uname -n", intern=TRUE))
message('Config file read as:')
write.table(as.data.frame(config$ser1), quote=FALSE, sep='\t', col.names=FALSE, file=stderr())

## load libraries
suppressPackageStartupMessages({
    library(methods)
    library(Matrix)
    library(parallel)
    library(data.table)
    library(matrixStats)
    library(pscl)
})


## get streaming lines
input <- "stdin"
newdata <- getStreamingLines(input, c("ts", "acct", "cnt"))
newdata <- newdata[, `:=`(ts=as.integer(ts),
                          cnt=as.integer(cnt))]
newdata <- newdata[!grepl("\\$$", acct)] # restrict to non-computer accounts
message("New data loaded. Number of lines: ", nl <- nrow(newdata))

if ( nl == 0 ) {
    message("No new coming data.")
    quit(status=0)    ### early exit due to no new coming data
}
