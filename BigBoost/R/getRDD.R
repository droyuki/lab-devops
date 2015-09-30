#!/usr/bin/env Rscript
stime <- Sys.time()
stimefmt <- format(stime, "%Y_%m_%d_%H%M")
setwd("/opt/BigBoost/R/")
source("getRDD_helper.R")
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
newdata <- getStreamingLines(input)
workerName <- system("uname -n", intern=TRUE)
cat("Rscript run on ",workerName,"\n")
cat(newdata)