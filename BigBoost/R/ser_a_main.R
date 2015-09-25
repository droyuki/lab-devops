#!/usr/bin/env Rscript
# smart expert rule A (anomaly detection on count data with excess zeroes) - streaming version


stime <- Sys.time()
stimefmt <- format(stime, "%Y_%m_%d_%H%M")


## set working directory (default at /opt/spark/work/app-* for RDDPipe)
setwd("./")


## source helper functions
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
wsize <- as.integer(config[[sec]]["wsize"])
tsize <- as.integer(config[[sec]]["tsize"])
ncoef <- as.integer(config[[sec]]["ncoef"])
ncore <- as.integer(config[[sec]]["ncore"])
hcount_fname <- config[[sec]]["hcount_fname"]
hcoef_fname <- config[[sec]]["hcoef_fname"]
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


## do sanity check
# each account should only have one count per streaming job
if ( nrow(newdata) != uniqueN(newdata$acct) ) {
    warning(call.=FALSE, 
            "At least one account has multiple counts. Take only the count with latest timestamp.")
    setorder(newdata, acct, -ts)
    newdata <- newdata[, .SD[1], by="acct"]
}


## load/build historical counts
if ( !file.exists(hcount_fname) ) {
    message("No historical file found. Do cold-start.")
    train_data_matrix <- toSparseMovingCount(newdata, wsize, ncore)
    save("train_data_matrix", file=hcount_fname)
    message("Historical counts file created and saved.")
    quit(status=0)    ### exit on ground zero: first counts saved
} else {
    message("Historical counts file checked:")
    message('\t', hcount_fname)
    load(hcount_fname)
    message("Historical counts file loaded.")
    
    ## update count history
    # for existing account(s)
    newdata <- newdata[, new_acct:=!(acct %in% colnames(train_data_matrix))]
    newdata_oldAcct <- newdata[new_acct == FALSE]
    newdata_matrix_oldAcct <- matrix(0, 1, ncol(train_data_matrix), 
                                     dimnames=list(NULL, colnames(train_data_matrix)))
    newdata_matrix_oldAcct[,newdata[new_acct == FALSE, acct]] <- newdata[new_acct == FALSE, cnt]
    train_data_matrix <- rbind2(train_data_matrix, newdata_matrix_oldAcct)
    # for new-coming account(s)
    newdata_newAcct <- newdata[new_acct == TRUE]
    newdata_matrix_newAcct <- matrix(NA_real_, nrow(train_data_matrix), nrow(newdata_newAcct), 
                                     dimnames=list(NULL, newdata_newAcct$acct))
    newdata_matrix_newAcct[nrow(newdata_matrix_newAcct),newdata_newAcct$acct] <- newdata_newAcct$cnt
    train_data_matrix <- cbind2(train_data_matrix, newdata_matrix_newAcct)

    # purge counts
    if ( nrow(train_data_matrix) > tsize + ncoef ) {
        train_data_matrix <- tail(train_data_matrix, tsize + ncoef)
        rownames(train_data_matrix) <- NULL
    }
    save("train_data_matrix", file=hcount_fname)
    message("Historical counts file updated and saved.")
}


## update coefficient history
# only for account with new count different with the corresponding earliest count
# new accounts will be modeled once the earliest count is not NA and is different from the latest count

if ( !file.exists(hcoef_fname) ) {
    
    if ( (nr <- nrow(train_data_matrix)) < tsize ) {
        message("Accumulating data. Algorithm not run.")
        message("Number of rows in training set: ", nr)
        quit(status=0)    ### exit on cold-start period: data not enough for learning
    }
    
    qualified_acct <- selectQualifiedAccount(train_data_matrix, 4)
    if ( !length(qualified_acct) ) {
        message("Data accumulated, but no account qualified for ZIP modeling.")
        quit(status=0)    ### exit on cold-start period: no qualified account for ZIP modeling
    }

    message("Algorithm first run!")
    
    # run ZIP model (first run)
    stime <- proc.time()
    new_coefs <- runZIPPar(train_data_matrix, qualified_acct, ncore)
    etime <- proc.time()
    message("Time elapsed for fitting ZIP model: ", (etime - stime)["elapsed"])
    
    historical_coefs <- new_coefs["count",,drop=FALSE]
    rownames(historical_coefs) <- NULL
    save("historical_coefs", file=hcoef_fname)
    message("Historical coefs file saved.")
    quit(status=0)    ### exit on cold-start period: data not enough for detection

} else {
    
    # update historical coefs
    load(hcoef_fname)
    new_coefs_container_oldAcct <- tail(historical_coefs, 1)

    new_counts_differ <- as.logical(head(train_data_matrix, 1) != tail(train_data_matrix, 1))
    acct_need_update <- colnames(train_data_matrix)[new_counts_differ]
    acct_need_update <- acct_need_update[!is.na(acct_need_update)]

    qualified_acct <- selectQualifiedAccount(train_data_matrix, 4)
    acct_need_update <- intersect(acct_need_update, qualified_acct)
    if ( !length(acct_need_update) ) {
        message("Data accumulated, but no account qualified for ZIP modeling.")
        quit(status=0)    ### exit on cold-start period: no qualified account for ZIP modeling
    }

    acct_need_update_oldAcct <- acct_need_update[acct_need_update %in% colnames(historical_coefs)]
    acct_need_update_newAcct <- acct_need_update[!acct_need_update %in% colnames(historical_coefs)]
    
    message("Number of old accounts requiring coefficent update: ", length(acct_need_update_oldAcct))
    message("Number of new accounts requiring coefficent update: ", length(acct_need_update_newAcct))
    
    stime <- proc.time()
    # new_coefs <- runZIPPar(train_data_matrix, acct_need_update, ncore)["count",,drop=FALSE]
    new_coefs <- apply(train_data_matrix[,acct_need_update,drop=FALSE], 2, runZIP)["count",,drop=FALSE]
    etime <- proc.time()
    message("Time elapsed for fitting ZIP model: ", (etime - stime)["elapsed"])
    
    new_coefs_container_oldAcct[,acct_need_update_oldAcct] <- new_coefs[,acct_need_update_oldAcct]
    historical_coefs <- rbind2(historical_coefs, new_coefs_container_oldAcct)
    new_coefs_container_newAcct <- matrix(NA_real_, nrow(historical_coefs), length(acct_need_update_newAcct), 
                                          dimnames=list(NULL, acct_need_update_newAcct))
    new_coefs_container_newAcct[nrow(new_coefs_container_newAcct),acct_need_update_newAcct] <- new_coefs[,acct_need_update_newAcct]
    historical_coefs <- cbind2(historical_coefs, new_coefs_container_newAcct)
    rownames(historical_coefs) <- NULL

    if ( nrow(historical_coefs) < ncoef ) {
        message("Accumulating coefs. Not yet for detection.")
        message("Number of coefs accumulated: ", nrow(historical_coefs))
        save("historical_coefs", file=hcoef_fname)
        quit(status=0)    ### exit on cold-start period: data not enough for detection
    }
    
    # detect anomaly
    message("Run zero-inflated model.")
    hasNA <- apply(historical_coefs, 2, function(x) sum(is.na(x)) /length(x))
    acct_qualified_for_detect <- colnames(historical_coefs)[hasNA <= .1]    
    cnst_med <- colMedians(historical_coefs[1:ncoef,acct_qualified_for_detect], na.rm=TRUE)
    cnst_iqr <- colIQRs(historical_coefs[,acct_qualified_for_detect], na.rm=TRUE)
    cnst <- (tail(historical_coefs[,acct_qualified_for_detect], 1) - cnst_med) / cnst_iqr
    cnst <- cnst[1,] # drop the second dimension
    detected_acct <- names(cnst[is.finite(cnst) & cnst > 5])
    
    detected_acct_counts <- train_data_matrix[,detected_acct,drop=FALSE]
    detected_acct_lastCounts <- tail(detected_acct_counts, 1)
    historical_high <- apply(detected_acct_counts[1:ncoef,,drop=FALSE], 2, max)
    detected_acct <- detected_acct[as.logical(detected_acct_lastCounts > historical_high + 2)]

    if ( length(detected_acct) ) {
        message("Anomaly detected by zero-inflated model:")
        message(paste(paste(detected_acct, 
                            detected_acct_lastCounts[,detected_acct], sep=": "), collapsed='\n'))
        saveCounts(train_data_matrix[,detected_acct,drop=FALSE], file.path(log_path, sprintf("detected_datamatrix_zin_%s.RData", stimefmt)))
        write(paste(detected_acct, "zero-inflated", sep=','), stdout())
    } else {
        message("No anomaly detected by zero-inflated model.")
    }

    # do complementary zero-excluded detection
    message("Run zero-excluded model.")
    detected_acct_zeroExcluded <- apply(train_data_matrix[,newdata$acct,drop=FALSE], 2, runZeroExcludedModel)
    detected_acct_zeroExcluded <- names(detected_acct_zeroExcluded)[detected_acct_zeroExcluded]
    if ( length(detected_acct_zeroExcluded) ) {
        message(paste(paste(detected_acct_zeroExcluded, 
                            tail(train_data_matrix[,detected_acct_zeroExcluded], 1), sep=": "), collapsed='\n'))
        saveCounts(train_data_matrix[,detected_acct_zeroExcluded,drop=FALSE], file.path(log_path, sprintf("detected_datamatrix_zex_%s.RData", stimefmt)))
        write(paste(detected_acct_zeroExcluded, "zero-excluded", sep=','), stdout())
    } else {
        message("No anomaly detected by zero-excluded model.")
    }

    # purge coefs
    if ( nrow(historical_coefs) > ncoef ) {
        historical_coefs <- tail(historical_coefs, ncoef)
        rownames(historical_coefs) <- NULL
    }
    save("historical_coefs", file=hcoef_fname)
    message("Historical coefs file updated and saved.")
    
}



