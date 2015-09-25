## define helper functions for SmartExpertRule1
suppressPackageStartupMessages({
    library(Rcpp)
    library(compiler)
})

getCommandLineArg <- function() {
    cmdargs <- commandArgs()
    scriptname <- gsub("--file=", '', grep("--file=", cmdargs, value=TRUE))
    if ( !"--args" %in% cmdargs ) {
        message(sprintf("Usage: %s [1|2]", scriptname))
        quit(status=1)
    }
    args <- cmdargs[-(1:which(cmdargs == "--args"))]
    if ( !args[1] %in% as.character(1:2) )
        stop("Command line arg not in pre-defined set.")
    names(args) <- "data_typeid"
    args
}; getCommandLineArg <- cmpfun(getCommandLineArg, options=list(suppressUndefined=TRUE))

getStreamingLines <- function(input="stdin", cols) {
    require(data.table)
    f <- file(input)
    open(f)
    on.exit(close(f))
    ncol <- length(cols)
    res <- list()
    for ( i in 1:ncol )
        res[[i]] <- character()
    while( length(line <- readLines(f, n=1)) > 0 ) {
        lineparsed <- unlist(strsplit(line, ','))
        for ( i in 1:ncol )
            res[[i]] <- c(res[[i]], lineparsed[i])   
    }
    res <- as.data.table(res)
    setnames(res, cols)
    res
}; getStreamingLines <- cmpfun(getStreamingLines, options=list(suppressUndefined=TRUE))
