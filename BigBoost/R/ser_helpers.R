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

readIniConfig <- function(fname) {
    if ( !file.exists(fname) )
        stop("File not found.")
    conf <- read.table(fname, sep='=', fill=TRUE, header=FALSE, strip.white=TRUE,
                       col.names=c("key", "val"), stringsAsFactors=FALSE,
                       comment.char="#")
    section_pos <- grepl("^\\[.*\\]$", conf$key)
    section_name <- gsub("\\[|\\]", '', conf$key[section_pos])
    conf <- split(conf, cumsum(section_pos))
    names(conf) <- section_name
    lapply(conf, function(x) setNames(x$val, x$key)[-1])
}; readIniConfig <- cmpfun(readIniConfig, options=list(suppressUndefined=TRUE))

loadBunch <- function(fnames, ACCT=NULL) {
    # not used in the streaming version
    require(data.table)
    require(parallel)
    fnames <- file.path(fnames, "part-00000")
    fnames <- fnames[file.info(fnames)$size != 0]
    DT <- rbindlist(mclapply(fnames, fread, sep=',', header=FALSE, mc.cores=detectCores()))
    setnames(DT, c("ts", "acct", "cnt"))
    if ( !is.null(ACCT) )
        DT <- DT[acct %in% ACCT]
    DT
}; loadBunch <- cmpfun(loadBunch, options=list(suppressUndefined=TRUE))

loadBunchRaw <- function(fnames, ACCT=NULL) {
    # not used in the streaming version
    require(data.table)
    require(parallel)
    fnames <- file.path(fnames, "part-00000")
    DT <- rbindlist(mclapply(fnames, fread, sep=',', header=FALSE, mc.cores=detectCores()))
    setnames(DT, c("ts", "eventcode", "dc", "acct", "ip", "retcode"))
    if ( !is.null(ACCT) )
        DT <- DT[acct %in% ACCT]
    DT[, ts:=as.integer(gsub('\\[', '', ts))]
    DT[, retcode:=gsub('\\]', '', retcode)]
    DT
}; loadBunchRaw <- cmpfun(loadBunchRaw, options=list(suppressUndefined=TRUE))

cppFunction('NumericVector movingCount_
            (NumericVector tindex, NumericVector dataT, NumericVector dataC, int stepsize) {
                int n = tindex.size();
                NumericVector out(n);
                for (int i = 0; i < n; i++) {
                    int begin = tindex[i];
                    int end = begin + stepsize;
                    NumericVector cnts = dataC[(dataT >= begin) & (dataT < end)];
                    out(i) = sum(cnts);
                }
            return out;
            }')

movingCount <- function(DT, tindex, stepsize=1) {
    if ( !inherits(DT, "data.frame") )
        stop("DT must inherits data.frame.")
    if ( sum(!c("tunit", "cnt") %in% names(DT)) )
        stop("Column \"tunit\" and/or \"cnt\" not found in the given input data.")
    movingCount_(tindex=tindex, dataT=DT$tunit, dataC=DT$cnt, stepsize=stepsize)
}; movingCount <- cmpfun(movingCount, options=list(suppressUndefined=TRUE))

toSparseMovingCount <- function(DT, wsize, ncore=detectCores(), ACCT=NULL) {
    require(parallel)
    require(Matrix)
    DTcopy <- copy(DT)
    if ( !is.null(ACCT) )
        DTcopy <- DTcopy[acct %in% ACCT]
    DTcopy[, tunit:=as.integer(ts / wsize)]
    all_tunits <- min(DTcopy$tunit):max(DTcopy$tunit)
    M <- mclapply(split(DTcopy, DTcopy$acct), movingCount, tindex=all_tunits, mc.cores=ncore)
    Matrix(do.call(cbind, M), sparse=TRUE)
}; toSparseMovingCount <- cmpfun(toSparseMovingCount, options=list(suppressUndefined=TRUE))

selectQualifiedAccount <- function(mat, min_uniq=3) {
    require(data.table)
    if ( !is.numeric(min_uniq) )
        stop("Argument min_uniq must be numeric.")
    unique_cnt <- apply(mat, 2, uniqueN)
    na_cnt <- apply(mat, 2, function(x) sum(is.na(x)))
    colnames(mat)[unique_cnt >= min_uniq & na_cnt == 0]
}; selectQualifiedAccount <- cmpfun(selectQualifiedAccount, options=list(suppressUndefined=TRUE))

runZIP <- function(x) {
    require(pscl)
    if ( !is.numeric(x) )
        stop("Argument x must be numeric.")
    if ( !is.null(dim(x)))
        stop("Argument x should not have attribute dimension.")
    zipcoef <- 
        tryCatch({
            zipfit <- zeroinfl(x ~ 1, dist="poisson")
            zipfit <- unlist(zipfit$coef)
            names(zipfit) <- c("count", "zero")
            zipfit
        }, error=function(cond) {
            c(count=NA_real_, zero=NA_real_)
        })
    zipcoef
}; runZIP <- cmpfun(runZIP, options=list(suppressUndefined=TRUE))

runZIPPar <- function(mat, acct, nth, type="psock") {
    require(parallel)
    require(Matrix)
    if ( type == "fork" ) {
        cl <- makeForkCluster(nth)
    } else if ( type == "psock" ) {
        cl <- makePSOCKcluster(nth)
    } else {
        stop("Argument type must be one of \"fork\" or \"psock\".")
    }
    on.exit(stopCluster(cl))
    parApply(cl, mat[,acct,drop=FALSE], 2, runZIP)
}; runZIPPar <- cmpfun(runZIPPar, options=list(suppressUndefined=TRUE))

runZeroExcludedModel <- function(x) {
    if ( sum(is.na(x)) )
        return(FALSE)
    n <- length(x)
    latest.x <- x[n]
    if ( latest.x == 0 )
        return(FALSE)
    x <- x[-n]
    x_no0 <- x[x != 0]
    if ( length(x_no0) < 30 )
        return(FALSE)
    if ( IQR(x_no0) == 0 ) {
        return(FALSE)
    } else {
        (latest.x - median(x_no0)) / IQR(x_no0) > 5
    }
}; runZeroExcludedModel <- cmpfun(runZeroExcludedModel, options=list(suppressUndefined=TRUE))

runZeroExcludedModel2 <- function(x) {
    # not used
    require(robustbase)
    n <- length(x)
    latest.x <- x[n]
    if ( latest.x == 0 )
        return(FALSE)
    x <- x[-n]
    x_no0 <- x[x != 0]
    isOutlying <- (adjOutlyingness(x_no0, only.outlyingness=T) == 1)
    tail(isOutlying, 1)
}; runZeroExcludedModel2 <- cmpfun(runZeroExcludedModel2, options=list(suppressUndefined=TRUE))

write2DB <- function(DT, stdout=FALSE) {
    # not used in the streaming version
    require(data.table)
    setorder(DT, acct, ip, ts)
    out <- DT[, .SD[1], by="acct,ip"] # take first occurrence for each account-host pair
    setnames(out, "ts", "log_ts")
    setnames(out, "acct", "fromNode")
    setnames(out, "ip", "toNode")
    out <- out[, `:=`(fromGroup='A',
                      toGroup="H",
                      algorithmName="smart_expert_rule_1",
                      logType="AD",
                      subject="Too many machines successfully authenticated compared to own history.",
                      reason="",
                      categoryid="0",
                      ext="{}")]
    out <- out[, Gid:=.GRP, by="fromNode"]
    out <- out[, hash:=sapply(paste0(Sys.time(), algorithmName, Gid), digest::digest, algo="md5", serialize=FALSE)]
    # order of fields must be respected
    out <- out[, c("log_ts", 
                   "fromNode", 
                   "fromGroup", 
                   "toNode", 
                   "toGroup", 
                   "algorithmName", 
                   "logType", 
                   "subject", 
                   "reason", 
                   "ext", 
                   "hash", 
                   "categoryid"), with=FALSE]
    if ( stdout ) {
        write.table(out, '',
                    sep='\t', quote=FALSE, row.names=FALSE, col.names=FALSE)
    } else {
        write.table(out, pipe(sprintf("java -jar %s %s", insertDB_jar, insertDB_conf)),
                    sep='\t', quote=FALSE, row.names=FALSE, col.names=FALSE)
    }
}; write2DB <- cmpfun(write2DB, options=list(suppressUndefined=TRUE))

saveCounts <- function(mat, fname) {
    save(mat, file=fname)
}; saveCounts <- cmpfun(saveCounts, options=list(suppressUndefined=TRUE))



