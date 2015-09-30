## define helper functions for SmartExpertRule1
suppressPackageStartupMessages({
    library(Rcpp)
    library(compiler)
})

getStreamingLines <- function(input="stdin") {
    f <- file(input)
    open(f)
    on.exit(close(f))
    line <- readLines(f, n=1)
    line
}; getStreamingLines <- cmpfun(getStreamingLines, options=list(suppressUndefined=TRUE))
