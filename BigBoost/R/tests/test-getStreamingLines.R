


context("Read streaming input")

testfname <- "test-streaming-input"
testfname2 <- "test-streaming-input-1line"
testfname3 <- "test-streaming-input-null"

test_that("argument cols must be at least of length 1", {
    expect_error(getStreamingLines(input=testfname), regexp="argument \"cols\" is missing, with no default")
    expect_error(getStreamingLines(input=testfname, cols=NULL), regexp="attempt to select less than one element")
    expect_output(str(getStreamingLines(input=testfname, cols=c('a'))), regexp="data.table.*1 variable")
    expect_output(str(getStreamingLines(input=testfname, cols=c('a', 'b'))), regexp="data.table.*2 variable")
    expect_output(str(getStreamingLines(input=testfname, cols=c('a', 'b', 'c'))), regexp="data.table.*3 variable")
    expect_output(str(getStreamingLines(input=testfname, cols=c('a', 'b', 'c', 'd'))), regexp="data.table.*4 variable")
})

test_that("one-line data is respected", {
    expect_is(getStreamingLines(input=testfname2, cols=c('a')), "data.table")
    expect_is(getStreamingLines(input=testfname2, cols=c('a', 'b')), "data.table")
    expect_is(getStreamingLines(input=testfname2, cols=c('a', 'b', 'c')), "data.table")
    expect_is(getStreamingLines(input=testfname2, cols=c('a', 'b', 'c', 'd')), "data.table")
})

test_that("even empty data is respected", {
    expect_is(getStreamingLines(input=testfname3, cols=c('a')), "data.table")
    expect_is(getStreamingLines(input=testfname3, cols=c('a', 'b')), "data.table")
    expect_is(getStreamingLines(input=testfname3, cols=c('a', 'b', 'c')), "data.table")
    expect_is(getStreamingLines(input=testfname3, cols=c('a', 'b', 'c', 'd')), "data.table")
})

test_that("cols must be a character vector", {
    expect_identical(names(getStreamingLines(input=testfname, cols=rep('a', 3))), rep('a', 3))
    expect_error(getStreamingLines(input=testfname, cols=rep(1, 3)), regexp="Needs to be type 'character'")
})



