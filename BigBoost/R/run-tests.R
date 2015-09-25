#!/usr/bin/env Rscript

library(testthat)

source("ser_helpers.R")

test_dir("tests", reporter="Summary")
