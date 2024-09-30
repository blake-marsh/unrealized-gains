rm(list = ls())

library(data.table)
library(zoo)

setwd("~/unrealized-gains/")

## load the h15 data
h15 = fread("./data/H15_B.csv", sep=",", stringsAsFactors=F)
h15[,date := as.Date(TIME_PERIOD, format='%Y-%m-%d')]

## convert h15 to decimals
h15_names = c('RIFSPFF_N.B', 'RIFLGFCM01_N.B', 'RIFLGFCM03_N.B', 'RIFLGFCM06_N.B', 'RIFLGFCY01_N.B',
                             'RIFLGFCY02_N.B', 'RIFLGFCY03_N.B', 'RIFLGFCY05_N.B', 'RIFLGFCY07_N.B',
                             'RIFLGFCY10_N.B', 'RIFLGFCY20_N.B', 'RIFLGFCY30_N.B')
h15_names_dec = c('tyd00m', 'tyd01m', 'tyd03m', 'tyd06m', 'tyd01y',
                            'tyd02y', 'tyd03y', 'tyd05y', 'tyd07y',
                            'tyd10y', 'tyd20y', 'tyd30y')
h15[,(h15_names_dec) := lapply(.SD, function(x) as.numeric(x)/100), .SDcols=h15_names]

## keep subset of the data
h15 = h15[which(date >= as.Date('2015-01-01')),list(date, tyd00m, tyd01m, tyd03m, tyd06m, tyd01y,
                                                                  tyd02y, tyd03y, tyd05y, tyd07y,
                                                                  tyd10y, tyd20y, tyd30y)]

## drop non-trade days
h15 = h15[complete.cases(h15),]

# Save the data
h15 = saveRDS(h15, "./data/H15.rds")

