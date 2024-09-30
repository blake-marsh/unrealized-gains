rm(list = ls())

library(RPostgres)
library(data.table)
library(zoo)

setwd("~/unrealized-gains/")

#---------------------------
# Connect to WRDS database
#---------------------------
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')


#------------------------
# Get the raw trace data
#------------------------
df = readRDS("./data/trace_enhanced_clean.rds")

#----------------------
# get a list of cusips
#----------------------
cusips = unique(df$cusip_id)

#------------------
# query debt types
#------------------
q = paste("SELECT DISTINCT debt_type_cd
           FROM trace.masterfile
           WHERE cusip_id IS NOT NULL
             AND debt_type_cd IS NOT NULL
             AND cusip_id IN (",paste(shQuote(cusips), collapse=","), ")")
q <- dbSendQuery(wrds, q)
debt_types <- dbFetch(q)
setDT(debt_types)
dbClearResult(q)

#--------------------
# query coupon types
#--------------------
q = paste("SELECT DISTINCT cpn_type_cd
           FROM trace.masterfile
           WHERE cusip_id IS NOT NULL
             AND cpn_type_cd IS NOT NULL
             AND cusip_id IN (",paste(shQuote(cusips), collapse=","), ")")
q <- dbSendQuery(wrds, q)
cpn_types <- dbFetch(q)
setDT(cpn_types)
dbClearResult(q)

#-----------------
# Export datasets
#-----------------
write.table(debt_types, "./data/trace_enhanced_debt_type_codes.csv", sep=",", row.names=F)
write.table(cpn_types, "./data/trace_enhanced_cpn_type_code.csv", sep=",", row.names=F)



