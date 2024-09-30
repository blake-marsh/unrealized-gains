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

#---------------------------------------
# Read the clean and merged trace data
#---------------------------------------
df = readRDS("./data/trace_enhanced_with_bbg_characteristics.rds")

#----------
# Load H15
#----------
h15 = readRDS("./data/H15.rds")

#--------------------------
# get a list of trade days
#--------------------------
trade_days = h15[,list(date)]

#----------------------------------------------------
# Get list of cusips with trace debt types to delete
#----------------------------------------------------

## get a list of cusips
cusips = unique(df$cusip_id)

## query the master file
q = paste("SELECT DISTINCT cusip_id, 1 as delete 
           FROM trace.masterfile
           WHERE cusip_id IS NOT NULL
             AND debt_type_cd IS NOT NULL
             AND debt_type_cd IN ('1M-NT', '2LN-NT', 'B-DPSH', 'B-PF', 'B-SPV',
                                  'DEPSH', 'IDX', 'JSB-DPSH', 'OTH', 'OTH-BND',
                                  'OTH-CBND', 'OTH-ENT', 'OTH-ILS', 'OTH-NT', 'OTH-OTH',
                                  'PFDSTK', 'SB-DPSH', 'SBN-NT', 'SB-PF', 'S-ENT', 'S-EQTR',
                                  'S-NTBC', 'S-OTH', 'SSC-OTH', 'S-SP', 'STPROD', 'S-VBND',
                                  'TRPFDSEC', 'UN-DPSH', 'UN-ENT', 'UN-PF') 
	     AND mtrty_dt >= CAST('2021-01-01' AS DATE)
	     AND cusip_id IN (",paste(shQuote(cusips), collapse=","), ")") 
q <- dbSendQuery(wrds, q)
debt_type_delete <- dbFetch(q)
setDT(debt_type_delete)
dbClearResult(q)

## check duplicates
any(duplicated(debt_type_delete$cusip_id))

#-----------------------
# close wrds connection
#-----------------------
dbDisconnect(wrds)

#--------------------------
# Print merged sample size
#--------------------------
raw_count = df[,list(trades = .N,
		     CUSIPs = length(unique(cusip_id)),
		     banks = length(unique(id_rssd)))]
raw_count[,step := "Raw data"]

#--------------------------------------------
# Remove trades reported on non-trading days
#--------------------------------------------
df = merge(df, trade_days, by.x="trd_exctn_dt", by.y="date")

nontrade_days_count = df[,list(trades = .N,
                               CUSIPs = length(unique(cusip_id)),
                               banks = length(unique(id_rssd)))]
nontrade_days_count[,step := "Non-trading days"]

#-------------------------------------------
# Keep trades that have BBG Characteristics
#-------------------------------------------
df = df[!is.na(bbg),]

bbg_count = df[,list(trades = .N,
                     CUSIPs = length(unique(cusip_id)),
                     banks = length(unique(id_rssd)))]
bbg_count[,step := "BBG Characteristics"]

#----------------------------------------------------------
# Remove trades that are not bank issuer names in trace
# To update: check issuer name file and add cusips to list
#----------------------------------------------------------
df = df[which(!(cusip_id %in% c('370425RZ5', '36186CBY8'))),]

nonbank_count = df[,list(trades = .N,
                     CUSIPs = length(unique(cusip_id)),
                     banks = length(unique(id_rssd)))]
nonbank_count[,step := "Non-bank Trades"]

#-------------------
# Remove debt types
#-------------------
df = merge(df, debt_type_delete, by=c("cusip_id"), all.x=T)
df = df[is.na(delete),]
df[,delete := NULL]
df = df[which(sub_prdct != "ELN"),]

debt_type_count = df[,list(trades = .N,
                           CUSIPs = length(unique(cusip_id)),
                           banks = length(unique(id_rssd)))]
debt_type_count[,step := "Debt Type"]

#----------------------------
# Retain FIXED VARIABLE FLOATING and STEP coupon types
#----------------------------
df = df[which(!is.na(bbg_cpn_typ) & bbg_cpn_typ %in% c('FIXED', 'VARIABLE', 'STEP CPN')),]

cpn_type_count = df[,list(trades = .N,
                           CUSIPs = length(unique(cusip_id)),
                           banks = length(unique(id_rssd)))]
cpn_type_count[,step := "Coupon Type"]

#----------------------------------------------
# Remove bonds without issue or maturity dates
#----------------------------------------------
df = df[which(!is.na(issue_dt) & !is.na(maturity_dt)),]
    
dt_count = df[,list(trades = .N,
                    CUSIPs = length(unique(cusip_id)),
                    banks = length(unique(id_rssd)))]
dt_count[,step := "Issue/Maturity Date"]

#-----------------------------------------------------------------
# Drop bonds with maturity less than 1 year or more than 30 years
#-----------------------------------------------------------------

## calculate original and remaining maturity in years
df[,maturity_yrs_orig := as.numeric(maturity_dt - issue_dt)/365]
df[,maturity_yrs_remain := as.numeric(maturity_dt - trd_exctn_dt)/365]

## trim remaining maturity less than 1 year and more than 30
df = df[which(1 <= maturity_yrs_remain & maturity_yrs_remain <= 30),]

## drop if bond has a short-term debt rating
df = df[which(!(rtg_fitch %in% c('F1', 'F1+'))),]

maturity_count = df[,list(trades = .N,
                          CUSIPs = length(unique(cusip_id)),
                          banks = length(unique(id_rssd)))]
maturity_count[,step := "Remaining Maturity"]

#-------------------------------------------------------------
# Check for at least one 100,000+ trade on each day per CUSIP
#-------------------------------------------------------------

## max trade size per date
df[,max_trade_size := max(quantity, na.rm=T), by=list(trd_exctn_dt, cusip_id)]

## keep if max greater than 100,000
df = df[which(max_trade_size >= 100000),]

df[, max_trade_size := NULL]

size_count = df[,list(trades = .N,
                      CUSIPs = length(unique(cusip_id)),
                      banks = length(unique(id_rssd)))]
size_count[,step := "Trade Size"]

#--------------------
# add the bank types
#--------------------

## determine quarter of trade
df[,trade_qtr := as.Date(as.yearqtr(trd_exctn_dt)+0.25)-1]

## get the bank types
bhc_groups = readRDS("./data/MAY9_agGroups.rds")
setDT(bhc_groups)
bhc_groups = bhc_groups[,list(DT, ID_RSSD, GSIB, CCAR)]
bhc_groups[,DT := as.Date(as.character(DT), format = '%Y%m%d')]

## merge in bank type flag
df = merge(df, bhc_groups, by.x=c('trade_qtr', 'id_rssd'), by.y=c('DT', 'ID_RSSD'), all.x=T)
df[,GSIB := ifelse(is.na(GSIB), 0, 1)]
df[,CCAR := ifelse(is.na(CCAR), 0, 1)]

#-------------------
# Stack the counts
#------------------

## build table
count_table = rbindlist(list(raw_count,nontrade_days_count, bbg_count, 
			     nonbank_count, debt_type_count, cpn_type_count,  
			     dt_count, maturity_count, size_count), fill=T, use.names=T)

print("Observation Counts by step")
print(count_table)

saveRDS(count_table, "./data/trace_enhanced_observation_count.rds")

#--------------------
# Export the dataset
#--------------------
saveRDS(df, "./data/trace_enhanced_sample.rds")
#write.table(df, "./data/trace_enhanced_sample.psv", row.names=F, sep="|", na = ".")


