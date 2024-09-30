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

#---------------------------
# Read the clean trace data
#---------------------------
df = readRDS("./data/trace_enhanced_clean.rds")
print(paste("Number of trades:", nrow(df)))

#-------------------------
# Read the bloomberg data
#-------------------------

## read first bbg pull
bbg1 = fread("./data/trace_enhanced_bbg_cusip_characteristics_file1.csv", sep=",", stringsAsFactors=F, header=T, na.strings=c("#N/A", "N.A."))
setnames(bbg1, old=c('CPN().value', 'cpn_freq().value', 'ISSUE_DT().value', 'MATURITY().value', 'rtg_fitch()', 'rtg_moody()', 'rtg_sp()'),
              new=c('cpn_rate', 'cpn_freq', 'issue_dt', 'maturity_dt', 'rtg_fitch', 'rtg_moody', 'rtg_sp'))
bbg1[,issue_dt := as.Date(issue_dt, origin='1899-12-30')]
bbg1[,maturity_dt := as.Date(maturity_dt, origin='1899-12-30')]
bbg1 = bbg1[which(cusip_id != '########'),]
bbg1[,delete := ifelse(cusip_id == '173000000' & issue_dt == as.Date('2013-04-30'), 1, 0)]
bbg1 = bbg1[which(delete == 0),]
bbg1[,delete := NULL]
any(duplicated(bbg1$cusip_id))

## read second bbg pull
bbg2 = fread("./data/trace_enhanced_bbg_cusip_characteristics_file2.csv", sep=",", stringsAsFactors=F, header=T, na.strings=c("#N/A", "N.A."))
setnames(bbg2, old=c('CPN().value', 'cpn_freq().value', 'ISSUE_DT().value', 'MATURITY().value', 'rtg_fitch()', 'rtg_moody()', 'rtg_sp()'),
              new=c('cpn_rate', 'cpn_freq', 'issue_dt', 'maturity_dt', 'rtg_fitch', 'rtg_moody', 'rtg_sp'))
bbg2[,issue_dt := as.Date(issue_dt, format='%m/%d/%Y')]
bbg2[,maturity_dt := as.Date(maturity_dt, format='%m/%d/%Y')]
any(duplicated(bbg2$cusip_id))

## real the third bbg pull
bbg3 = fread("./data/trace_enhanced_bbg_cusip_characteristics_file3.csv", sep=",", stringsAsFactors=F, header=T, na.strings=c("#N/A", "N.A."))
setnames(bbg3, old=c('CPN().value', 'cpn_freq().value', 'ISSUE_DT().value', 'MATURITY().value', 'rtg_fitch()', 'rtg_moody()', 'rtg_sp()'),
              new=c('cpn_rate', 'cpn_freq', 'issue_dt', 'maturity_dt', 'rtg_fitch', 'rtg_moody', 'rtg_sp'))
bbg3[,issue_dt := as.Date(issue_dt, origin='1899-12-30')]
bbg3[,maturity_dt := as.Date(maturity_dt, origin='1899-12-30')]
any(duplicated(bbg3$cusip_id))

## stack the datasets
bbg = rbindlist(list(bbg1, bbg2, bbg3), use.names=T, fill=T)

## check duplicates
any(duplicated(bbg$cusip_id))

## add bloomebrg flag
bbg[,bbg := 1]

#-----------
# Dedup
#-----------
bbg = unique(bbg)

#-------------------------
## check duplicate CUSIPs
#-------------------------
print(paste("Any duplicated CUSIPs in BBG:", any(duplicated(bbg$cusip_id))))

#------------------------------------
# merge the characteristics to TRACE
#------------------------------------
df = merge(df, bbg, by="cusip_id", all.x=T)
print(paste("Number of trades after BBG characteristics merge:", nrow(df)))

#------------------------------------
# Merge in the coupon types from BBG
#------------------------------------

## read coupon types data
cpn_types = fread("./data/all_bond_coupon_types.csv", sep=",", stringsAsFactors=F)
cpn_types[,bbg_cpn_typ := ifelse(!is.na(bbg_cpn_typ) & bbg_cpn_typ == '#N/A', NA, bbg_cpn_typ)]
print(paste("Any duplicated CUSIPs in BBG coupon types:", any(duplicated(cpn_types$cusip_id))))

## merge to bloomberg data
df = merge(df, cpn_types, by="cusip_id", all.x=T)
print(paste("Number of trades after BBG coupon type merge:", nrow(df)))

## create a fixed coupon indicator
df[,fixed := ifelse(bbg_cpn_typ == "FIXED" & !is.na(bbg_cpn_typ), 1, 0)]

#------------------------------------------
# Create an indictor for subordinated debt
# using TRACE debt type codes
#------------------------------------------

## get a list of cusips
cusips = unique(df$cusip_id)

## query the master file
q = paste("SELECT DISTINCT cusip_id, 1 as subordinated
           FROM trace.masterfile
           WHERE cusip_id IS NOT NULL
             AND debt_type_cd IS NOT NULL
             AND debt_type_cd IN ('B-BND', 'B-BNT', 'B-CBND', 'B-CSEC', 'B-DEB',
                                  'B-NT', 'JRSUBNT', 'JSB-BND', 'JSB-CPD', 'JSB-CSEC',
                                  'JSB-DEB', 'JSB-NT', 'JSB-PF', 'JSB-SPV', 'SB-BNT',
                                  'SB-CSEC', 'SB-DEB', 'SB-NT', 'SUBBNT', 'SUBDEB', 'SUBNT')
             AND mtrty_dt >= CAST('2021-01-01' AS DATE)
             AND cusip_id IN (",paste(shQuote(cusips), collapse=","), ")")
q <- dbSendQuery(wrds, q)
subordinated_debt <- dbFetch(q)
setDT(subordinated_debt)
dbClearResult(q)

## check duplicates
any(duplicated(subordinated_debt$cusip_id))

## merge to dataset
df = merge(df, subordinated_debt, by="cusip_id", all.x=T)
df[,subordinated := ifelse(is.na(subordinated), 0, subordinated)]

#---------------------------------------
# Create an indicator for secured debt
# using TRACE debt type codes
#--------------------------------------
## get a list of cusips
cusips = unique(df$cusip_id)

## query the master file
q = paste("SELECT DISTINCT cusip_id, 1 as secured
           FROM trace.masterfile
           WHERE cusip_id IS NOT NULL
             AND debt_type_cd IS NOT NULL
             AND debt_type_cd IN ('SSC-CBND', 'SSC-NT')
             AND mtrty_dt >= CAST('2021-01-01' AS DATE)
             AND cusip_id IN (",paste(shQuote(cusips), collapse=","), ")")
q <- dbSendQuery(wrds, q)
secured_debt <- dbFetch(q)
setDT(secured_debt)
dbClearResult(q)

## check duplicates
any(duplicated(secured_debt$cusip_id))

## merge to dataset
df = merge(df, secured_debt, by="cusip_id", all.x=T)
df[,secured := ifelse(is.na(secured), 0, secured)]

#------------------------
# add a zero coupon flag
#------------------------
df[,cpn_rate := ifelse(is.na(cpn_rate), 0, cpn_rate)]
df[,cpn_freq := ifelse(cpn_rate == 0 |(cpn_freq == 0 & !is.na(cpn_freq)), NA, cpn_freq)]

df[,zero_cpn := ifelse(bbg_cpn_typ == 'ZERO COUPON', 1, 0)]

#--------------------------------------
# Flag new issuance in the trade year
#-------------------------------------
df[,issued_in_trade_year := ifelse(year(trd_exctn_dt) == year(issue_dt), 1, 0)]

#--------------------
# Calculate bond age
#--------------------
df[,age := as.numeric(trd_exctn_dt - issue_dt)/365]

#------------------------
# Create ratings groups
#-----------------------

## S&P ratings
sp_scale = c("AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+")
df[,rtg_sp_cat := NA]
count = 1
for (i in sp_scale){
  df[,rtg_sp_cat := ifelse(!is.na(rtg_sp) & rtg_sp == i, count, rtg_sp_cat)]
  count = count + 1
}
df[,rtg_sp_cat := ifelse(!is.na(rtg_sp) & rtg_sp == 'Ap', 6, rtg_sp_cat)]
df[,rtg_sp_cat := ifelse(!is.na(rtg_sp) & rtg_sp == 'A-p', 7, rtg_sp_cat)]
df[,rtg_sp_cat := ifelse(!is.na(rtg_sp) & rtg_sp == 'BBB+p', 8, rtg_sp_cat)]
df[,rtg_sp_cat := ifelse(is.na(rtg_sp) | rtg_sp == "NR", 99, rtg_sp_cat)]

## Fitch ratings
fitch_scale = c("AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+")
df[,rtg_fitch_cat := NA]
count = 1
for (i in fitch_scale){
  df[,rtg_fitch_cat := ifelse(!is.na(rtg_fitch) & rtg_fitch == i, count, rtg_fitch_cat)]
  count = count + 1
}
df[,rtg_fitch_cat := ifelse(!is.na(rtg_fitch) & rtg_fitch == "AA-(EXP)", 4, rtg_fitch_cat)]
df[,rtg_fitch_cat := ifelse(is.na(rtg_fitch) | rtg_fitch == "NR", 99, rtg_fitch_cat)]
df[,rtg_fitch_cat := ifelse(!is.na(rtg_fitch) & rtg_fitch == "WD", -1, rtg_fitch_cat)]

## Moodys Ratings
moody_scale = c("Aaa", "Aa1", "Aa2", "Aa3", "A1", "A2", "A3", "Baa1", "Baa2", "Baa3", "Ba1", "Ba2", "Ba3", "B1")
df[,rtg_moody_cat := NA]
count = 1
for (i in moody_scale){
  df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == i, count, rtg_moody_cat)]
  count = count + 1
}
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == 'A1 *-', 5, rtg_moody_cat)]
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == 'A2 *-', 6, rtg_moody_cat)]
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody %in% c('A3 *-', "A3u"), 7, rtg_moody_cat)]
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == 'Aa2 *-', 3, rtg_moody_cat)]
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == 'Baa1 *-', 8, rtg_moody_cat)]
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == 'Baa2 *-', 9, rtg_moody_cat)]

df[,rtg_moody_cat := ifelse(is.na(rtg_moody) | rtg_moody == "NR", 99, rtg_moody_cat)]
df[,rtg_moody_cat := ifelse(!is.na(rtg_moody) & rtg_moody == "WR", -1, rtg_moody_cat)]

#-------------------------
# Check final trade count
#-------------------------
print(paste("Final trade count:", nrow(df)))

#--------------------------------
# Get a list of unmatched CUSIPs
#--------------------------------

## identify unmatched
unmatched = unique(df[is.na(bbg),list(cusip_id)])

## count unmatched cusips
print(paste("Unmatched CUSIP count:", nrow(unmatched)))

## export unmatched
#write.table(unmatched, "./data/unmatched_cusips.psv", sep="|", row.names=F)
saveRDS(unmatched, "./data/unmatched_cusips.rds")

#------------------------
# export the merged data
#------------------------
#write.table(df, "./data/trace_enhanced_with_bbg_characteristics.psv", sep="|", row.names=F)
saveRDS(df, "./data/trace_enhanced_with_bbg_characteristics.rds")




