rm(list = ls())

library(RPostgres)
library(data.table)
library(zoo)
library(stringr)

setwd("~/unrealized-gains/")

#-----------------
# WRDS connection
#-----------------
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')

#--------------
# Get Y9C data
#--------------
q = "SELECT CAST(a.rssd9999 AS DATE) as date, 
            a.rssd9001 as id_rssd, a.rssd9017 as name,  
            a.bhck2170 as total_assets, b.id_cusip, b.id_lei, 
            CAST(b.id_tax as varchar(9)) as id_tax, b.nm_lgl
     FROM bank.wrds_holding_bhck_2 as a
       LEFT JOIN (SELECT date_start, date_end, id_rssd, nm_lgl,
                         CASE 
                           WHEN id_cusip = '0' THEN NULL
                           ELSE id_cusip
                         END as id_cusip, 
                         CASE 
                           WHEN id_tax = 0 THEN NULL
                           ELSE id_tax 
                         END as id_tax, id_lei
                  FROM bank.wrds_struct_attributes_active
               UNION 
                  SELECT date_start, date_end, id_rssd, nm_lgl,
                         CASE 
                           WHEN id_cusip = '0' THEN NULL
                           ELSE id_cusip
                         END as id_cusip, 
                         CASE 
                           WHEN id_tax = 0 THEN NULL
                           ELSE id_tax 
                         END as id_tax, id_lei
                  FROM bank.wrds_struct_attributes_closed) as b
       ON a.rssd9001 = b.id_rssd 
        AND a.rssd9999 between b.date_start and b.date_end
      WHERE a.rssd9999 = CAST('2022-12-31' AS DATE)"
q = dbSendQuery(wrds, q)
may9c = dbFetch(q)
dbClearResult(q)
setDT(may9c)

any(duplicated(may9c[,list(date, id_rssd)]))

#----------------------------------
# Get the NYFED's public bank list
#----------------------------------
nyfed = fread("./data/crsp_20230930.csv", sep=",", stringsAsFactors=F)
nyfed[,nyfed_public := 1]
nyfed = nyfed[which(dt_end >= 20221231),]
any(duplicated(nyfed[,list(entity)]))

## merge to Y9C
may9c = merge(may9c, nyfed[,list(entity, permco, nyfed_public)], by.x='id_rssd', by.y = 'entity', all.x=T)
any(duplicated(may9c[,list(date, id_rssd)]))


#-----------------------
# Merge CUSIP from CRSP
#-----------------------
may9c_permcos = may9c$permco[!is.na(may9c$permco)]
q = paste("SELECT DISTINCT permco, permno, substring(trim(hcusip), 1, 6) as cusip6_crsp, htick,
            CAST(begdat AS DATE) as begdat,
            CAST(enddat AS DATE) as enddat
           FROM crspq.dsfhdr62
           WHERE CAST('2022-12-31' AS DATE) between begdat and enddat 
             AND hcusip IS NOT NULL
             AND permco IN (", paste(may9c_permcos, sep=' ', collapse=','), ")", sep="")
q = dbSendQuery(wrds, q)
crsp = dbFetch(q)
dbClearResult(q)
setDT(crsp)
any(duplicated(crsp[,list(permco, permno, begdat)]))

## get distinct cusip6
crsp = unique(crsp[,list(permco, cusip6_crsp, htick)])
any(duplicated(crsp$permco))

## merge to Y9C
may9c = merge(may9c, crsp, by=c('permco'), all.x=T)
any(duplicated(may9c[,list(id_rssd, date)]))

#--------------------
# Get compustat data
#--------------------
q = "SELECT CAST(a.gvkey AS varchar(6)) as gvkey, CAST(a.datadate AS DATE) as datadate, a.fyr,
        a.datacqtr, a.datafqtr, a.atq, a.tic, substring(trim(a.cusip), 1, 6) as cusip_cs, a.conm, b.conml, b.ein,
        CAST(b.naics as varchar(6)) as naics
     FROM (SELECT *
           FROM comp.fundq
           WHERE atq != 0
             AND atq IS NOT NULL
             AND datadate >= CAST('2009-01-01' AS DATE)
             AND datacqtr IS NOT NULL
             AND indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C' AND popsrc = 'D') as a
       LEFT JOIN (SELECT gvkey, ein, naics, conml
                  FROM comp.company) as b
         ON a.gvkey = b.gvkey "
q = dbSendQuery(wrds, q)
compustat = dbFetch(q)
setDT(compustat)
dbClearResult(q)

compustat[,date := as.Date(as.yearqtr(datacqtr)+0.25)-1]

compustat[,ein := str_replace(ein, '-', '')]

#dbDisconnect(wrds)

#-------------
# Clean names
#-------------

## Y9C name
may9c[,clean_nmlgl := stringr::str_replace_all(nm_lgl, stringr::regex("[^a-zA-Z0-9 ]"), " ")]
may9c[,clean_nmlgl := tolower(clean_nmlgl)]
may9c[,clean_nmlgl := stringr::str_replace_all(clean_nmlgl, "[:space:]", " ")]
may9c[,clean_nmlgl := stringr::str_squish(clean_nmlgl)]



## clean compustat name
compustat[,cs_name := ifelse(is.na(conml), conm, conml)]
compustat[,clean_cs_name := stringr::str_replace_all(cs_name, stringr::regex("[^a-zA-Z0-9 ]"), " ")]
compustat[,clean_cs_name := tolower(clean_cs_name)]
compustat[,clean_cs_name := stringr::str_replace_all(clean_cs_name, "[:space:]", " ")]
compustat[,clean_cs_name := stringr::str_squish(clean_cs_name)]

#--------------------------------
# Merge compustat to Y9C by EIN
#-------------------------------
ein_match = merge(may9c[!is.na(id_tax)], compustat[!is.na(ein)], by.x=c('date', 'id_tax'), by.y=c('date', 'ein'))
ein_match[,tier := 'ein']

no_match = may9c[which(!(id_rssd %in% ein_match$id_rssd)),]

#-----------------------
# Merge compustat by name
#-----------------------
name_match = merge(no_match[!is.na(clean_nmlgl),], compustat[!is.na(clean_cs_name)], by.x=c('date', 'clean_nmlgl'), by.y=c('date', 'clean_cs_name'))
name_match[,tier := 'name']

no_match = no_match[which(!(id_rssd %in% name_match$id_rssd)),]

#---------------------
# Export only matches
#---------------------
matches = rbindlist(list(ein_match, name_match), use.names=T, fill=T)
matches = matches[,list(date, id_rssd, gvkey, nm_lgl, id_tax, htick, id_cusip, cusip6_crsp, cusip_cs, total_assets, tier)]
matches = unique(matches)
#write.table(matches, "./data/y9c_cusip_matches.psv", sep='|', row.names=F)
saveRDS(matches, "./data/y9c_cusip_matches.rds")

#------------------------------------------------------
# Get all 6 digit CUSIPs from Compustat security table
#------------------------------------------------------
q = "SELECT DISTINCT gvkey, substring(cusip, 1, 6) as cusip
     FROM comp.security"
q = dbSendQuery(wrds, q)
cs_security = dbFetch(q)
setDT(cs_security)
dbClearResult(q)

setnames(cs_security, old = c('cusip') , new = c('ID_CUSIP'))

## close wrds connection
dbDisconnect(wrds)

## merge in the id_rssds from the Y9C data
cs_security = merge(cs_security, unique(matches[,list(gvkey, id_rssd, htick)]), by="gvkey")

## get unique cusips by id_rssd
cs_security = unique(cs_security[,list(id_rssd, ID_CUSIP)])

#--------------------------------------------
# Stack the CUSIPs from the securities table
# and the CRSP/Compustat/NIC results
#--------------------------------------------

results = reshape(matches[,list(id_rssd, id_cusip, cusip_cs, cusip6_crsp, htick)],
		  varying = c('cusip6_crsp', 'cusip_cs', 'id_cusip'),
		  v.names = "ID_CUSIP",
		  timevar = "source",
		  direction = "long"
		  )

## stack with the security table results
results = rbindlist(list(cs_security, results[,list(id_rssd, ID_CUSIP, htick)]), use.names = T, fill=T)

## dedup
results = unique(results)

# drop rows with missing ticker but CUSIP shows up elsewhere
#results[,count := .N, by=list(id_rssd, ID_CUSIP)]
#results[,has_tck := ifelse(!is.na(htick), 1, 0)]
#results[,has_tck := max(has_tck), by=list(id_rssd, ID_CUSIP)]
#results[,delete := ifelse(count > 1 & is.na(htick) & has_tck == 1, 1,0)]
#results = results[which(delete == 0),]
#results[,(c('count', 'has_tck', 'delete')) := NULL]

#----------------
# Export results
#----------------

## tickers
tcks = unique(results[!is.na(htick),list(id_rssd, htick)])
any(duplicated(tcks[,list(htick)]))
any(duplicated(tcks[,list(id_rssd)]))
saveRDS(tcks, "./data/y9c_results_tcks.rds")

## cusips
cusips = unique(results[!is.na(ID_CUSIP), list(id_rssd, ID_CUSIP)])
cusips[, count := .N, by=list(ID_CUSIP)]
cusips = cusips[which(count == 1),]
cusips[,count := NULL]
any(duplicated(cusips[,list(ID_CUSIP)]))
saveRDS(cusips, "./data/y9c_results_cusips.rds")

#write.table(results, "./data/y9c_cusip_match_all_results.psv", sep="|", row.names=F, na='')
saveRDS(results, "./data/y9c_cusip_match_all_results.rds")



