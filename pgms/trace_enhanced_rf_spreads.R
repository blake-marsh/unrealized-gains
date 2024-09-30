rm(list = ls())

library(data.table)
library(zoo)
library(lubridate)
library(RPostgres)
library(stats)
library(parallel)

setwd("~/unrealized-gains")

#----------------
# Parallel setup
#----------------
cores = detectCores()
print(paste("Cores:", cores))

#-------------------------
# FUNCTION: coupon dates
#-------------------------
cpn_dates <- function(issue_date, maturity_date, trade_date=NA, cpn_freq=NA, rtn="pmts"){
     
  ## Figure out the months between coupons

  if (is.na(cpn_freq)) { 
    cpn_freq = 1
  }

   cpn_freq_months = 12/cpn_freq
  
   ## vector to store the dates
   coupon_dates = c()

   ## generate payment dates
   count = 0
   repeat {
      temp = maturity_date %m-% months(cpn_freq_months*count)
      if (temp > issue_date) {
         coupon_dates = c(coupon_dates,temp)
         count = count + 1
      } else {
         break }
   }   

  ## get dates since trade dates
      coupon_dates = coupon_dates[which(coupon_dates >= trade_date)]
 
  ## sort dates
  coupon_dates = sort(as.Date(coupon_dates))
  
  ## number of payments remaining
  pmts = length(coupon_dates)
  
  ## setup return
  if (rtn == "pmts") {
     r = pmts 
  } else if (rtn == "coupon_dates") {
	  r = coupon_dates
  } else if (rtn == "all") {
     r = list('coupon_dates'=coupon_dates, 'pmts'=pmts) 
  }
 
  return(r)
}

#---------------------------------------
# FUNCTION: Present value calcuation of risk-free
# using market rates
#----------------------------------------
PV_calc <- function(trade_date, issue_date, maturity_date, coupon, cpn_freq, method="continuous") {

    ## get coupon payment 
    if (is.na(cpn_freq)) {
         
	coupon_pmt = 0
	cpn_freq = 1

    } else {

       coupon_pmt = 100*(coupon/100)/cpn_freq
    }
    
    ## determine coupon dates
    d = cpn_dates(issue_date, maturity_date, trade_date=trade_date, cpn_freq=cpn_freq, rtn="coupon_dates") 
    
    # number of days to each coupon payment
    # from trade date
    cpn_days = as.numeric(d - trade_date)

    # Calculate present value of coupons
    #  loop over days to coupon payment
    #    1) Get interpolated treasury rate
    #    2) discount the coupon payment
    h15_trade = h15[which(date == trade_date),]
    h15_pts = sort(unique(h15_trade$days))
    nper = 0
    PV = 0
 
    for (i in cpn_days) {
        nper = nper + 1

        ## get the treasury yield
        l = sort(h15_pts[which(h15_pts <= i)], decreasing=T)[1]
        u = h15_pts[which(h15_pts >= i)][1]
        a = as.numeric(h15_trade[which(days == l),list(yield)])
        b = as.numeric(h15_trade[which(days == u),list(yield)])
        step = ifelse(u-l != 0, (b-a)/(u-l), 0)
        r = a + step*(i -l)

        ## coupon present-value
        if (method == "discrete") {
            coupon_PV = coupon_pmt/((1+r/cpn_freq)^nper)
        } else if (method == "continuous") {
            coupon_PV = coupon_pmt/exp((r/cpn_freq)*nper)
        } 
        PV = PV + coupon_PV
    }

    ## This assumes the last coupon 
    ## is paid on the maturity date
    if (method == "discrete") {
       PV = PV + (100/(1+r/cpn_freq)^nper)
    } else if (method == "continuous") {
       PV = PV + (100/(exp((r/cpn_freq)*nper)))
    }

    ## price as percent of face value
    price = PV/100*100
    
    #return(list('PV' = PV, 'price' = price))
    return(price)
}

#----------------------------------------------
# FUNCTION: solve for YTM
#    for zero coupon pmts = years to maturity
#----------------------------------------------
ytm_func <- function(r, PV, nper, coupon, cpn_freq, method="continuous") {

  if (is.na(coupon) | coupon == 0){
     if (method == "discrete") {
         price = 100/(1 + r)^nper
     } else if (method == "continuous") {
         price = 100/exp(r*nper)    
     }

  } else {

     c = 100*(coupon/100)/cpn_freq 
     if (method == "discrete") {
         price = 100/(1+r/cpn_freq)^nper
         for (i in seq(nper)){
           coupon_PV = c/((1+r/cpn_freq)^i)
           price = price + coupon_PV
         }
     } else if (method == "continuous") {
         price = 100/exp((r/cpn_freq)*nper)
         for (i in seq(nper)){
           coupon_PV = c/exp((r/cpn_freq)*i)
           price = price + coupon_PV
         }
      }
  }
  diff = PV - price
  return(diff)
}

get_ytm <- function(PV, nper, coupon, cpn_freq, method="continuous") {

 r = tryCatch(
    uniroot(f=ytm_func, interval=c(1E-15, 10), PV=PV, nper=nper, coupon=coupon, cpn_freq=cpn_freq, method=method, tol=1E-6)$root, 
    error = function(e) { return(NA) }
 )
 return(r)
}

#------------------
# Load TRACE data
#------------------

start.time = Sys.time()

df = readRDS("./data/trace_enhanced_sample.rds")

end.time = Sys.time()

print(paste("trace load time:", end.time - start.time))
print(paste("TRACE sample rows:", nrow(df)))

#--------------------
# Get Treasury rates
#--------------------

## read the data
h15 = readRDS("./data/H15.rds")
h15 = h15[which(date >= as.Date('2015-01-01')),]

## reshape the data
cols = names(h15)[which(substr(names(h15), 1, 3) == "tyd")]
h15 = reshape(h15,
              idvar = "date",
              varying = cols,
              v.names = "yield",
              timevar = "maturity",
              times = cols,
              direction = "long")
setDT(h15)

# Calculate days
h15[,fstub := substring(maturity,6,6)]
h15[,nstub := as.numeric(substring(maturity,4,5))]
h15[,days := ifelse(fstub == "m", floor(nstub/12*365), nstub*365)]
h15[,(c("fstub", "nstub")) := NULL]

#----------------
# Sample data
#----------------
#df = df[which(trd_exctn_dt == as.Date('2019-01-07') & cusip_id == '46625HJZ4'),]
#df = df[which(trd_exctn_dt == as.Date('2019-01-04') & cusip_id == '46625HJZ4'),]
#df = df[1:10000,]
#df = df[which(zero_cpn == 1),]

#--------------------------------------------
# Determine the number of payments remaining
#--------------------------------------------
start.time = Sys.time()
df[,nper := mcmapply(cpn_dates, issue_dt, maturity_dt, trd_exctn_dt, cpn_freq, rtn="pmts", mc.cores=cores)]
end.time = Sys.time()
print(paste("nper time:", end.time - start.time))

saveRDS(df, "./data/trace_enhanced_sample_w_payments.rds")

#----------------------------------------------------
# Calculate present value of the risk free synthetic
#----------------------------------------------------
start.time = Sys.time()
df[,price_rf := mcmapply(PV_calc, trd_exctn_dt, issue_dt, maturity_dt, cpn_rate, cpn_freq, method="continuous", mc.cores=cores)]
end.time = Sys.time()
print(paste("RF continuous pricing time:", end.time - start.time))

start.time = Sys.time()
df[,price_rf_discrete := mcmapply(PV_calc, trd_exctn_dt, issue_dt, maturity_dt, cpn_rate, cpn_freq, method="discrete", mc.cores=cores)]
end.time = Sys.time()
print(paste("RF discrete pricing time:", end.time - start.time))

#-------------------------
# Solve for risk-free YTM
#-------------------------
start.time = Sys.time()
df[,ytm_rf := mcmapply(get_ytm, price_rf, nper, cpn_rate, cpn_freq, method="continuous", mc.cores=cores)]
end.time = Sys.time()
print(paste("Risk-free continuous YTM time:", end.time - start.time))

start.time = Sys.time()
df[,ytm_rf_discrete := mcmapply(get_ytm, price_rf_discrete, nper, cpn_rate, cpn_freq, method="discrete", mc.cores=cores)]
end.time = Sys.time()
print(paste("Risk-free discrete YTM time:", end.time - start.time))

#----------------------
# Solve for trade YTM
#---------------------
start.time = Sys.time()
df[,ytm_trade := mcmapply(get_ytm, price, nper, cpn_rate, cpn_freq, method="continuous", mc.cores=cores)]
end.time = Sys.time()
print(paste("Trade continuous YTM time:", end.time - start.time))

start.time = Sys.time()
df[,ytm_trade_discrete := mcmapply(get_ytm, price, nper, cpn_rate, cpn_freq, method="discrete", mc.cores=cores)]
end.time = Sys.time()
print(paste("Trade discrete YTM time:", end.time - start.time))

#-------------------
# calculate spreads
#-------------------

## spreads from reported TRACE yields
df[,rf_spread := 100*100*(yield/100 - ytm_rf)]
df[,rf_spread_discrete := 100*100*(yield/100 - ytm_rf_discrete)]

## spreads with calculate yield from TRACE price
df[,rf_spread_recalc := 100*100*(ytm_trade - ytm_rf)]
df[,rf_spread_recalc_discrete := 100*100*(ytm_trade_discrete - ytm_rf_discrete)]

#--------------------------------------
# Count obs with missing yield spreads
#--------------------------------------

obs_count = readRDS("./data/trace_enhanced_observation_count.rds")

missing_count = df[(!is.na(yield) | !is.na(ytm_trade)),list(trades = .N,
                                          CUSIPs = length(unique(cusip_id)),
                                          banks = length(unique(id_rssd)))]
missing_count[,step := "Missing Spread"]

obs_count = rbindlist(list(obs_count, missing_count), use.names=T, fill=T)
print(obs_count)

saveRDS(obs_count, "./data/trace_enhanced_observation_count.rds")

#-------------
# Export data
#-------------
saveRDS(df, "./data/trace_enhanced_rf_spreads.rds")
write.table(df, "./data/trace_enhanced_rf_spreads.psv", sep="|", row.names=F, na="")



