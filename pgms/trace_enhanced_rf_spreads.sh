#!/bin/bash

#$ -cwd

#$ -N trace_enhanced
#$ -pe onenode 8
#$ -l m_mem_free=6G
#$ -o $HOME/unrealized-gains/logfiles/trace_enhanced.o.log
#$ -e $HOME/unrealized-gains/logfiles/trace_enhanced.e.log


cd ~/unrealized-gains/


# download the H15 data
#source ~/virtualenv/base_python/bin/activate
#python ./pgms/download_h15.py
R CMD BATCH --no-save --no-restore ./pgms/format_h15.R ./logfiles/format_h15.Rout
#deactivate

# do the CUSIP match to Y9C data
R CMD BATCH --no-save --no-restore ./pgms/y9c_cusip_match.R ./logfiles/y9c_cusip_match.Rout

## get trace data and process
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_query.R ./logfiles/trace_enhanced_query.Rout
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_debt_types.R ./logfiles/trace_enhanced_debt_types.Rout
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_add_bbg_characteristics.R ./logfiles/trace_enhanced_add_bbg_characteristics.Rout
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_sample.R ./logfiles/trace_enhanced_sample.Rout

## calculate the risk-free spreads off synthetic Treasuries
R CMD BATCH --no-save --no-restore ./pgms/trace_enhanced_rf_spreads.R ./logfiles/trace_enhanced_rf_spreads.Rout




