#!/bin/bash

# Usage: bash run_summary YYYYMMDD

source ~/.bash_profile
conda activate cta

obsdate=$1

echo "Run summary: $obsdate"
lstchain_create_run_summary -d $obsdate > /fefs/aswg/data/real/monitoring/RunSummary/log/summary_$obsdate.log 2>&1 

