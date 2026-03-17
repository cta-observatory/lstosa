#!/bin/bash

# --------------------------------------------------------------------
# Copy the available calibration and DL1 datacheck to the LST1 webserver.
# --------------------------------------------------------------------

# Export parameters from osa-env.sh
source /fefs/aswg/workspace/maria.rivero/lstosa/src/osa/crontab/osa-env.sh

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

RA_DIR="${LSTN1}/running_analysis/${obsdate}"

# -------------------------
# Check running_analysis
# -------------------------
if [ ! -e "$RA_DIR" ]; then
    exit
fi

# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run COPY DATACHECK
# -------------------------
{
    copy_datacheck \
	-c "$CFG" \
	-d "$OBS_DATE" LST1

}  > /dev/null 2>&1
