#!/bin/bash

# --------------------------------------------------------------------
# Runs from DL1A, produces Cat-B calibration and tailcut files 
# --------------------------------------------------------------------

# Export parameters from osa-env.sh
source /fefs/aswg/workspace/maria.rivero/lstosa/src/osa/crontab/osa-env.sh

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

LOGDIR="${LSTN1}/OSA/Sequencer_log"
LOGFILE="${LOGDIR}/${OBS_DATE}_tailcuts_LST1.log"

RA_DIR="${LSTN1}/running_analysis/${obsdate}"

# -------------------------
# Check running_analysis
# -------------------------
if [ ! -e "$RA_DIR" ]; then
    echo "No running analysis directory for ${OBS_DATE} yet" >> "$LOGFILE"
    exit
fi

# -------------------------
# Check NightFinished.txt
# -------------------------
exists() {
    compgen -G "$1" > /dev/null
}

if exists "${LSTN1}/OSA/Closer/${obsdate}/v*/NightFinished.txt" ; then
    echo "Date ${obsdate} is already closed for LST1" >> "$LOGFILE"
    exit
fi

# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run SEQUENCER CATEGORY B
# -------------------------
{
    sequencer_catB_tailcuts \
	-c "$CFG" \
	-d "$OBS_DATE" LST1 \
        "$@"

} >> "$LOGFILE" 2>&1

