#!/bin/bash

# --------------------------------------------------------------------
# Runs from DL1A, produces Cat-B calibration and tailcut files 
# --------------------------------------------------------------------

obsdate=$(date +\%Y\%m\%d -d yesterday)

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
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run SEQUENCER CATEGORY B
# -------------------------
{
    sequencer_catB_tailcuts \
	-c "$CFG" \
	-d "$OBS_DATE" LST1
} >> "$LOGFILE" 2>&1

