#!/bin/bash

# --------------------------------------------------------------------
# Launch Gain Selection process given an observation date (OBS_DATE), 
# configuration file (CFG), and after activating the conda environment
# (CONDA_ENV); these three parameters are exported from osa-env.sh 
# Please note that Gain Selection will start up to 6 AM (when the night has ended),
# and Gain Selection register is saved in {OBS_DATE}_LST1.log
# --------------------------------------------------------------------

# Export parameters from osa-env.sh
source /fefs/aswg/workspace/maria.rivero/lstosa/src/osa/crontab/osa-env.sh

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

LOGDIR="${LSTN1}/OSA/GainSel_log"
WORKDIR="${LSTN1}/OSA/GainSel/${obsdate}"

LOGFILE="${LOGDIR}/${OBS_DATE}_LST1.log"
FLAG_FILE="${WORKDIR}/GainSelFinished.txt"

# -------------------------
# Check GainSelFinished file exists
# -------------------------
if [ -e "$FLAG_FILE" ]; then
    echo "GainSelFinished.txt exists for ${OBS_DATE}" >> "$LOGFILE"
    exit
fi
# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run GAIN SELECTION
# -------------------------
{
    gain_selection \
        --no-queue-check \
        -c "$CFG" \
        -d "$OBS_DATE" \
        -s 6 

} >> "$LOGFILE" 2>&1
