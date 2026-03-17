#!/bin/bash

# --------------------------------------------------------------------
# Check Gain Selection process given an observation date (OBS_DATE),
# configuration file (CFG), and after activating the conda environment
# (CONDA_ENV); these three parameters are exported from osa-env.sh; 
# When Gain Selection has finished, it 
# creates a flag file for the given date: GainSelFinished.txt
# ...
# Make gain selection xhtml table and copy it to the lst1 webserver.
# --------------------------------------------------------------------

# Export parameters from osa-env.sh
source /fefs/aswg/workspace/maria.rivero/lstosa/src/osa/crontab/osa-env.sh

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

WORKDIR="${LSTN1}/OSA/GainSel/${obsdate}"
FLAG_FILE="${WORKDIR}/GainSelFinished.txt"

LOGDIR="${LSTN1}/OSA/GainSel_log"
LOGFILE="${LOGDIR}/${OBS_DATE}_check_LST1.log"
LOGFILE_WEB="${LOGDIR}/GainSelWeb_${OBS_DATE}_LST1.log"

GS_HTML="${LSTN1}/OSA/GainSelWeb/osa_gainsel_status_${OBS_DATE}.html"
GS_LSTOSA="/home/www/html/datacheck/lstosa/gainsel.xhtml"

# ---------------------------------
# Check GainSelFinished file exists
# ---------------------------------
if [ -e "$FLAG_FILE" ]; then
    echo "GainSelFinished.txt exists for ${OBS_DATE}" >> "$LOGFILE"
    exit
fi

# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run GAIN SELECTION CHECK
# -------------------------
{
    gain_selection --check \
        -c "$CFG" \
	-d "$OBS_DATE" \
        "$@"

} >> "$LOGFILE" 2>&1

# -------------------------
# Update GAIN SELECTION WEB
# -------------------------
{
    gainsel_webmaker \
        -c "$CFG"
} >> "$LOGFILE_WEB" 2>&1

{
    if [ $? = 0 ]; then
        scp "$GS_HTML" datacheck:"$GS_LSTOSA"
    fi
} >> "$LOGFILE_WEB" 2>&1
