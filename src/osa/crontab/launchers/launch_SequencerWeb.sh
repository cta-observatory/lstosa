#!/bin/bash

# --------------------------------------------------------------------
# Make sequencer xhtml table and copy it to the lst1 webserver
# --------------------------------------------------------------------

# Export parameters from osa-env.sh
source /fefs/aswg/workspace/maria.rivero/lstosa/src/osa/crontab/osa-env.sh

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

LOGDIR="${LSTN1}/OSA/Minor_logs/"
LOGFILE="${LOGDIR}/${obsdate}_sequencer-web.log"
LOGFILE2="${LOGDIR}/${obsdate}_sequencer-web_2.log"

HTMLDIR="${LSTN1}/OSA/SequencerWeb/"
HTMLFILE="${HTMLDIR}/osa_status_${obsdate}.html"
HTMLFILE2="${HTMLDIR}/osa_status_2_${obsdate}.html"

SEQUENCER_WEB="/home/www/html/datacheck/lstosa/sequencer.xhtml"
SEQUENCER2_WEB="/home/www/html/datacheck/lstosa/sequencer_2.xhtml"

GSDIR="${LSTN1}/OSA/GainSel/${obsdate}"
FLAG_FILE="${GSDIR}/GainSelFinished.txt"

# -------------------------
# Check GainSelFinished.txt
# -------------------------
if [ ! -e "$FLAG_FILE" ]; then
    echo "No GainSelFinished.txt for ${OBS_DATE} yet" >> "$LOGFILE"
    exit
fi

# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run SEQUENCER WEBMAKER
# -------------------------
{
    sequencer_webmaker \
        -c "$CFG"
} >> "$LOGFILE" 2>&1

{
    if [ $? = 0 ]; then
        scp "$HTMLFILE" datacheck:"$SEQUENCER_WEB"
    fi
} >> "$LOGFILE" 2>&1

# -------------------------
# Run SEQUENCER WEBMAKER 2
# -------------------------

{
    sequencer_webmaker_2 \
        -c "$CFG"
} >> "$LOGFILE2" 2>&1

{
    if [ $? = 0 ]; then
        scp "$HTMLFILE2" datacheck:"$SEQUENCER2_WEB"
    fi
} >> "$LOGFILE2" 2>&1
