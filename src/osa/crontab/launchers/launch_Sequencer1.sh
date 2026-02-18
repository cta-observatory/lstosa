#!/bin/bash

# --------------------------------------------------------------------
# Run SEQUENCER only up to DL1A, given an observation date (OBS_DATE), 
# configuration file (CFG), and after activating the conda environment
# (CONDA_ENV); these three parameters are exported from osa-env.sh 
# --------------------------------------------------------------------

obsdate=$(date +\%Y\%m\%d -d yesterday)

LOGDIR="${LSTN1}/OSA/Sequencer_log"
LOGFILE="${LOGDIR}/${OBS_DATE}_1_LST1.log"

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
# Run SEQUENCER up to DL1A
# -------------------------
{
    sequencer \
	-c "$CFG" \
	--no-dl1ab \
	-d "$OBS_DATE" LST1 

} >> "$LOGFILE" 2>&1
