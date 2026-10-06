#!/bin/bash

# --------------------------------------------------------------------
# Run the DVR (Data Volume Reduction) automated pipeline, for OBS_DATE,
# once the night has been closed by OSA (NightFinished.txt exists).
# --------------------------------------------------------------------


source /local/home/lstanalyzer/osa-env.sh
source "$CONDA_ENV"

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

PROJECT_DIR="/fefs/aswg/lstosa/src/osa/dvr_auto"      # carpeta que contiene config.yaml
LOGDIR="${LSTN1}/OSA/DVR_log"
LOGFILE="${LOGDIR}/${obsdate}_DVR.log"
LOCKFILE="/tmp/dvr_auto_lstanalyzer.lock"

mkdir -p "$LOGDIR"

# -------------------------
# Check the night is closed
# -------------------------
exists() {
    compgen -G "$1" > /dev/null
}

if ! exists "${LSTN1}/OSA/Closer/${obsdate}/v*/NightFinished.txt"; then
    echo "Date ${obsdate} not closed yet for LST1, skipping DVR" >> "$LOGFILE"
    exit
fi

# -------------------------------------------------
# Avoid overlapping runs: DVR can take several hours
# -------------------------------------------------
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "DVR already running, skipping this run (obsdate ${obsdate})" >> "$LOGFILE"
    exit
fi

# -------------------------
# Run DVR pipeline
# -------------------------
cd "$PROJECT_DIR" || { echo "Cannot cd to $PROJECT_DIR" >> "$LOGFILE"; exit 1; }

{
    python -m dvr_auto.cli "$obsdate" "$obsdate" --profile lstanalyzer

}  >> "$LOGFILE" 2>&1
