#!/bin/bash

# --------------------------------------------------------------------
# Launch the closer without forcing it (no -f option). It checks first
# tailcut directories in running_analysis; if there is a tailcut, it
# means that already Sequencer 2 was executed, and we start launching
# Autocloser.
# --------------------------------------------------------------------

# Export parameters from osa-env.sh
source /fefs/aswg/workspace/maria.rivero/lstosa/src/osa/crontab/osa-env.sh

# Convert YYYY-MM-DD to YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

LOGDIR="${LSTN1}/OSA/Autocloser_log"
LOGFILE="${LOGDIR}/${OBS_DATE}_LST1.log"

# -------------------------
# Check tailcuts directory
# -------------------------
not_exists() {
    ! compgen -G "$1" > /dev/null
}

if not_exists "${LSTN1}/running_analysis/${obsdate}/v*/tailcut*" ; then
    echo "No tailcut directory for ${OBS_DATE} yet" >> "$LOGFILE"
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
# Run AUTOCLOSER
# -------------------------
{
    autocloser \
	-c "$CFG" \
	-d "$OBS_DATE" LST1 \
	"$@"

} >> "$LOGFILE" 2>&1
