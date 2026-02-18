#!/bin/bash

# --------------------------------------------------------------------
# Launch the closer without forcing it (no -f option). It checks first
# tailcut directories in running_analysis; if there is a tailcut, it
# means that already Sequencer 2 was executed, and we start launching
# Autocloser.
# --------------------------------------------------------------------

obsdate=$(date +\%Y\%m\%d -d yesterday)
LOGDIR="${LSTN1}/OSA/Autocloser_log"
LOGFILE="${LOGDIR}/${OBS_DATE}_LST1.log"

# -------------------------
# Check tailcuts directory
# -------------------------
not_exists()
{
  [ ! -e "$1" ]
}

TARGET_DIR="${LSTN1}/running_analysis/${obsdate}/v*/tailcut*"
if not_exists $TARGET_DIR ; then
    echo "No tailcut directory for ${OBS_DATE} yet" >> "$LOGFILE"
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
	-d "$OBS_DATE" LST1

} >> "$LOGFILE" 2>&1
