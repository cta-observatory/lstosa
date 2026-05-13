#!/bin/bash

# -------------------------
# Load environment
# -------------------------
source /fefs/aswg/lstosa/src/osa/crontab/osa-env.sh

# If OBS_DATE not set, use yesterday
if [ -z "$OBS_DATE" ]; then
    OBS_DATE=$(date -d "yesterday" +%Y-%m-%d)
fi

# Convert YYYY-MM-DD → YYYYMMDD
obsdate=$(date -d "$OBS_DATE" +%Y%m%d)

# -------------------------
# Helpers
# -------------------------
exists() {
    compgen -G "$1" > /dev/null
}

# -------------------------
# Check NightFinished.txt
# -------------------------
if ! exists "${LSTN1}/OSA/Closer/${obsdate}/v*/NightFinished.txt" ; then
    exit
fi


# -------------------------
# DONE FILE (STOP FUTURE RUNS)
# -------------------------
DONE_FILE="${LSTN1}/OSA/Organize/${obsdate}/DONE.txt"

if [ -f "$DONE_FILE" ]; then
    exit
fi






# -------------------------
# Detect simulation mode (-s)
# -------------------------
SIMULATION=false

for arg in "$@"; do
    if [ "$arg" = "-s" ]; then
        SIMULATION=true
    fi
done

# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run Python script
# -------------------------
organize \
    -c "$CFG" \
    -d "$obsdate" \
    "$@"
    
status=$?

echo "STATUS=$status"

if [ $status -eq 0 ] && [ "$SIMULATION" = false ]; then
    mkdir -p "$(dirname "$DONE_FILE")"
    touch "$DONE_FILE"
fi

exit $status
