#!/bin/bash

# -------------------------
# Load environment
# -------------------------
source /fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/osa-env.sh

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
if ! exists "/fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/test_osa/test_files0/OSA/Closer/${obsdate}/v*/NightFinished.txt" ; then
    exit
fi

#/fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/test_osa/test_files0/
#${LSTN1}/OSA/Closer/

# -------------------------
# DONE FILE (STOP FUTURE RUNS)
# -------------------------
# Aqui esto tengo que cambiarlo hay que ver donde se mete esto, ademas de que ese directorio no existe
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
python /fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/src/osa/scripts/rutas_organize_def.py \
    -c "$CFG" \
    -d "$obsdate" \
    "$@"

# -------------------------
# Mark as done (ONLY if not simulation)
# -------------------------
DONE_FILE="${LSTN1}/OSA/Organize/${obsdate}/DONE.txt"


if [ "$SIMULATION" = false ]; then
    mkdir -p "$(dirname "$DONE_FILE")"
    touch "$DONE_FILE"
fi
