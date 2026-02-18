#!/bin/bash

# --------------------------------------------------------------------
# Finishes the processing producing DL1AB and DL2
# --------------------------------------------------------------------

obsdate=$(date +\%Y\%m\%d -d yesterday)

LOGDIR="${LSTN1}/OSA/Sequencer_log"
LOGFILE="${LOGDIR}/${OBS_DATE}_2_LST1.log"

#------------OPTION A------------------------------------------------
RUNS_FILE="${LSTN1}/monitoring/RunSummary/RunSummary_${obsdate}.ecsv"
CONFIG_DIR="${LSTN1}/auxiliary/TailCuts"
if [ ! -e "$RUNS_FILE" ] ; then
    echo "No $RUNS_FILE" > "$LOGFILE"
    exit
fi

# Lines to skip in the header of RunSummary files
skip_headers=21
found_file=false
while IFS=, read -r runs _
do
    if ((skip_headers))
    then
        ((skip_headers--))
    else
        # Check if a file containing $runs exists
        if [ -e "${CONFIG_DIR}/dl1ab_Run${runs}.json" ] ; then
            echo "Found file containing $runs" >> "$LOGFILE"
            found_file=true
            break
        fi

    fi
done < "$RUNS_FILE"

if ! $found_file; then
    echo "No matching files found for any runs. Exiting." >> "$LOGFILE"
    exit
fi
# -----------------------------------------------------------------------

#------------OPTION B------------------------------------------------
#not_exists()
#{
#  [ ! -e "$1" ]
#}
#
# Check if tailcut_finder file exists
#TARGET_FILE="${LSTN1}/running_analysis/${obsdate}/v*/log/tailcut*"
#if not_exists $TARGET_FILE ; then
#    echo "No tailcut directory for ${OBS_DATE} yet" >> "$LOGFILE"
#    exit
#fi
#
# Check if it is too recent (less than 5 minutes old)
#DIR_PATH=$(ls -d $TARGET | head -n 1)
#MOD_TIME=$(stat -c %Y "$DIR_PATH")
#NOW=$(date +%s)
#DIFFERENCE=$((NOW - MOD_TIME))
#
#if [ "$DIFFERENCE" -lt 300 ]; then
#    echo "The tailcut directory is too recent ($DIFFERENCE s). Exiting." >> "$LOGFILE"
#    exit
#fi
#--------------------------------------------------------------------

# -------------------------
# Environment
# -------------------------
source "$CONDA_ENV"

# -------------------------
# Run SEQUENCER
# -------------------------
{
    sequencer \
	-c "$CFG" \
	-d "$OBS_DATE" LST1 

} >> "$LOGFILE" 2>&1
