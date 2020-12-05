#!/bin/sh
#
# Define Yesterday (NGHT0) and today (NIGHT1)
#
NIGHT0=$(date +"%Y%m%d" -d "yesterday")
NIGHT1=$(date +"%Y%m%d")
#
#-Basic directories
#
export osa=/fefs/aswg/lstosa
export real=/fefs/aswg/data/real
export dl1=$real/DL1
export dl2=$real/DL2
export ra=$real/running_analysis
export running=$real/running_analysis
export cali=$real/calibration
#
#-Concrete yesterday and today directories
#
#export dl1y="$dl1/$NIGHT0"
#export dl1t="$dl1/$NIGHT1"
#export ray="$ra/$NIGHT0"
#export rat="$ra/$NIGHT0"
#export dl2y="$dl2/$NIGHT0"
#export dl2t="$del2/$NIGHT1"
#export caliy="$cali/$NIGHT0"
#export calit="$cali/$NIGHT1"


