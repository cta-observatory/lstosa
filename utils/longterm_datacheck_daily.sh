#!/bin/bash

#SBATCH -A dpps                                                                                                                                                                                                                               
#SBATCH -p short                                                                                                        
#SBATCH --cpus-per-task=1                                                                                               
#SBATCH --mem-per-cpu=4G                                                                                                
#SBATCH -D /fefs/aswg/data/real/OSA/DL1DataCheck_LongTerm                                                                                                
#SBATCH -o log/slurm_longterm_datacheck_%j.out                                                                              
#SBATCH -e log/slurm_longterm_datacheck_%j.err

# Usage: $sbatch longterm_datacheck_daily.sh YYYYMMDD

date=$1  # in the format YYYYMMDD
prod_id="v0.7.3"
dl1_prod_id=tailcut84

OUTPUT_DIR="/fefs/aswg/data/real/OSA/DL1DataCheck_LongTerm/v0.7/$date"
mkdir -p $OUTPUT_DIR
WORK_DIR="/tmp/OSA/${SLURM_JOBID}"
mkdir -p $WORK_DIR
DL1DIR="/fefs/aswg/data/real/DL1/$date/$prod_id"
CURRENT_DIRECTORY=`pwd`/

# Check if tmp dir was created
if [[ ! "$WORK_DIR" || ! -d "$WORK_DIR" ]]; then
  echo "Could not create tmp dir"
  exit 1
fi

# Make sure tmp directory gets removed even if the script exits abnormally.
trap "exit 1" HUP INT PIPE QUIT TERM
trap 'rm -rf "$WORK_DIR"' EXIT

echo Start copy: `date +%FT%T`
# Copy files month by month: muons files subrun-wise and dl1 datacheck run-wise
# FIXME: loop over months or directories without having to assume any prior month list.
cp $DL1DIR/$dl1_prod_id/datacheck_dl1_LST-1.Run?????.h5 $WORK_DIR/.
cp $DL1DIR/muons_LST-1.Run*.fits $WORK_DIR/.
echo End copy: `date +%FT%T`

# Run long term within tmp directory
cd $WORK_DIR
python /fefs/aswg/software/virtual_env/ctasoft/cta-lstchain/lstchain/scripts/longterm_dl1_check.py

cd $CURRENT_DIRECTORY
# Copy outcome to final destination
cp $WORK_DIR/longterm_dl1_check.h5 $OUTPUT_DIR/DL1_datacheck_$date.h5
cp $WORK_DIR/longterm_dl1_check.html $OUTPUT_DIR/DL1_datacheck_$date.html

# FIXME: For the moment the copy to datacheck webserver is not working within this script
# It must be done separately.
#scp $OUTPUT_DIR/longterm_dl1_check.{h5,html} datacheck:/home/www/html/datacheck/dl1/.

exit 0
