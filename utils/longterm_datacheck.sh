#!/bin/bash

#SBATCH -A dpps                                                                                                                                                                                                                               
#SBATCH -p short                                                                                                        
#SBATCH --cpus-per-task=1                                                                                               
#SBATCH --mem-per-cpu=2G                                                                                                
#SBATCH -D=/fefs/aswg/data/real/OSA/DL1DataCheck_LongTerm                                                                                                
#SBATCH -o slurm_longterm_datacheck_%j.out                                                                              
#SBATCH -e slurm_longterm_datacheck_%j.err

# Usage: $sbatch longterm_datacheck.sh

OUTPUT_DIR="/fefs/aswg/data/real/OSA/DL1DataCheck_LongTerm"
mkdir -p $OUTPUT_DIR
WORK_DIR="/tmp/OSA/${SLURM_JOBID}"
mkdir -p $WORK_DIR
DL1DIR="/fefs/aswg/data/real/DL1"
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
for month in 201911 202001 202002 202006 202007 202008 202009 202010 202011
do
    cp $DL1DIR/${month}*/v0.6.[13]_v05/datacheck_dl1_LST-1.Run?????.h5 $WORK_DIR/.
    cp $DL1DIR/${month}*/v0.6.[13]_v05/muons_LST-1.Run*.fits $WORK_DIR/.
done
echo End copy: `date +%FT%T`

# Run long term within tmp directory
cd $WORK_DIR
srun python /fefs/aswg/software/virtual_env/ctasoft/cta-lstchain/lstchain/scripts/longterm_dl1_check.py

cd $CURRENT_DIRECTORY
# Copy outcome to final destination
cp $WORK_DIR/longterm_dl1_check.* $OUTPUT_DIR/.

# Copy to datacheck webserver
scp $OUTPUT_DIR/longterm_dl1_check.{h5,html} datacheck:/home/www/html/datacheck/dl1/.

exit 0
