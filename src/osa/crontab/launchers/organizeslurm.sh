#!/usr/bin/bash
#SBATCH --job-name=organize
#SBATCH --time=02:00:00
#SBATCH --chdir=/fefs/onsite/data/lst-pipe/LSTN-01/OSA/Organize
#SBATCH --output=log/slurm-%j.out
#SBATCH --error=log/slurm-%j.err


#OBS_DATE=2020-01-17 example
source /local/home/lstanalyzer/osa-env.sh

/usr/bin/bash /fefs/aswg/lstosa/src/osa/crontab/launchers/launch_organize.sh 
# options: -s --no-gainsel --no-running
