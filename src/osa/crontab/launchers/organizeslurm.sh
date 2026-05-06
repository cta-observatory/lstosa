#!/usr/bin/bash
#SBATCH --job-name=organize
#SBATCH --time=02:00:00
#SBATCH --output=/fefs/onsite/data/lst-pipe/LSTN-01/OSA/Organize/log/slurm-%j.out
#SBATCH --error=/fefs/onsite/data/lst-pipe/LSTN-01/OSA/Organize/log/slurm-%j.err


#OBS_DATE=2020-01-17 example
source /local/home/lstanalyzer/osa-env.sh

/usr/bin/bash /fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/src/osa/crontab/launchers/launch_organize.sh 
# options: -s --no-gainsel --no-running
