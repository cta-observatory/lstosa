#!/bin/bash
#SBATCH --job-name=organize
#SBATCH --time=02:00:00
#SBATCH --chdir=/fefs/aswg/lstosa/src/osa/crontab
#SBATCH --output=/fefs/aswg/lstosa/src/osa/crontab/log/slurm-%j.out
#SBATCH --error=/fefs/aswg/lstosa/src/osa/crontab/log/slurm-%j.err



#OBS_DATE=2020-01-17 example
source /fefs/aswg/lstosa/src/osa/crontab/osa-env.sh

	
/usr/bin/bash /fefs/aswg/lstosa/src/osa/crontab/launchers/launch_organize.sh
# options: -s --no-gainsel --no-running
