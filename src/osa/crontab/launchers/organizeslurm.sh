#!/usr/bin/bash

# This launcher is executed directly from cron.
# SLURM `#SBATCH` directives are not used here because the script is not
# submitted via `sbatch`; keep this file as a normal cron launcher.


#OBS_DATE=2020-01-17 example
source /fefs/aswg/lstosa/src/osa/crontab/osa-env.sh

/usr/bin/bash /fefs/aswg/lstosa/src/osa/crontab/launchers/launch_organize.sh 
# options: -s --no-gainsel --no-running
