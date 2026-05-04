#!/usr/bin/bash
#SBATCH --job-name=organize
#SBATCH --time=02:00:00
#SBATCH --output=/fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/test_osa/test_files0/OSA/Organize_log/slurm-%j.out
#SBATCH --error=/fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/test_osa/test_files0/OSA/Organize_log/slurm-%j.err


OBS_DATE=2020-01-17 #ejemplo
source /fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/osa-env.sh

/usr/bin/bash /fefs/aswg/workspace/manuel.martinezherresanchez/lstosa/src/osa/crontab/launchers/launch_organize.sh 
