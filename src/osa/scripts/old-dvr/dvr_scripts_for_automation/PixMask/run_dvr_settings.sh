#!/bin/bash

while read p; do
  echo "#!/bin/bash" > dvr_settings.sh
  echo "lstchain_dvr_pixselector -n 1 -f \"$p\" " >> dvr_settings.sh
  sbatch -A osa -p short --mem-per-cpu=8G dvr_settings.sh
done <all_runs.txt
