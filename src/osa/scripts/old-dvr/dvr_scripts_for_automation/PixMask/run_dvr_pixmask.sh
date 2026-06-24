#!/bin/bash

while read p; do
  echo "#!/bin/bash" > dvr_pixmask.sh
  echo "lstchain_dvr_pixselector --action create_pixel_masks -f \"$p\"" >> dvr_pixmask.sh
  sbatch -A osa -p long --mem-per-cpu=10G dvr_pixmask.sh
done < all_runs.txt
