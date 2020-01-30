#!/bin/env python
#SBATCH -p compute
#SBATCH --tasks=60
#SBATCH --nodes=2
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1600
#SBATCH -t 0-48:00
#SBATCH -o ./log/slurm.%j.%N.out
#SBATCH -e ./log/slurm.%j.%N.err

import sys
import subprocess
#subprocess.call(["print("Hellow world")"])
print("Hellow world")

filename = sys.argv[1] 
outdir = sys.argv[2] 
calibrationfile = sys.argv[3] 
pedetalfile = sys.argv[4] 
drivefile = sys.argv[5] 


print(filename)
print(outdir)
print(calibrationfile)
print(pedetalfile)
print(drivefile)
