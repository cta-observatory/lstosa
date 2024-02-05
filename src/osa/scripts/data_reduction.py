"""Script to run the gain selection over a list of dates."""
import logging
import glob
import os
import subprocess as sp
from pathlib import Path
from textwrap import dedent

import click
from astropy.table import Table

from osa.scripts.reprocessing import get_list_of_dates, check_job_status_and_wait
from osa.utils.utils import wait_for_daytime
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

PATH = "PATH=/fefs/aswg/lstosa/DVR/offline_data_volume_reduction_v0.2.1/build/:$PATH"
LD_LIBRARY_PATH = "LD_LIBRARY_PATH=/usr/lib64/"

#PART 1: we generate the .sh pointing out the main indications for SLURM, introducing the instructions of the function to be executed and that calculates the time it takes to execute this job

def get_sbatch_script(
    run_id, log_dir, name_job,i
):
    """Build the sbatch job pilot script for running the pixel selection."""
    sbatch_part = dedent(
        f"""\
    #!/bin/bash

    #SBATCH -D {log_dir}
    #SBATCH --export {PATH},{LD_LIBRARY_PATH}
    #SBATCH -p long
    """)
    if name_job:
        sbatch_part+=dedent(
                 f"""\
            #SBATCH -o "pixel_selection_{run_id:05d}_%j.log"
            #SBATCH --job-name "pixel_selection_{run_id:05d}"
            """
        )
    else:
        sbatch_part += dedent(
            f"""\
            #SBATCH -o "pixel_selection_{run_id:05d}_{i}_%j.log"
            #SBATCH --job-name "pixel_selection_{run_id:05d}_{i}"
            """
        )
    sbatch_part+= dedent(
            """\
    echo $PATH
    echo $LD_LIBRARY_PATH
    echo " Hostname : " 
    /usr/bin/hostname
    echo " "
    n_subruns=0
    total_time=0
    """)

    return sbatch_part

def get_sbatch_instruction(
    run_id, log_dir, input_file, output_dir, pixelmap_file
):
    return dedent(
        f"""\             
    start=$(/usr/bin/date '+%H:%M:%S')
    echo $start
    lst_dvr {input_file} {output_dir} {pixelmap_file}
    end=$(/usr/bin/date '+%H:%M:%S')
    echo $end
    subruntime=$(($(/usr/bin/date -d "$end" +%s) - $(/usr/bin/date -d "$start" +%s)))
    echo $subruntime
    n_subruns=$((n_subruns + 1))
    total_time=$((total_time + subruntime))
    """
    )
def get_sbatch_time():
    return dedent(
        """\            
    time_aprox=$((total_time / n_subruns))
    echo $time_aprox
    """
    )

#PART 2:In this function check that pixel_mask exist to write the job (file.sh this is the job_file to launch)

def drafts_job_file(original_dir,output_dir,log_dir,name_job,first_subrun,run_id,subrun,write_job_file,job_file,i):
    #checks if the pixel file exists 
    new_file = Path(f"{original_dir}/LST-1.1.Run{run_id:05d}.{subrun:04d}.fits.fz")
    pixel_file = Path(f"/fefs/aswg/data/real/auxiliary/DataVolumeReduction/PixelMasks/Pixel_selection_LST-1.Run{run_id:05d}.{subrun:04d}.h5")

    if not os.path.exists(pixel_file):
             pixel_file = Path(f"/fefs/aswg/data/real/auxiliary/DataVolumeReduction/PixelMasks/recreated/Pixel_selection_LST-1.Run{run_id:05d}.{subrun:04d}.h5")
             if not os.path.exists(pixel_file):
                 all_streams=original_dir.glob(f"LST-1.?.Run{run_id:05d}.{subrun:04d}.fits.fz")
                 for all_stream in all_streams:
                    #print(f"Copia este new_file: {all_stream}")
                    sp.run(["cp", all_stream, output_dir])
                    continue  # Skip creating instructions for this subrun
             else:
                 if not write_job_file:
                     write_job_file = True
                 with open(job_file, "a") as f:
                      if subrun == first_subrun :  # Only write instructions for the first subrun of the run
                          f.write(get_sbatch_script(run_id, log_dir,name_job,i))
                      f.write(
                          get_sbatch_instruction(
                            run_id,
                            log_dir,
                            new_file,
                            output_dir,
                            pixel_file
                           )
                          )
  

    else:
             if not write_job_file:
                 write_job_file = True
             with open(job_file, "a") as f:
                  if subrun == first_subrun:  # Only write instructions for the first subrun of the run
                      f.write(get_sbatch_script(run_id, log_dir,name_job,i))
                  f.write(
                      get_sbatch_instruction(
                        run_id,
                        log_dir,
                        new_file,
                        output_dir,
                        pixel_file
                       )
                      )
   
    if write_job_file:
            with open(job_file, "a") as f:
                f.write(get_sbatch_time())

#PART 3: In this function apply pixel_selection for files which have got pixel_mask(only data_runs have pixel mask!!!) and copy for files that haven't got it. So for those that are reduced we call the function that writes the job to check if the files exist(drafts_job_file), and write the sh with the 3 functions of part 1.
#def apply_pixel_selection(date: str):
def apply_pixel_selection(date):
    """
    Submit the jobs to apply the pixel selection to the data for a given date
    on a run-by-run basis.
    """

    run_summary_dir = Path("/fefs/aswg/data/real/monitoring/RunSummary")
    run_summary_file = run_summary_dir / f"RunSummary_{date}.ecsv"
    summary_table = Table.read(run_summary_file)
    # Apply pixel selection only to DATA runs
    data_runs = summary_table[summary_table["run_type"] == "DATA"]
    output_basedir = Path("/fefs/aswg/data/real/R0V")
    output_dir = output_basedir / date
    log_dir = output_basedir / "log" / date
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    original_dir = Path(f"/fefs/aswg/data/real/R0G/{date}")
    if not os.path.exists(original_dir):
            original_dir= Path (f"/fefs/aswg/data/real/R0/{date}")
#    d_run = data_runs[data_runs["run_id"] == run]
#    print(d_run)
#    for run in d_run:
    for run in data_runs:
        # Check slurm queue status and sleep for a while to avoid overwhelming the queue
        #check_job_status_and_wait(max_jobs=1500)
        # Avoid running jobs while it is still night time
        #wait_for_daytime(start=12, end=18)

        run_id = run["run_id"]
        files = glob.glob(f"{original_dir}/LST-1.?.Run{run_id:05d}.????.fits.fz")
        subrun_numbers = [int(file[-12:-8]) for file in files]
        run=int(run_id)
        n_subruns = max(subrun_numbers)
        write_job_file = False
        #check the number of subruns, because if it is more than 200 we split the run into many jobs
        if n_subruns>=190:
            group_size = 100
            i=0
            for start_subrun in range(0, n_subruns+1, group_size):
                end_subrun = min(start_subrun + group_size, n_subruns+1)
                i=i+1
        
                job_file = log_dir / f"dvr_reduction_{run:05d}_{start_subrun}-{end_subrun}.sh"
                first_subrun=start_subrun
                for subrun in range(start_subrun, end_subrun):
                    name_job=False
                    job = drafts_job_file(original_dir, output_dir, log_dir, name_job,first_subrun,run_id, subrun,write_job_file, job_file,i)
                
                if os.path.exists(job_file):
                    #print(f"se va a lanzar el siguiente job{job_file}")
                    sp.run(["sbatch", job_file], check=True)

        else:
            job_file_2 = log_dir / f"dvr_reduction_{run:05d}.sh"
            first_subrun=0
            i=0
            for subrun in range (n_subruns +1):
                  name_job=True
                  job3=drafts_job_file(original_dir,output_dir,log_dir,name_job,first_subrun,run_id,subrun,write_job_file,job_file_2,i)

            if os.path.exists(job_file_2):
                  #print(f"se va a lanzar el siguiente job{job_file_2}")
                  sp.run(["sbatch", job_file_2], check=True)

    #the calibration files won't reduced
    calib_runs = summary_table[summary_table["run_type"] != "DATA"]

    for run in calib_runs:
        # Avoid copying files while it is still night time
        #wait_for_daytime(start=12, end=18)

        run_id = run["run_id"]
        r0_files = original_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

        for file in r0_files:
            #print(f"copia este archivo: {file}")
            sp.run(["cp", file, output_dir])

@click.command()
@click.argument("dates-file", type=click.Path(exists=True, path_type=Path))
def main(dates_file: Path = None):
    """
    Loop over the dates listed in the input file and launch the gain selection
    script for each of them. The input file should list the dates in the format
    YYYYMMDD one date per line.
    """
    log.setLevel(logging.INFO)

    list_of_dates = get_list_of_dates(dates_file)
#    with open('DVR/PixelSel/time_limit.csv', mode='r', newline='') as file:
#        reader = csv.reader(file)
#        header = next(reader)
#        for row in reader:
#            date = row[0]
#            run = int(row[1]) 
    for date in list_of_dates:
            print (date)
            apply_pixel_selection(date)
    log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
