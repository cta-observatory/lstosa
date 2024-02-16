"""Script to run the data volume reduction over a list of dates."""
import logging
import glob
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


def get_sbatch_script(
    run_id, log_dir, name_job, i
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
    """Calculate the time it takes to execute the job."""
    return dedent(
        """\            
    time_aprox=$((total_time / n_subruns))
    echo $time_aprox
    """
    )

def drafts_job_file(original_dir,output_dir,log_dir,name_job,first_subrun,run_id,subrun,job_file,i):
    """Check if the pixel_mask file exists and write the job file to be launched.""" 
    new_file = Path(f"{original_dir}/LST-1.1.Run{run_id:05d}.{subrun:04d}.fits.fz")
    pixel_masks_dir = Path("/fefs/aswg/data/real/auxiliary/DataVolumeReduction/PixelMasks/")
    pixel_file = pixel_masks_dir / f"Pixel_selection_LST-1.Run{run_id:05d}.{subrun:04d}.h5"

    if not pixel_file.exists():
        all_streams = original_dir.glob(f"LST-1.?.Run{run_id:05d}.{subrun:04d}.fits.fz")
        for stream in all_streams:
            log.info(f"No PixelMask file found for run {run_id:05d}.{subrun}, \
                copying file {stream} to {output_dir}")
            sp.run(["cp", stream, output_dir]) 
    else:
        with open(job_file, "a") as f:
            if subrun == first_subrun:  # Only write instructions for the first subrun of the run
                f.write(get_sbatch_script(run_id, log_dir,name_job, i))
                f.write(
                  get_sbatch_instruction(
                    run_id,
                    log_dir,
                    new_file,
                    output_dir,
                    pixel_file
                    )
                )
        with open(job_file, "a") as f:
            f.write(get_sbatch_time())

def apply_pixel_selection(date: str, start: int, end: int):
    """
    Submit the jobs to apply the pixel selection to the data for a given date
    on a run-by-run basis. Only data runs have pixel mask files, the rest of
    the files are directly copied without being reduced. 
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

    if not original_dir.exists():
        original_dir = Path (f"/fefs/aswg/data/real/R0/{date}")

    for run in data_runs:
        # Check slurm queue status and sleep for a while to avoid overwhelming the queue
        check_job_status_and_wait(max_jobs=1500)
        # Avoid running jobs while it is still night time
        wait_for_daytime(start, end)

        run_id = run["run_id"]
        files = glob.glob(f"{original_dir}/LST-1.?.Run{run_id:05d}.????.fits.fz")
        subrun_numbers = [int(file[-12:-8]) for file in files]
        run = int(run_id)
        n_subruns = max(subrun_numbers)

        # If the number of subruns is above 190, the run is split into multiple jobs
        if n_subruns>=190:
            group_size = 100
            i = 0
            for start_subrun in range(0, n_subruns+1, group_size):
                end_subrun = min(start_subrun + group_size, n_subruns+1)
                i = i+1
                job_file = log_dir / f"dvr_reduction_{run:05d}_{start_subrun}-{end_subrun}.sh"
                first_subrun = start_subrun
                for subrun in range(start_subrun, end_subrun):
                    name_job = False
                    drafts_job_file(
                        original_dir,
                        output_dir,
                        log_dir,
                        name_job,
                        first_subrun,
                        run_id,
                        subrun,
                        job_file,
                        i
                    )
                
                if job_file.exists():
                    log.info(f"Launching job {job_file}")
                    sp.run(["sbatch", job_file], check=True)
        else:
            job_file = log_dir / f"dvr_reduction_{run:05d}.sh"
            first_subrun = 0
            i = 0
            for subrun in range(n_subruns+1):
                name_job = True
                drafts_job_file(
                    original_dir,
                    output_dir,
                    log_dir,
                    name_job,
                    first_subrun,
                    run_id,
                    subrun,
                    job_file,
                    i
                )

            if job_file.exists():  
                log.info(f"Launching job{job_file}")
                sp.run(["sbatch", job_file], check=True)

    # Non-data files won't be reduced
    calib_runs = summary_table[summary_table["run_type"] != "DATA"]

    for run in calib_runs:
        # Avoid copying files while it is still night time
        wait_for_daytime(start, end)

        run_id = run["run_id"]
        r0_files = original_dir.glob(f"LST-1.?.Run{run_id:05d}.????.fits.fz")

        for file in r0_files:
            log.info(f"Copying {file} to {output_dir}")
            sp.run(["cp", file, output_dir])

@click.command()
@click.argument("dates-file", type=click.Path(exists=True, path_type=Path))
@click.option("-s", "--start-time", type=int, default=10, help="Time to (re)start data reduction in HH format.")
@click.option("-e", "--end-time", type=int, default=18, help="Time to stop data reduction in HH format.")
def main(dates_file: Path = None, start_time: int = 10, end_time: int = 18):
    """
    Loop over the dates listed in the input file and launch the data reduction
    script for each of them. The input file should list the dates in the format
    YYYYMMDD one date per line.
    """
    log.setLevel(logging.INFO)

    list_of_dates = get_list_of_dates(dates_file)
    for date in list_of_dates:
        log.info(f"Applying pixel selection for date {date}")
        apply_pixel_selection(date, start_time, end_time)

    log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
