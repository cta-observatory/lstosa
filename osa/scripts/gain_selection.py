#!/usr/bin/env python

"""Script to run the gain selection over a list of dates."""

import logging
import subprocess as sp
from pathlib import Path
from astropy.io import ascii
import glob

import click

from osa.scripts.reprocessing import get_list_of_dates
from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))

def apply_gain_selection(date: str):
    run_summary_file = "/fefs/aswg/data/real/monitoring/RunSummary/RunSummary_"+date+".ecsv"
    data = ascii.read(run_summary_file)
    data.add_index("run_id")
    data = data[(data['run_type']=='DATA')]   # apply gain selection only to DATA runs
    
    output_dir = "/fefs/aswg/data/real/R0/gain_selected/"+date
    
    for run in data["run_id"]:
        
        ref_time = data.loc[run]["dragon_reference_time"]
        ref_counter = data.loc[run]["dragon_reference_counter"]
        module = data.loc[run]["dragon_reference_module_index"]
        ref_source = data.loc[run]["dragon_reference_source"]
        
        input_files = glob.glob("/fefs/aswg/data/real/R0/"+date+"/LST-1.1.Run0"+str(run)+".????.fits.fz")
        
        for file in input_files:
            
            log.info("Applying gain selection to file: {}".format(file))
            
            sbatch_cmd = [
                "sbatch",
                "--mem=10GB",
                "--job-name=gain_selection",
                "-D",
                output_dir,
                "--parsable",
            ]
            
            cmd_select_gain = [ 
                "lst_select_gain", 
                file, 
                output_dir, 
                str(ref_time), 
                str(ref_counter), 
                str(module), 
                str(ref_source).upper()]
            
            cmd = sbatch_cmd+cmd_select_gain
            
            sp.run(cmd_select_gain)

@click.command()
@click.argument('dates-file', type=click.Path(exists=True))   

def main(dates_file: Path):
    """
    Loop over the dates listed in the input file and launch the gain selection script for each of them.
    The input file should list the dates in the format YYYYMMDD one date per line.
    """
    logging.basicConfig(level=logging.INFO)

    list_of_dates = get_list_of_dates(dates_file)

    for date in list_of_dates:
        apply_gain_selection(date)

    log.info("Done! No more dates to process.")


if __name__ == "__main__":
    main()
