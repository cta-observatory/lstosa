"""
Functions to compute significance and build the theta2 plots for each source.

Based on Explore_DL2_data.ipynb from LST-1 analysis 2022 school.
"""

import logging
import subprocess
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import astropy
import astropy.units as u
import click
import numpy as np
import pandas as pd
from ctapipe.containers import EventType
from gammapy.stats import WStatCountsStatistic
from lstchain.io.io import dl2_params_lstcam_key
from lstchain.reco.utils import (
    get_effective_time, extract_source_position, compute_theta2
)
from matplotlib import pyplot as plt

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import extractruns, extractsequences, extractsubruns
from osa.nightsummary.nightsummary import run_summary_table
from osa.utils.cliopts import (
    set_default_directory_if_needed,
    get_prod_id,
    get_dl2_prod_id
)
from osa.utils.logging import myLogger
from osa.utils.utils import DATACHECK_BASEDIR
from osa.utils.utils import lstdate_to_dir

log = myLogger(logging.getLogger(__name__))

DEFAULT_CFG = Path(__file__).parent / '../../cfg/sequencer.cfg'

THETA2_GLOBAL_CUT = 0.04
THETA2_RANGE = (0, 1)
NORM_RANGE_THETA2_MIN = 0.5
NORM_RANGE_THETA2_MAX = 1

mpl_linewidth = 1.6
mpl_rc = {
    "figure.autolayout": True,
    "figure.dpi": 150,
    "font.size": 12,
    "lines.linewidth": mpl_linewidth,
    "axes.grid": True,
    "grid.linestyle": ":",
    "grid.linewidth": 1.4,
    "axes.linewidth": mpl_linewidth,
    "xtick.major.size": 7,
    "xtick.major.width": mpl_linewidth,
    "xtick.minor.size": 4,
    "xtick.minor.width": mpl_linewidth,
    "xtick.minor.visible": False,
    "ytick.major.size": 7,
    "ytick.major.width": mpl_linewidth,
    "ytick.minor.size": 4,
    "ytick.minor.width": mpl_linewidth,
    "ytick.minor.visible": False,
}
plt.style.use(mpl_rc)


def list_of_source(date_obs: str) -> dict:
    """
    Get the list of sources from the sequences' information.

    Parameters
    ----------
    date_obs : str

    Returns
    -------
    sources : Dict[str, list]
    """

    # Build the sequences
    summary_table = run_summary_table(date_obs)
    subrun_list = extractsubruns(summary_table)
    run_list = extractruns(subrun_list)
    sequence_list = extractsequences(run_list)

    # Create a dictionary of sources and their corresponding sequences
    source_dict = {sequence.run: sequence.source_name for sequence in sequence_list}

    source_dict_grouped = defaultdict(list)
    for key, val in sorted(source_dict.items()):
        source_dict_grouped[val].append(key)

    return dict(source_dict_grouped)


def create_hist(theta2_on, theta2_off):
    nbins = round((THETA2_RANGE[1] / THETA2_GLOBAL_CUT) * 2)
    hist_on, bin_edges_on = np.histogram(
        theta2_on, density=False, bins=nbins, range=THETA2_RANGE
    )
    hist_off, bin_edges_off = np.histogram(
        theta2_off, density=False, bins=nbins, range=THETA2_RANGE
    )

    bin_width = bin_edges_on[1] - bin_edges_off[0]
    bin_center = bin_edges_on[:-1] + (bin_width / 2)

    return hist_on, hist_off, bin_edges_on, bin_edges_off, bin_center


def lima_significance(hist_on, hist_off, bin_edges_on, bin_edges_off, eff_time):
    N_on = np.sum(hist_on[bin_edges_on[1:] <= THETA2_GLOBAL_CUT])
    N_off = np.sum(hist_off[bin_edges_off[1:] <= THETA2_GLOBAL_CUT])

    idx_min = (np.abs(bin_edges_on - NORM_RANGE_THETA2_MIN)).argmin()
    idx_max = (np.abs(bin_edges_on - NORM_RANGE_THETA2_MAX)).argmin()

    Non_norm = np.sum(hist_on[idx_min:idx_max])
    Noff_norm = np.sum(hist_off[idx_min:idx_max])

    alpha = Noff_norm / Non_norm

    stat = WStatCountsStatistic(n_on=N_on, n_off=N_off, alpha=alpha)
    significance_lima = stat.sqrt_ts

    text_statistics = (
        f'N$_{{\\rm on}}$ = {N_on:.0f}\n'
        f'N$_{{\\rm off}}$ = {N_off:.0f}\n'
        f'Livetime = {eff_time.to(u.h):.1f}\n'
        f'Li&Ma significance = {significance_lima:.1f} $\\sigma$'
    )
    log.info(text_statistics)

    box_color = 'yellow' if significance_lima > 5 else 'white'

    return text_statistics, box_color


def select_data(data):

    t_effective, _ = get_effective_time(data)
    gammaness = np.array(data.gammaness)
    leakage_intensity_width_2 = np.array(data.leakage_intensity_width_2)
    intensity = np.array(data.intensity)
    wl = np.array(data.wl)
    event_type = np.array(data.event_type)
    t_effective.to(u.min)

    # Cuts. TODO: Use a config file.
    gammaness_cut = 0.7
    intensity_cut = 50
    wl_cut = 0.0
    log.info(
        f'Gammaness cut: {gammaness_cut}\n'
        f'Intensity cut: {intensity_cut}\n'
        f'Width/Length cut: {wl_cut}'
    )

    # Mask for data selection
    condition = (
        (gammaness > gammaness_cut) &
        (intensity_cut < intensity) &
        (wl > wl_cut) &
        (event_type != EventType.FLATFIELD.value) &
        (event_type != EventType.SKY_PEDESTAL.value) &
        (leakage_intensity_width_2 < 0.2)
    )
    return data[condition]


def plot_theta2(
        bin_center,
        hist_on,
        hist_off,
        legend_text,
        box_color,
        source_name,
        date_obs,
        runs,
        highlevel_dir,
):
    fig, ax = plt.subplots()

    ax.errorbar(
        bin_center,
        hist_on,
        yerr=np.sqrt(hist_on),
        fmt='o',
        label='ON data',
    )
    ax.errorbar(
        bin_center,
        hist_off,
        yerr=np.sqrt(hist_off),
        fmt='s',
        label='Background',
    )
    ax.set_xlim(0, 0.5)
    ax.axvline(THETA2_GLOBAL_CUT, color='black', ls='--', alpha=0.75)
    ax.set_xlabel("$\\theta^{2}$ [deg$^{2}$]")
    ax.set_ylabel("Counts")
    ax.legend(
        title=legend_text, facecolor=box_color, loc='upper right'
    )._legend_box.align='left'
    ax.set_title(
        f"Source: {source_name}. Date: {date_obs.strftime('%Y-%m-%d')}\n Runs: {runs}"
    )
    plot_path = (
        highlevel_dir / f"Theta2_{source_name}_{date_obs.strftime('%Y-%m-%d')}.png"
    )
    plt.savefig(plot_path, dpi=300)
    return plot_path


@click.command()
@click.argument('telescope', type=click.Choice(['LST1', 'LST2']))
@click.option(
    '-d',
    '--date-obs',
    type=click.DateTime(formats=["%Y_%m_%d"]),
    default=(date.today() - timedelta(days=1)).strftime("%Y_%m_%d")
)
@click.option(
    '-c', '--config',
    type=click.Path(dir_okay=False),
    default=DEFAULT_CFG,
    help='Read option defaults from the specified cfg file',
)
def main(date_obs, telescope, config):

    log.setLevel(logging.INFO)

    # Initial setup of global parameters
    options.date = date_obs.strftime('%Y_%m_%d')
    night_dir = lstdate_to_dir(options.date)
    options.tel_id = telescope
    options.prod_id = get_prod_id()
    options.dl2_prod_id = get_dl2_prod_id()
    options.directory = set_default_directory_if_needed()
    dl2_directory = Path(cfg.get('LST1', 'DL2_DIR'))
    highlevel_directory = (
        Path(cfg.get('LST1', 'HIGHLEVEL_DIR')) / night_dir / options.prod_id
    )
    highlevel_directory.mkdir(parents=True, exist_ok=True)
    host = cfg.get('WEBSERVER', 'HOST')

    # Create high-level directory in the webserver
    dest_directory = (
        DATACHECK_BASEDIR /
        "high_level" /
        options.prod_id /
        date_obs.strftime('%Y-%m-%d')
    )
    cmd = ["ssh", host, "mkdir", "-p", dest_directory]
    subprocess.run(cmd, capture_output=True, check=True)

    sources = list_of_source(options.date)
    log.info(f"Sources: {sources}")

    for source in sources:
        if source is not None:
            data = pd.DataFrame()
            runs = sources[source]
            log.info(f"Source: {source}, runs: {runs}")

            for run in runs:
                input_file = (
                    dl2_directory / night_dir / options.prod_id / options.dl2_prod_id /
                    f"dl2_LST-1.Run{run:05d}.h5"
                )
                data = pd.concat([data, pd.read_hdf(input_file, key=dl2_params_lstcam_key)])

            selected_events = select_data(data)

            try:
                true_source_position = extract_source_position(
                    data=selected_events,
                    observed_source_name=source
                )
                off_source_position = [element * -1 for element in true_source_position]

                theta2_on = np.array(compute_theta2(selected_events, true_source_position))
                theta2_off = np.array(compute_theta2(selected_events, off_source_position))

                hist_on, hist_off, bin_edges_on, bin_edges_off, bin_center = create_hist(
                    theta2_on, theta2_off
                )
                text, box_color = lima_significance(
                    hist_on=hist_on,
                    hist_off=hist_off,
                    bin_edges_on=bin_edges_on,
                    bin_edges_off=bin_edges_off,
                    eff_time=get_effective_time(data)[0],
                )
                pdf_file = plot_theta2(
                    bin_center=bin_center,
                    hist_on=hist_on,
                    hist_off=hist_off,
                    legend_text=text,
                    box_color=box_color,
                    source_name=source,
                    date_obs=date_obs,
                    runs=runs,
                    highlevel_dir=highlevel_directory,
                )
                cmd = ["scp", pdf_file, f"{host}:{dest_directory}/."]
                subprocess.run(cmd, capture_output=True, check=True)

            except astropy.coordinates.name_resolve.NameResolveError:
                log.warning(f"Source {source} not found in the catalog. Skipping.")
                # TODO: get ra/dec from the TCU database instead


if __name__ == "__main__":
    main()
