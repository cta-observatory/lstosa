"""
Functions to compute significance and build the theta2 plots for each source.

Based on Explore_DL2_data.ipynb from LST-1 analysis 2022 school.
"""

import logging
import subprocess
from pathlib import Path

import astropy
import astropy.units as u
import click
import numpy as np
import pandas as pd
import toml
from ctapipe.containers import EventType
from gammapy.stats import WStatCountsStatistic
from lstchain.io.io import dl2_params_lstcam_key
from lstchain.reco.utils import (
    get_effective_time, extract_source_position, compute_theta2
)
from matplotlib import pyplot as plt

from osa.configs import options
from osa.configs.config import cfg
from osa.nightsummary.extract import list_of_runs_and_sources
from osa.paths import DATACHECK_WEB_BASEDIR, DEFAULT_CFG
from osa.utils.cliopts import (
    set_default_directory_if_needed,
    get_prod_id,
    get_dl2_prod_id
)
from osa.utils.logging import myLogger
from osa.utils.utils import lstdate_to_dir, YESTERDAY

__all__ = [
    'create_hist',
    'lima_significance',
    'event_selection',
    'plot_theta2',
]

log = myLogger(logging.getLogger(__name__))

SELECTION_CUTS_FILE = Path(__file__).parent / 'selection_cuts.toml'

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


def create_hist(theta2_on, theta2_off, theta2_config: dict):
    nbins = round((theta2_config["range"][1] / theta2_config["global_cut"]) * 2)
    hist_on, bin_edges_on = np.histogram(
        theta2_on, density=False, bins=nbins, range=tuple(theta2_config["range"])
    )
    hist_off, bin_edges_off = np.histogram(
        theta2_off, density=False, bins=nbins, range=tuple(theta2_config["range"])
    )

    bin_width = bin_edges_on[1] - bin_edges_off[0]
    bin_center = bin_edges_on[:-1] + (bin_width / 2)

    return hist_on, hist_off, bin_edges_on, bin_edges_off, bin_center


def lima_significance(
        hist_on,
        hist_off,
        bin_edges_on,
        bin_edges_off,
        eff_time,
        theta2_config: dict,
):
    N_on = np.sum(hist_on[bin_edges_on[1:] <= theta2_config["global_cut"]])
    N_off = np.sum(hist_off[bin_edges_off[1:] <= theta2_config["global_cut"]])

    idx_min = (np.abs(bin_edges_on - theta2_config["norm_range_min"])).argmin()
    idx_max = (np.abs(bin_edges_on - theta2_config["norm_range_max"])).argmin()

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


def event_selection(
        data: pd.DataFrame,
        selection_cuts: dict
) -> pd.DataFrame:
    """Return the dataframe with the selected events."""
    t_effective, _ = get_effective_time(data)
    gammaness = np.array(data.gammaness)
    leakage_intensity_width_2 = np.array(data.leakage_intensity_width_2)
    intensity = np.array(data.intensity)
    wl = np.array(data.wl)
    event_type = np.array(data.event_type)
    t_effective.to(u.min)

    log.info(
        f'Gammaness cut: {selection_cuts["gammaness"]}\n'
        f'Intensity cut: {selection_cuts["intensity"]}\n'
        f'Width/Length cut: {selection_cuts["wl"]}'
    )
    # Mask for event selection
    condition = (
        (gammaness > selection_cuts["gammaness"]) &
        (intensity > selection_cuts["intensity"]) &
        (wl > selection_cuts["wl"]) &
        (event_type != EventType.FLATFIELD.value) &
        (event_type != EventType.SKY_PEDESTAL.value) &
        (leakage_intensity_width_2 < selection_cuts["leakage_intensity_width_2"])
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
        theta2_config
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
    ax.axvline(theta2_config["global_cut"], color='black', ls='--', alpha=0.75)
    ax.set_xlabel("$\\theta^{2}$ [deg$^{2}$]")
    ax.set_ylabel("Counts")
    ax.legend(
        title=legend_text, facecolor=box_color, loc='upper right'
    )._legend_box.align = 'left'
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
    default=YESTERDAY.strftime("%Y_%m_%d")
)
@click.option(
    '-c', '--config',
    type=click.Path(dir_okay=False),
    default=DEFAULT_CFG,
    help='Read option defaults from the specified cfg file',
)
def main(
        date_obs,
        telescope="LST1",
        config=DEFAULT_CFG,
):

    log.setLevel(logging.INFO)

    log.info(f"Config: {config}")

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
    config = toml.load(SELECTION_CUTS_FILE)

    # Create high-level directory in the webserver
    dest_directory = (
        DATACHECK_WEB_BASEDIR /
        "high_level" /
        options.prod_id /
        date_obs.strftime('%Y-%m-%d')
    )
    cmd = ["ssh", host, "mkdir", "-p", dest_directory]
    subprocess.run(cmd, capture_output=True, check=True)

    sources = list_of_runs_and_sources(options.date)
    log.info(f"Sources: {sources}")

    for source in sources:
        if source is not None:
            df = pd.DataFrame()
            runs = sources[source]
            log.info(f"Source: {source}, runs: {runs}")

            for run in runs:
                input_file = (
                    dl2_directory / night_dir / options.prod_id / options.dl2_prod_id /
                    f"dl2_LST-1.Run{run:05d}.h5"
                )
                df = pd.concat(
                    [df, pd.read_hdf(input_file, key=dl2_params_lstcam_key)]
                )

            selected_events = event_selection(data=df, selection_cuts=config["cuts"])

            try:
                true_source_position = extract_source_position(
                    data=selected_events,
                    observed_source_name=source
                )
                off_source_position = [element * -1 for element in true_source_position]

                theta2_on = np.array(
                    compute_theta2(selected_events, true_source_position)
                )
                theta2_off = np.array(
                    compute_theta2(selected_events, off_source_position)
                )

                hist_on, hist_off, bin_edges_on, bin_edges_off, bin_center = create_hist(
                    theta2_on, theta2_off, config["theta2"]
                )
                text, box_color = lima_significance(
                    hist_on=hist_on,
                    hist_off=hist_off,
                    bin_edges_on=bin_edges_on,
                    bin_edges_off=bin_edges_off,
                    eff_time=get_effective_time(df)[0],
                    theta2_config=config["theta2"]
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
                    theta2_config=config["theta2"]
                )
                cmd = ["scp", pdf_file, f"{host}:{dest_directory}/."]
                subprocess.run(cmd, capture_output=True, check=True)

            except astropy.coordinates.name_resolve.NameResolveError:
                log.warning(f"Source {source} not found in the catalog. Skipping.")
                # TODO: get ra/dec from the TCU database instead


if __name__ == "__main__":
    main()
