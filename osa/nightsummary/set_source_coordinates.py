"""Script to update the source coordinates in the RunCatalog ECSV files."""

import logging
from pathlib import Path

import click
import numpy as np
from astropy.coordinates import SkyCoord, name_resolve
from astropy.table import Table

from osa.utils.logging import myLogger

log = myLogger(logging.getLogger(__name__))


@click.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--source", type=str)
@click.option("--ra", type=float, help="Right Ascension in degrees")
@click.option("--dec", type=float, help="Declination in degrees")
def main(file: Path = None, source: str = None, ra: float = np.nan, dec: float = np.nan):
    """Update the source coordinates in the RunCatalog ECSV files."""
    log.setLevel(logging.INFO)

    table = Table.read(file)
    table.add_index("run_id")

    for run in table:
        source_name = run["source_name"]
        try:
            coords = SkyCoord.from_name(source_name)
            run["source_ra"] = coords.ra.deg
            run["source_dec"] = coords.dec.deg

        except name_resolve.NameResolveError:
            if source is None:
                log.warning(
                    f"Could not resolve coordinates for {source_name}. "
                    "Add coordinates through the command line."
                )
            if source_name == source:
                run["source_ra"] = ra
                run["source_dec"] = dec

    log.info(f"Updated coordinates in {file}")
    table.write(file, format="ascii.ecsv", overwrite=True, delimiter=",")


if __name__ == "__main__":
    main()
