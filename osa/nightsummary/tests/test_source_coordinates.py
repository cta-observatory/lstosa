from astropy.coordinates import SkyCoord
from astropy.table import Table

from osa.scripts.tests.test_osa_scripts import run_program


def test_source_coordinates(run_catalog):
    assert run_catalog.exists()

    output = run_program(
        'source_coordinates',
        '--source',
        'MadeUpSource',
        '--ra',
        '31.22',
        '--dec',
        '52.95',
        str(run_catalog)
    )

    assert output.returncode == 0
    table = Table.read(run_catalog)

    # Check new coordinates
    crab_coords = SkyCoord.from_name('Crab')
    assert table[table['source_name'] == 'Crab']['source_ra'] == crab_coords.ra.deg
    assert table[table['source_name'] == 'Crab']['source_dec'] == crab_coords.dec.deg
    assert table[table['source_name'] == 'MadeUpSource']['source_ra'] == 31.22
    assert table[table['source_name'] == 'MadeUpSource']['source_dec'] == 52.95
