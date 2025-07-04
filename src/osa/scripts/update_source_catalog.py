import logging
import subprocess as sp
from pathlib import Path
from textwrap import dedent
import argparse
import pandas as pd

from astropy import units as u
from astropy.table import Table, join, unique, vstack
from astropy.time import Time
from lstchain.io.io import dl1_params_lstcam_key
from lstchain.reco.utils import add_delta_t_key, get_effective_time

from osa.configs.config import cfg
from osa.utils.cliopts import valid_date
from osa.paths import DEFAULT_CFG, get_major_version, get_dl1_prod_id
from osa.utils.utils import get_lstchain_version, date_to_dir, date_to_iso


pd.set_option("display.float_format", "{:.1f}".format)

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")
log = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
        "-c",                                                                                            
        "--config",                                                                                      
        action="store",
        type=Path,
        default=DEFAULT_CFG,
        help="Configuration file",
)
parser.add_argument(                                                                                     
        "-d",                                                                                            
        "--date",                                                                                        
        default=None,
        type=valid_date,
        help="Night to apply the gain selection in YYYY-MM-DD format",
)
parser.add_argument(
        "-v",
        "--version",
        type=str,
        default=get_lstchain_version()
)

def add_table_to_html(html_table):
    return dedent(
        f"""\
    <html>
      <head>
        <link href="osa.css" rel="stylesheet" type="text/css">
      </head>
    <body>
    {html_table}
    </body>
    </html>
    """
    )


def add_query_table_to_html(html_table):
    return dedent(
        f"""\
    <html>
     <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8">
    <style>
    body {{font-family: sans-serif;}}
    table.dataTable {{width: auto !important; margin: 0 !important;}}
    .dataTables_filter, .dataTables_paginate {{float: left !important; margin-left:1em}}
    </style>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css">
        <script type="text/javascript" language="javascript" src="https://code.jquery.com/jquery-3.5.1.js"></script>
        <script type="text/javascript" language="javascript" src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>

    <script type="text/javascript" class="init">

    $(document).ready(function() {{
        $('#example').DataTable();
    }} );

    </script>

    </head>

     <body>
      <script>
    var astropy_sort_num = function(a, b) {{
        var a_num = parseFloat(a);
        var b_num = parseFloat(b);

        if (isNaN(a_num) && isNaN(b_num))
            return ((a < b) ? -1 : ((a > b) ? 1 : 0));
        else if (!isNaN(a_num) && !isNaN(b_num))
            return ((a_num < b_num) ? -1 : ((a_num > b_num) ? 1 : 0));
        else
            return isNaN(a_num) ? -1 : 1;
    }}

    jQuery.extend( jQuery.fn.dataTableExt.oSort, {{
        "optionalnum-asc": astropy_sort_num,
        "optionalnum-desc": function (a,b) {{ return -astropy_sort_num(a, b); }}
    }});

    $(document).ready(function() {{
        $('#table139855676982704').dataTable({{
            order: [],
            pageLength: 50,
            lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, 'All']],
            pagingType: "full_numbers",
            columnDefs: [{{targets: [0, 1, 2, 4], type: "optionalnum"}}]
        }});
    }} );
    </script>
        {html_table}
        </body>
        </html>
        """
    )


def add_start_and_elapsed(table: Table, datedir: str, version: str) -> None:
    """Add columns with the timestamp of first events and elapsed time of the runs.

    This information is taken from the merged DL1 files. Two new columns are added
    to the input table.

    Parameters
    ----------
    table : astropy.table.Table
        Astropy Table to which the two new columns are to be added.
    datedir : str
        Date directory in YYYYMMDD format.
    version : str
        Production version of the processing in the format 'vW.X.Y.Z'.
    """
    if "run_id" not in table.columns:
        raise KeyError("Run ID not present in given table. Please check its content.")

    start_times = []
    elapsed_times = []

    for run in table["run_id"]:
        major_version = get_major_version(version)
        dl1b_config_file = Path(cfg.get("LST1", "TAILCUTS_FINDER_DIR")) / f"dl1ab_Run{run:05d}.json"
        dl1_prod_id = get_dl1_prod_id(dl1b_config_file)
        dl1_dir = Path(cfg.get("LST1", "DL1_DIR"))
        file = dl1_dir / datedir / major_version / dl1_prod_id / f"dl1_LST-1.Run{run:05d}.h5"
        df = pd.read_hdf(file, key=dl1_params_lstcam_key)

        # Timestamp of the first event
        first_time = Time(df["dragon_time"][0], format="unix", scale="utc")
        start_times.append(first_time.utc.iso)

        # Elapsed time of the run
        df_delta = add_delta_t_key(df)
        _, elapsed_t = get_effective_time(df_delta)
        elapsed_times.append(elapsed_t.to(u.min))

    # Modify the input table by adding two new columns
    table.add_column(elapsed_times, name="Elapsed [min]")
    table.add_column(start_times, name="Run start [UTC]")


def copy_to_webserver(html_file, csv_file):
    sp.run(["scp", str(html_file), "datacheck:/home/www/html/datacheck/lstosa/."], check=True)
    sp.run(["scp", str(csv_file), "datacheck:/home/www/html/datacheck/lstosa/."], check=True)


def main():
    """Update source catalog with new run entries from a given date in format YYYY-MM-DD.

    Notes
    -----
    It needs to be run as lstanalyzer user.
    """
    args = parser.parse_args()

    catalog_path = Path(cfg.get("LST1", "SOURCE_CATALOG")) / "LST_source_catalog.ecsv"
    catalog_table = Table.read(catalog_path)

    # Open table for given date and append its content to the table with entire catalog
    datedir = date_to_dir(args.date)
    run_catalog_dir = Path(cfg.get("LST1", "RUN_CATALOG")) 
    today_catalog = Table.read(run_catalog_dir / f"RunCatalog_{datedir}.ecsv")
    run_summary_dir = Path(cfg.get("LST1", "RUN_SUMMARY_DIR"))
    today_runsummary = Table.read(run_summary_dir / f"RunSummary_{datedir}.ecsv")
    # Keep only astronomical data runs
    today_runsummary = today_runsummary[today_runsummary["run_type"] == "DATA"]
    todays_info = join(today_runsummary, today_catalog)
    todays_info.add_column(date_to_iso(args.date), name="date_dir")
    todays_info.keep_columns(["run_id", "source_name", "date_dir"])

    # Add start of run in iso format and elapsed time for each run
    log.info("Getting run start and elapsed time")
    add_start_and_elapsed(todays_info, datedir, args.version)

    # Change column names
    todays_info.rename_column("run_id", "Run ID")
    todays_info.rename_column("source_name", "Source name")
    todays_info.rename_column("date_dir", "Date directory")

    # Add new rows from given date to the whole catalog table
    log.info("Adding new rows to table")
    new_table = vstack([catalog_table, todays_info])
    table_unique = unique(new_table, keys="Run ID", keep="last")

    # To pandas
    log.info("Converting to pandas and HTML")
    df = table_unique.to_pandas()
    df = df.sort_values(by="Run ID", ascending=False)

    # To HTML
    html_table = df.to_html(index=False, justify="left")
    html_table = html_table.replace(
        '<table border="1" class="dataframe">',
        '<table class="display compact" id="table139855676982704">',
    )
    html_content = add_query_table_to_html(html_table)

    # Save the HTML and ECSV files and copy them to the LST-1 webserver
    html_file = Path(cfg.get("LST1", "SOURCE_CATALOG")) / "LST_source_catalog.html"
    html_file.write_text(html_content)
    table_unique.write(catalog_path, delimiter=",", overwrite=True)

    copy_to_webserver(html_file, catalog_path)


if __name__ == "__main__":
    main()
