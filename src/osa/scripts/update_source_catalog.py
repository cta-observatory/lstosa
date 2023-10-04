import logging
import subprocess as sp
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import click
import re
import pandas as pd
from astropy import units as u
from astropy.table import Table, join, vstack, unique
from astropy.time import Time
from lstchain.io.io import dl1_params_lstcam_key
from lstchain.reco.utils import get_effective_time, add_delta_t_key
from osa.utils.utils import get_lstchain_version

pd.set_option('display.float_format', '{:.1f}'.format)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)
log = logging.getLogger(__name__)


BASE_DL1 = Path("/fefs/aswg/data/real/DL1")
BASE_MONITORING = Path("/fefs/aswg/data/real/monitoring")


def add_table_to_html(html_table):
    return dedent(f"""\
    <html>
      <head>
        <link href="osa.css" rel="stylesheet" type="text/css">
      </head>
    <body>
    {html_table}
    </body>
    </html>
    """)


def add_query_table_to_html(html_table):
    return dedent(f"""\
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
        """)


def add_run_start_iso(table):
    start_times = []
    for timestamp in table["run_start"]:
        start_time = Time(timestamp * u.ns, format="unix_tai")
        start_times.append(start_time.utc.iso)
    table.replace_column("run_start", start_times)


def add_elapsed(table, datedir, version):
    elapsed_times = []
    for run in table["run_id"]:
        major_version = re.search(r'\D\d+\.\d+', version)[0]
        file = BASE_DL1 / datedir / major_version / f"tailcut84/dl1_LST-1.Run{run:05d}.h5"
        df = pd.read_hdf(file, key=dl1_params_lstcam_key)
        df_delta = add_delta_t_key(df)
        _, elapsed_t = get_effective_time(df_delta)

        elapsed_times.append(elapsed_t.to(u.min))

    table.add_column(elapsed_times, name="Elapsed [min]")


def copy_to_webserver(html_file, csv_file):
    sp.run(["scp", str(html_file), "datacheck:/home/www/html/datacheck/lstosa/."])
    sp.run(["scp", str(csv_file), "datacheck:/home/www/html/datacheck/lstosa/."])


@click.command()
@click.argument(
    'date',
    type=click.DateTime(formats=["%Y-%m-%d"])
)
@click.option("-v", "--version", type=str, default=get_lstchain_version())
def main(date: datetime = None, version: str = get_lstchain_version()):
    """
    Update source catalog with new run entries from a given date in
    format YYYY-MM-DD. It needs to be run as lstanalyzer user.
    """
    csv_file = Path("/fefs/aswg/data/real/OSA/Catalog/LST_source_catalog.ecsv")
    table = Table.read(csv_file)

    # Open today's table and append its content to general table
    datedir = date.strftime("%Y%m%d")
    today_catalog = Table.read(BASE_MONITORING / f"RunCatalog/RunCatalog_{datedir}.ecsv")
    today_runsummary = Table.read(BASE_MONITORING / f"RunSummary/RunSummary_{datedir}.ecsv")
    today_runsummary = today_runsummary[today_runsummary["run_type"] == "DATA"]
    todays_join = join(today_runsummary, today_catalog)
    todays_join.add_column(date.strftime("%Y-%m-%d"), name="date_dir")
    todays_join.keep_columns(["run_id", "run_start", "source_name", "date_dir"])
    # Add start of run in iso format and elapsed time for each run
    log.info("Getting run start and elapsed time")
    add_run_start_iso(todays_join)
    add_elapsed(todays_join, datedir, version)
    # Change col names
    todays_join.rename_column('run_id', 'Run ID')
    todays_join.rename_column('run_start', 'Run start [UTC]')
    todays_join.rename_column('source_name', 'Source name')
    todays_join.rename_column('date_dir', 'Date directory')

    # Add new rows
    log.info("Adding new rows to table")
    new_table = vstack([table, todays_join])
    table_unique = unique(new_table, keys="Run ID", keep='last')

    # To pandas and HTML
    log.info("Converting to pandas and HTML")
    df = table_unique.to_pandas()
    df = df.sort_values(by="Run ID", ascending=False)
    html_table = df.to_html(index=False, justify="left")
    html_table = html_table.replace(
        '<table border="1" class="dataframe">',
        '<table class="display compact" id="table139855676982704">')
    html_content = add_query_table_to_html(html_table)
    html_file = Path("LST_source_catalog.html")
    html_file.write_text(html_content)
    table_unique.write(csv_file, delimiter=",", overwrite=True)

    copy_to_webserver(html_file, csv_file)


if __name__ == "__main__":
    main()
