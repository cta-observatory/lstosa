import datetime
from pathlib import Path
import subprocess as sp
import sys
import argparse
from osa.configs import options


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y_%m_%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'."
        raise argparse.ArgumentTypeError(msg)


# FIXME: unify with sequencer cli parsing?
def argument_parser():
    parser = argparse.ArgumentParser(
        description="Script to make an xhtml from LSTOSA sequencer output"
    )
    parser.add_argument("-d", "--date", help="Date - format YYYY_MM_DD", type=valid_date)
    parser.add_argument(
        "-c",
        "--config-file",
        dest="osa_config_file",
        default="cfg/sequencer.cfg",
        help="OSA config file.",
    )

    return parser


def matrixtohtmltable(matrix, column_class, header, footer):
    """
    Header and footer are simple simple bool in order to build first and last line
    with outside the body with th, td. Column_class is a css class to align columns.
    """

    is_tbody_open = False
    is_tbody_closed = False
    print("<table>")
    for index, row in enumerate(matrix):
        if index == 0 and header:
            print("<thead>")
        elif index == len(matrix) - 1 and footer:
            print("<tfoot>")
        elif not is_tbody_open:
            print("<tbody>")
            is_tbody_open = True
        print("<tr>")
        for col in row:
            if (index == 0 and header) or row[0] == matrix[0][0]:
                # I know this invalidates html, but it is nice some headers in between
                print('<th class="{1}">{0}</th>'.format(col, column_class[row.index(col)]))
            else:
                print('<td class="{1}">{0}</td>'.format(col, column_class[row.index(col)]))
        print("</tr>")
        if index == 0 and header:
            print("</thead>")
        elif index == len(matrix) - 1 and footer:
            print("</tfoot>")
        elif not is_tbody_closed:
            if index == len(matrix) - 1 or index == len(matrix) - 2 and footer:
                print("</tbody>")
                is_tbody_closed = True
    print("</table>")


def webhead(header):
    print(
        '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">'
    )
    print('<html xmlns="http://www.w3.org/1999/xhtml">')
    print(" <head>")
    print('  <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />')
    print(header)
    print(" </head>")
    print(" <body>")


def webtail():
    print(" </body>")
    print("</html>")


def main():
    args = argument_parser().parse_args()
    if args.date:
        year = args.date.year
        month = args.date.month
        day = args.date.day
        options.date = f"{year:04}_{month:02}_{day:02}"
        strdate = f"{year:04}{month:02}{day:02}"
    else:
        # yesterday by default
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        options.date = yesterday.strftime("%Y_%m_%d")
        strdate = yesterday.strftime("%Y%m%d")

    run_summary_directory = Path("/fefs/aswg/data/real/monitoring/RunSummary")
    run_summary_file = run_summary_directory / f"RunSummary_{strdate}.ecsv"

    # FIXME: Parse this via command line as done in the sequencer.
    #        It has to somehow identify if data was taken and update the web.
    #        Otherwise the web does not needs to be updated.
    telescope = "LST1"

    if not run_summary_file.is_file():
        print(f"No RunSummary file found for {strdate}")
        sys.exit(1)

    # Print the output into a web page
    webhead(
        '<title>OSA Sequencer in the LST onsite IT center</title><link href="osa.css" rel="stylesheet" type="text/css" /><style>table{width:152ex;}</style>'
    )

    print("<h1>OSA sequencer in the LST onsite IT center</h1>")

    # Print the matrix
    column_class = [
        "left",
        "right",
        "right",
        "left",
        "right",
        "right",
        "left",
        "left",
        "left",
        "right",
        "right",
        "left",
        "left",
        "right",
        "right",
        "right",
        "right",
        "right",
        "right",
        "right",
        "right",
    ]

    #  Call sequencer
    commandargs = [
        "sequencer",
        "-c",
        args.osa_config_file,
        "-s",
        "-d",
        options.date,
        telescope,
    ]

    try:
        output = sp.run(commandargs, stdout=sp.PIPE, stderr=sp.STDOUT, encoding="utf-8")
    except sp.CalledProcessError:
        # Sorry, it does not work (day closed, asked with wrong parameters ...)
        print(f"Command with the following args {commandargs} failed, {output.returncode}")
        sys.exit(1)
    else:
        print(
            f'<p>Sequencer at {datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC. '
            f'Telescopes found: {telescope}</p>'
        )

        # Strip newlines and fit it into a table:
        lines = output.stdout.splitlines()

        if len(lines) > 1:
            matrix = []
            for l in lines:
                l_fields = l.split()
                if len(l_fields) == 21:
                    # Full line, all OK
                    matrix.append(l_fields)

            matrixtohtmltable(matrix, column_class, True, False)
        else:
            # Show just plain text
            print(f"<pre>{output.stdout}</pre>")

    # Print the closing html
    webtail()


if __name__ == "__main__":
    main()
