"""
Provenance post processing script for OSA pipeline
"""


from provenance.io import *
from pathlib import Path


def copy_used_file(src, out):
    """Copy file used in process"""

    # move file hashing from capture.py to io.py
    # use file hashing from io.py
    #

    # check src can be accessed in read mode
    #
    # copy file
    #

    pass


def parse_lines_dl1(prov_lines, out):
    """Process r0 to dl1 provenance info to bundle session at run scope."""
    for i in prov_lines:
        if "used_file" in i:
            copy_used_file("file", out)
    return prov_lines


def move_logfile(src, out):
    """Rename and move logfile"""
    pass


def make_json(filepath):
    """Produce a provenance json file"""
    pass


def make_graph(filepath):
    """Produce a provenance graph"""
    pass


if __name__ == "__main__":
    from osa.utils import cliopts
    options = cliopts.provprocessparsing()

    # check options.src is a file
    #
    #

    # check options.out is a valid folder
    # make folder log/ if does not exist
    # add log/ to options.out = /fefs/aswg/data/real/DL1/20200218/v0.4.3_v00
    #
    #
    options.out = Path(options.out)

    # process prov file
    processed_lines = parse_lines_dl1(read_prov(logname=options.src), options.out)
    #
    #

    # build base_filename with options.run and options.out
    #
    #
    base_filename = ""
    json_filepath = options.out / f"{base_filename}.json"
    png_filepath = options.out / f"{base_filename}.png"

    move_logfile(options.src, options.out)
    make_json(json_filepath)
    make_graph(png_filepath)
