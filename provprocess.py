"""
Provenance post processing script for OSA pipeline
"""

from osa.utils import cliopts, standardhandle
from provenance.capture import get_file_hash, get_activity_id
from provenance.io import *
from pathlib import Path, PurePath
import re
import shutil


def copy_used_file(src, out, tag_handle):
    """Copy file used in process"""

    # check src file exists
    if not Path(src).is_file():
        standardhandle.error(tag_handle, f"{src} file cannot be accessed", 2)

    hash_src = get_file_hash(src, buffer="content")
    filename = PurePath(src).name
    outpath = Path(out) / filename
    hash_out = ""

    # get hash and new name
    if outpath.exists():
        hash_out = get_file_hash(str(outpath), buffer="content")
        filename = filename + "_"
        outpath = Path(out) / filename

    # try copy file
    if hash_src != hash_out:
        try:
            shutil.copyfile(src, str(outpath))
        except Exception as ex:
            standardhandle.warning(tag_handle, f"could not copy {src} file into {str(outpath)}")
            standardhandle.warning(tag_handle, f"{ex}")


def parse_lines_dl1(prov_lines, out, tag_handle):
    """Process r0 to dl1 provenance info to bundle session at run scope."""

    i = 0
    size = 0
    working_lines = []
    id_activity_run = ""
    for line in prov_lines:

        # get info
        i += 1
        remove = False
        endTime = line.get("endTime", "")
        session_id = line.get("session_id", "")
        activity_id = line.get("activity_id", "")
        filepath = line.get("filepath", "")
        used_role = line.get("generated_role", "")
        generated_role = line.get("generated_role", "")
        parameters = line.get("parameters", "")
        name = line.get("name", "")

        # remove subruns info
        if "data/real/R0/" in filepath or used_role == "Observation subrun":
            remove = True
        if "data/real/DL1/" in filepath or generated_role == "DL1 subrun dataset":
            remove = True
        if parameters:
            del line["parameters"]["ObservationSubRun"]
        if name == "r0_to_dl1" and not session_id:
            size += 1
            if not id_activity_run:
                id_activity_run = get_activity_id()
        if size > 1:
            remove = True

        # new id
        if activity_id:
            line["activity_id"] = id_activity_run

        # copy used files
        if filepath and not remove:
            copy_used_file(filepath, out, tag_handle)
        if not remove:
            working_lines.append(line)

    return working_lines


def make_json(filepath):
    """Produce a provenance json file"""
    pass


def make_graph(filepath):
    """Produce a provenance graph"""
    pass


if __name__ == "__main__":

    options, tag = cliopts.provprocessparsing()

    # check options.src is a file
    if not Path(options.src).exists():
        standardhandle.error(tag, f"file {options.src} does not exist", 2)

    # check options.out is a folder
    if not Path(options.out).exists():
        standardhandle.error(tag, f"path {options.out} does not exist", 2)

    # make folder log/ if does not exist
    options.out = Path(options.out) / "log"
    if not options.out.exists():
        options.out.mkdir()

    # process prov file
    processed_lines = parse_lines_dl1(read_prov(logname=options.src), options.out, tag)

    # build base_filename with options.run and options.out
    #
    #
    base_filename = ""
    json_filepath = options.out / f"{base_filename}.json"
    png_filepath = options.out / f"{base_filename}.png"

    # shutil.move(options.src, log_path)
    make_json(json_filepath)
    make_graph(png_filepath)
