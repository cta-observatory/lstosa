"""
Provenance post processing script for OSA pipeline
"""

import shutil
from pathlib import Path, PurePath
from osa.utils import cliopts, standardhandle
from provenance.capture import get_activity_id, get_file_hash
from provenance.io import *
from provenance.utils import get_log_config
import yaml

provconfig = yaml.safe_load(get_log_config())
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]
PROV_PREFIX = provconfig["PREFIX"]


def copy_used_file(src, out, tag_handle):
    """Copy file used in process"""

    # check src file exists
    if not Path(src).is_file():
        standardhandle.error(tag_handle, f"{src} file cannot be accessed", 2)

    hash_src = get_file_hash(src, buffer="content")
    filename = PurePath(src).name
    destpath = Path(out) / filename
    hash_out = ""

    # get hash and new name
    if destpath.exists():
        hash_out = get_file_hash(str(destpath), buffer="content")
        filename = filename + "_"
        destpath = Path(out) / filename

    # try copy file
    if hash_src != hash_out:
        try:
            shutil.copyfile(src, str(destpath))
        except Exception as ex:
            standardhandle.warning(tag_handle, f"could not copy {src} file into {str(destpath)}")
            standardhandle.warning(tag_handle, f"{ex}")


def parse_lines_log(run_number, tag_handle):
    """Filter content in log file to produce a run wise session log."""
    filtered = []
    with open(LOG_FILENAME, "r") as f:
        for line in f.readlines():
            ll = line.split(PROV_PREFIX)
            if len(ll) < 3:
                standardhandle.error(tag_handle, f"Error in format of log file {LOG_FILENAME}", 2)
            prov_str = ll.pop()
            prov_dict = yaml.safe_load(prov_str)
            keep = False
            session_tag = prov_dict.get("session_tag", "0:0")
            tag_activity, tag_run = session_tag.split(":")
            if int(tag_run) == int(run_number):
                keep = True
            if keep:
                filtered.append(line)
    return filtered


# TODO: add granularity parameter in parse_lines_run function
#
#
def parse_lines_run(prov_lines, out, tag_handle):
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
        used_role = line.get("used_role", "")
        generated_role = line.get("generated_role", "")
        parameters = line.get("parameters", "")
        name = line.get("name", "")

        # remove subruns info
        if name == "R0SubrunDataset" or used_role == "Observation subrun":
            if filepath:
                r0filepath_str = filepath
            remove = True
        if name == "DL1SubrunDataset" or generated_role == "DL1 subrun dataset":
            if filepath:
                dl1filepath_str = filepath
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

        # copy not subruns used files
        if filepath and not remove:
            copy_used_file(filepath, out, tag_handle)

        # keep endtime
        # append collection run used and generated
        if endTime:
            if i == len(prov_lines):
                remove = False
                #
                entity_id = get_file_hash(r0filepath_str, buffer="path")
                r0filepath_str = r0filepath_str.replace(PurePath(r0filepath_str).name, "")
                used = {"entity_id": entity_id}
                used.update({"name": "R0Collection"})
                used.update({"type": "SetCollection"})
                used.update({"size": size})
                used.update({"filepath": r0filepath_str})
                working_lines.append(used)
                used = {"activity_id": id_activity_run}
                used.update({"used_id": entity_id})
                used.update({"used_role": "R0 Collection"})
                working_lines.append(used)
                #
                entity_id = get_file_hash(dl1filepath_str, buffer="path")
                dl1filepath_str = dl1filepath_str.replace(PurePath(dl1filepath_str).name, "")
                generated = {"entity_id": entity_id}
                generated.update({"name": "DL1Collection"})
                generated.update({"type": "SetCollection"})
                generated.update({"size": size})
                generated.update({"filepath": dl1filepath_str})
                working_lines.append(generated)
                generated = {"activity_id": id_activity_run}
                generated.update({"generated_id": entity_id})
                generated.update({"generated_role": "DL1 Collection"})
                working_lines.append(generated)

            else:
                remove = True

        if not remove:
            working_lines.append(line)

    return working_lines


if __name__ == "__main__":

    # provprocess.py
    # 02006
    # v0.4.3_v00
    # -c cfg/sequencer.cfg
    # -q
    options, tag = cliopts.provprocessparsing()

    from osa.configs.config import cfg
    pathRO = cfg.get("LST1", "RAWDIR")
    pathDL1 = cfg.get("LST1", "ANALYSISDIR")
    pathDL2 = cfg.get("LST1", "DL2DIR")
    GRANULARITY = {"r0_to_dl1": pathDL1}  # "dl1_to_dl2", "r0_to_dl2

    # check LOG_FILENAME exists
    if not Path(LOG_FILENAME).exists():
        standardhandle.error(tag, f"file {LOG_FILENAME} does not exist", 2)

    # check LOG_FILENAME is not empty
    if not Path(LOG_FILENAME).stat().st_size:
        standardhandle.warning(tag, f"file {LOG_FILENAME} is empty")
        exit()

    # build base_filename
    base_filename = f"{options.run}_prov"
    session_log_filename = f"{base_filename}.log"

    # parse LOG_FILENAME content for a specific run
    parsed_content = parse_lines_log(options.run, tag)

    # create temporal session log file
    with open(session_log_filename, 'w') as f:
        for line in parsed_content:
            f.write(line)

    # create prov products for each granularity
    for grain, fold in GRANULARITY.items():

        # derive destination folder
        step_path = Path(fold) / options.datefolder / options.subfolder

        # check destination folder exists
        if not step_path.exists():
            standardhandle.error(tag, f"path {step_path} does not exist", 2)

        # make folder log/ if does not exist
        outpath = step_path / "log"
        if not outpath.exists():
            outpath.mkdir()

        # define paths for prov products
        log_path = outpath / f"{grain}_{base_filename}.log"
        json_filepath = outpath / f"{grain}_{base_filename}.json"
        graph_filepath = outpath / f"{grain}_{base_filename}.pdf"

        # TODO: add granularity parameter in parse_lines_run function
        # process session log file created
        processed_lines = parse_lines_run(read_prov(filename=session_log_filename), str(outpath), tag)

        # copy session log file to its log folder
        shutil.copyfile(session_log_filename, log_path)

        # make json
        try:
            provdoc = provlist2provdoc(processed_lines)
            provdoc.serialize(str(json_filepath), indent=4)
        except Exception as ex:
            standardhandle.error(tag, f"problem while creating json: {ex}", 2)

        # make graph
        try:
            provdoc2graph(provdoc, str(graph_filepath), "pdf")
        except Exception as ex:
            standardhandle.error(tag, f"problem while creating graph: {ex}", 2)

    # remove temporal session log file
    remove_session_log_file = Path(session_log_filename)
    remove_session_log_file.unlink()

    # remove LOG_FILENAME
    if options.quit:
        remove_log_file = Path(LOG_FILENAME)
        remove_log_file.unlink()

