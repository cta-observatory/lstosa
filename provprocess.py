"""
Provenance post processing script for OSA pipeline
"""

import copy
import shutil
from pathlib import Path, PurePath

import yaml

from osa.utils import cliopts, standardhandle
from provenance.capture import get_activity_id, get_file_hash
from provenance.io import *
from provenance.utils import get_log_config

provconfig = yaml.safe_load(get_log_config())
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]
PROV_PREFIX = provconfig["PREFIX"]


def copy_used_file(src, out):
    """Copy file used in process."""

    # check src file exists
    if not Path(src).is_file():
        standardhandle.error(tag, f"{src} file cannot be accessed", 2)

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
            standardhandle.warning(tag, f"could not copy {src} file into {str(destpath)}")
            standardhandle.warning(tag, f"{ex}")


def parse_lines_log(filter_step, run_number):
    """Filter content in log file to produce a run/process wise session log."""
    filtered = []
    with open(LOG_FILENAME, "r") as f:
        for line in f.readlines():
            ll = line.split(PROV_PREFIX)
            if len(ll) != 3:
                standardhandle.warning(tag, f"format {PROV_PREFIX} mismatch in log file {LOG_FILENAME}\n{line}")
                continue
            prov_str = ll.pop()
            prov_dict = yaml.safe_load(prov_str)
            keep = False
            session_tag = prov_dict.get("session_tag", "0:0")
            tag_activity, tag_run = session_tag.split(":")
            if tag_run == run_number:
                keep = True
            if filter_step != "" and filter_step != tag_activity:
                keep = False
            # always keep first line / session start
            if keep or not filtered:
                filtered.append(line)
    return filtered


# TODO: add used DL1DL2Collection, generated DL2Collection
#
def parse_lines_run(filter_step, prov_lines, out):
    """Process provenance info to reduce session at run/process wise scope."""

    i = 0
    size = 0
    working_lines = []
    r0filepath_str = ""
    dl1filepath_str = ""
    dl2filepath_str = ""
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
        if (
            name == "DL1SubrunDataset"
            or generated_role == "DL1 subrun dataset"
            or used_role == "DL1 subrun dataset"
        ):
            if filepath:
                dl1filepath_str = filepath
            remove = True
        if name == "DL2SubrunDataset" or generated_role == "DL2 subrun dataset":
            if filepath:
                dl2filepath_str = filepath
            remove = True
        if parameters and "ObservationSubRun" in parameters:
            del line["parameters"]["ObservationSubRun"]
        # group subruns activities into a single run-wise activity
        if name == filter_step:
            size += 1
            if not id_activity_run:
                id_activity_run = get_activity_id()
        if size > 1:
            remove = True
        # replace with new run-wise activity_id
        if activity_id:
            line["activity_id"] = id_activity_run
        # filter grain
        session_tag = line.get("session_tag", "0:0")
        tag_activity, tag_run = session_tag.split(":")
        if tag_activity != filter_step:
            remove = True
        # copy not subruns used files
        if filepath and not remove:
            copy_used_file(filepath, out)
        # always keep first line / session start
        if session_id:
            remove = False

        # append collection run used and generated at endtime line of last activitiy
        if endTime and i == len(prov_lines) and size:
            if r0filepath_str and filter_step == "r0_to_dl1":
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
            if dl1filepath_str:
                entity_id = get_file_hash(dl1filepath_str, buffer="path")
                dl1filepath_str = dl1filepath_str.replace(PurePath(dl1filepath_str).name, "")
                dl1 = {"entity_id": entity_id}
                dl1.update({"name": "DL1Collection"})
                dl1.update({"type": "SetCollection"})
                dl1.update({"size": size})
                dl1.update({"filepath": dl1filepath_str})
                working_lines.append(dl1)
            if dl1filepath_str and filter_step == "r0_to_dl1":
                generated = {"activity_id": id_activity_run}
                generated.update({"generated_id": entity_id})
                generated.update({"generated_role": "DL1 Collection"})
                working_lines.append(generated)
            if dl1filepath_str and filter_step == "dl1_to_dl2":
                used = {"activity_id": id_activity_run}
                used.update({"used_id": entity_id})
                used.update({"used_role": "DL1 Collection"})
                working_lines.append(used)
            if dl2filepath_str and filter_step == "dl1_to_dl2":
                entity_id = get_file_hash(dl2filepath_str, buffer="path")
                dl2filepath_str = dl2filepath_str.replace(PurePath(dl2filepath_str).name, "")
                used = {"entity_id": entity_id}
                used.update({"name": "DL2Collection"})
                used.update({"type": "SetCollection"})
                used.update({"size": size})
                used.update({"filepath": dl2filepath_str})
                working_lines.append(used)
                used = {"activity_id": id_activity_run}
                used.update({"generated_id": entity_id})
                used.update({"generated_role": "DL2 Collection"})
                working_lines.append(used)

        if not remove:
            working_lines.append(line)

    # remove start session line
    if len(working_lines) == 1:
        working_lines = []

    return working_lines


def produce_provenance():
    """Create run-wise provenance products as JSON logs and graphs according to granularity."""

    # create prov products for each granularity level
    r0_to_dl1_processed_lines = []
    dl1_to_dl2_processed_lines = []
    for grain, fold in GRANULARITY.items():

        processed_lines = []
        # derive destination folder
        step_path = Path(fold) / options.datefolder / options.subfolder

        # check destination folder exists
        if not step_path.exists():
            standardhandle.error(tag, f"Path {step_path} does not exist", 2)

        # make folder log/ if does not exist
        outpath = step_path / "log"
        if not outpath.exists():
            outpath.mkdir()

        # define paths for prov products
        log_path = outpath / f"{grain}_{base_filename}.log"
        json_filepath = outpath / f"{grain}_{base_filename}.json"
        graph_filepath = outpath / f"{grain}_{base_filename}.pdf"

        # process temp log file
        if grain != "r0_to_dl2":
            processed_lines = parse_lines_run(grain, read_prov(filename=session_log_filename), str(outpath))
        if grain == "r0_to_dl1":
            r0_to_dl1_processed_lines = copy.deepcopy(processed_lines)
        if grain == "dl1_to_dl2":
            dl1_to_dl2_processed_lines = copy.deepcopy(processed_lines)
        if grain == "r0_to_dl2" and r0_to_dl1_processed_lines and dl1_to_dl2_processed_lines:
            processed_lines = r0_to_dl1_processed_lines + dl1_to_dl2_processed_lines[1:]

        if processed_lines:
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


if __name__ == "__main__":

    # provprocess.py
    # 02006
    # v0.4.3_v00
    # -c cfg/sequencer.cfg
    # -f r0_to_dl1
    # -q
    options, tag = cliopts.provprocessparsing()

    from osa.configs.config import cfg

    pathRO = cfg.get("LST1", "RAWDIR")
    pathDL1 = cfg.get("LST1", "ANALYSISDIR")
    pathDL2 = cfg.get("LST1", "DL2DIR")
    GRANULARITY = {"r0_to_dl1": pathDL1, "dl1_to_dl2": pathDL2, "r0_to_dl2": pathDL2}
    if options.filter:
        GRANULARITY = {options.filter: GRANULARITY[options.filter]}

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

    # parse LOG_FILENAME content for a specific run / process
    parsed_content = parse_lines_log(options.filter, options.run)

    # create temporal session log file
    with open(session_log_filename, "w") as f:
        for line in parsed_content:
            f.write(line)

    try:
        # create run-wise JSON logs and graphs for each
        produce_provenance()
    finally:
        # remove temporal session log file
        remove_session_log_file = Path(session_log_filename)
        remove_session_log_file.unlink()

    # remove LOG_FILENAME
    if options.quit:
        remove_log_file = Path(LOG_FILENAME)
        remove_log_file.unlink()
