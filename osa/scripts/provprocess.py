#!/usr/bin/env python

"""
Provenance post processing script for OSA pipeline
"""
import copy
import logging
import shutil
from pathlib import Path, PurePath

import yaml

from osa.configs import options
from osa.configs.config import cfg
from osa.provenance.capture import get_activity_id, get_file_hash
from osa.provenance.io import provdoc2graph, provdoc2json, provlist2provdoc, read_prov
from osa.provenance.utils import get_log_config
from osa.utils.cliopts import provprocessparsing

__all__ = ["copy_used_file", "parse_lines_log", "parse_lines_run", "produce_provenance"]

log = logging.getLogger(__name__)

provconfig = yaml.safe_load(get_log_config())
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]
PROV_PREFIX = provconfig["PREFIX"]


def copy_used_file(src, outdir):
    """
    Copy file used in process.

    Parameters
    ----------
    src
    outdir
    """
    # check src file exists
    if not Path(src).is_file():
        log.warning(f"{src} file cannot be accessed")

    hash_src = get_file_hash(src, buffer="content")
    filename = PurePath(src).name
    destpath = Path(outdir) / filename
    hash_out = ""

    # get hash and new name
    if destpath.exists():
        hash_out = get_file_hash(str(destpath), buffer="content")
        filename = filename + "_"
        destpath = Path(outdir) / filename

    # try copy file
    if hash_src != hash_out:
        try:
            shutil.copyfile(src, str(destpath))
            log.info(f"copying {destpath}")
        except Exception as ex:
            log.warning(f"could not copy {src} file into {str(destpath)}")
            log.warning(f"{ex}")


def parse_lines_log(filter_step, run_number):
    """
    Filter content in log file to produce a run/process wise session log.

    Parameters
    ----------
    filter_step
    run_number

    Returns
    -------
    filtered

    """
    filtered = []
    with open(LOG_FILENAME, "r") as f:
        for line in f.readlines():
            ll = line.split(PROV_PREFIX)
            if len(ll) != 3:
                log.warning(f"format {PROV_PREFIX} mismatch in log file {LOG_FILENAME}\n{line}")
                continue
            prov_str = ll.pop()
            prov_dict = yaml.safe_load(prov_str)
            keep = False
            session_tag = prov_dict.get("session_tag", "0:0")
            session_id = prov_dict.get("session_id", False)
            tag_activity, tag_run = session_tag.split(":")
            # filter by run
            if tag_run == run_number:
                keep = True
            # filter by activity
            if filter_step not in ["", tag_activity]:
                keep = False
            # always keep first session start
            if session_id and tag_run == run_number:
                keep = True
            # remove parallel sessions
            if session_id and len(filtered):
                keep = False
            if keep:
                filtered.append(line)
    return filtered


def parse_lines_run(filter_step, prov_lines, out):
    """
    Process provenance info to reduce session at run/process wise scope.

    Parameters
    ----------
    filter_step
    prov_lines
    out

    Returns
    -------
    working_lines

    """
    size = 0
    container = {}
    working_lines = []
    r0filepath_str = ""
    dl1filepath_str = ""
    dl2filepath_str = ""
    id_activity_run = ""
    end_time_line = ""
    for line in prov_lines:
        # get info
        remove = False
        endTime = line.get("endTime", "")
        session_id = line.get("session_id", "")
        activity_id = line.get("activity_id", "")
        filepath = line.get("filepath", "")
        used_role = line.get("used_role", "")
        generated_role = line.get("generated_role", "")
        parameters = line.get("parameters", "")
        name = line.get("name", "")
        content_type = line.get("contentType", "")
        used_id = line.get("used_id", "")

        # filter grain
        session_tag = line.get("session_tag", "0:0")
        tag_activity, tag_run = session_tag.split(":")
        if tag_activity != filter_step and not session_id:
            continue

        # remove subruns info
        if name == "DL1SubrunDataset":
            dl1filepath_str = filepath
        elif name == "DL2SubrunDataset":
            dl2filepath_str = filepath
        elif name == "R0SubrunDataset":
            r0filepath_str = filepath
        if "Subrun" in name or "subrun" in used_role or "subrun" in generated_role:
            remove = True
        if parameters and "ObservationSubRun" in parameters:
            del line["parameters"]["ObservationSubRun"]

        # remove sub-runs activities and info
        if name == filter_step and not id_activity_run:
            id_activity_run = get_activity_id()
        if name in container or used_id in container:
            remove = True
        if parameters and "parameters" in container:
            remove = True
        if name:
            container[name] = True
        if used_id:
            container[used_id] = True
        if parameters:
            container["parameters"] = True
        if endTime:
            remove = True
            end_time_line = line
            size += 1

        # replace with new run-wise activity_id
        if activity_id:
            line["activity_id"] = id_activity_run

        # copy used files not subruns not RFs
        if filepath and content_type != "application/x-spss-sav" and not remove:
            copy_used_file(filepath, out)

        if not remove:
            working_lines.append(line)

    # append collection run used and generated at endtime line of last activitiy
    if end_time_line:
        working_lines.append(end_time_line)
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
            dl1filepath_str = dl1filepath_str.replace(PurePath(dl1filepath_str).name, "")
            entity_id = get_file_hash(dl1filepath_str, buffer="path")
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
    # remove start session line
    else:
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
        if fold == pathDL2:
            step_path = Path(fold) / options.date / options.dl2_prod_id
        else:
            step_path = Path(fold) / options.date / options.prod_id

        # check destination folder exists
        if not step_path.exists():
            log.error(f"Path {step_path} does not exist")

        # make folder log/ if does not exist
        outpath = step_path / "log"
        outpath.mkdir(parents=True, exist_ok=True)

        # define paths for prov products
        log_path = outpath / f"{grain}_{base_filename}.log"
        json_filepath = outpath / f"{grain}_{base_filename}.json"
        graph_filepath = outpath / f"{grain}_{base_filename}.pdf"

        # process temp log file
        if grain != "r0_to_dl2":
            processed_lines = parse_lines_run(
                grain, read_prov(filename=session_log_filename), str(outpath)
            )
        if grain == "r0_to_dl1":
            r0_to_dl1_processed_lines = copy.deepcopy(processed_lines)
        if grain == "dl1_to_dl2":
            dl1_to_dl2_processed_lines = copy.deepcopy(processed_lines)
        if grain == "r0_to_dl2" and r0_to_dl1_processed_lines and dl1_to_dl2_processed_lines:
            processed_lines = r0_to_dl1_processed_lines + dl1_to_dl2_processed_lines[1:]

        if processed_lines:
            # make filtered session log file
            with open(log_path, "w") as f:
                for line in processed_lines:
                    f.write(f"{line}\n")
            log.info(f"creating {log_path}")
            provdoc = provlist2provdoc(processed_lines)
            # make json
            try:
                provdoc2json(provdoc, str(json_filepath))
                log.info(f"creating {json_filepath}")
            except Exception as ex:
                log.exception(f"problem while creating json: {ex}")
            # make graph
            try:
                provdoc2graph(provdoc, str(graph_filepath), "pdf")
                log.info(f"creating {graph_filepath}")
            except Exception as ex:
                log.exception(f"problem while creating graph: {ex}")


if __name__ == "__main__":

    # provprocess.py
    # 02006
    # v0.4.3_v00
    # -c cfg/sequencer.cfg
    # -f r0_to_dl1
    # -q
    provprocessparsing()

    # Logging
    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    format = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] (%(module)s.%(funcName)s): %(message)s"
    )
    handler.setFormatter(format)
    logging.getLogger().addHandler(handler)

    pathRO = cfg.get("LST1", "RAWDIR")
    pathDL1 = cfg.get("LST1", "DL1DIR")
    pathDL2 = cfg.get("LST1", "DL2DIR")
    GRANULARITY = {"r0_to_dl1": pathDL1, "dl1_to_dl2": pathDL2, "r0_to_dl2": pathDL2}
    if options.filter:
        GRANULARITY = {options.filter: GRANULARITY[options.filter]}

    # check LOG_FILENAME exists
    if not Path(LOG_FILENAME).exists():
        log.error(f"file {LOG_FILENAME} does not exist")

    # check LOG_FILENAME is not empty
    if not Path(LOG_FILENAME).stat().st_size:
        log.warning(f"file {LOG_FILENAME} is empty")
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
