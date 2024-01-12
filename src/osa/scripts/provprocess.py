#!/usr/bin/env python

"""Provenance post processing script for OSA pipeline."""

import copy
import logging
import shutil
import sys
from pathlib import Path, PurePath

import yaml

from osa.configs import options
from osa.configs.config import cfg
from osa.provenance.capture import get_activity_id, get_file_hash
from osa.provenance.io import provdoc2graph, provdoc2json, provlist2provdoc, read_prov
from osa.provenance.utils import get_log_config
from osa.utils.cliopts import provprocessparsing
from osa.utils.logging import myLogger

__all__ = ["copy_used_file", "parse_lines_log", "parse_lines_run", "produce_provenance"]

log = myLogger(logging.getLogger())

provconfig = yaml.safe_load(get_log_config())
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]
PROV_PREFIX = provconfig["PREFIX"]
PATH_DL1 = cfg.get("LST1", "DL1_DIR")
PATH_DL2 = cfg.get("LST1", "DL2_DIR")


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
            log.warning(f"could not copy {src} file into {destpath}: {ex}")


def parse_lines_log(filter_cut, calib_runs, run_number):
    """
    Filter content in log file to produce a run/process wise session log.

    Parameters
    ----------
    filter_cut
    calib_runs
    run_number

    Returns
    -------
    filtered
    """
    filtered = []
    if not filter_cut:
        filter_cut = "all"
    cuts = {
        "calibration": ["drs4_pedestal", "calibrate_charge"],
        "r0_to_dl1": ["r0_to_dl1", "dl1ab", "dl1_datacheck"],
        "dl1_to_dl2": ["dl1_to_dl2"],
    }
    cuts["all"] = cuts["calibration"] + cuts["r0_to_dl1"] + cuts["dl1_to_dl2"]

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

            # filter by run and calib runs
            if tag_run in [run_number, calib_runs]:
                keep = True
            # filter by activity
            if tag_activity not in cuts[filter_cut]:
                keep = False
            # only keep first session start
            if session_id and (tag_run in [run_number, calib_runs]):
                keep = True
            # make session starts with calibration
            if session_id and filter_cut == "all" and not filtered:
                prov_dict["session_id"] = f"{options.date}{run_number}"
                prov_dict["name"] = run_number
                prov_dict["observation_run"] = run_number
                line = f"{ll[0]}{PROV_PREFIX}{ll[1]}{PROV_PREFIX}{prov_dict}\n"
            # remove parallel sessions
            if session_id and filtered:
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
    mufilepath_str = ""
    ckfilepath_str = ""
    id_activity_run = ""
    end_time_line = ""
    osa_config_copied = False
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
        osa_cfg = line.get("config_file", "")

        # filter grain
        session_tag = line.get("session_tag", "0:0")
        tag_activity, _ = session_tag.split(":")
        if tag_activity != filter_step and not session_id:
            continue

        # remove subruns info
        if name == "DL1CheckSubrunDataset":
            ckfilepath_str = filepath
        elif name == "DL1SubrunDataset":
            dl1filepath_str = filepath
        elif name == "DL2SubrunDataset":
            dl2filepath_str = filepath
        elif name == "MuonsSubrunDataset":
            mufilepath_str = filepath
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

        # remove duplicated produced files
        if generated_role in container:
            remove = True
        if name == "DL2MergedFile":
            container[name] = True
        if "merged" in generated_role:
            container[generated_role] = True
        if name == "DL1CheckHDF5File":
            container[name] = True
        if "DL1Check HDF5 file" in generated_role:
            container[generated_role] = True
        if name == "DL1CheckPDFFile":
            container[name] = True
        if "DL1Check PDF file" in generated_role:
            container[generated_role] = True

        # replace with new run-wise activity_id
        if activity_id:
            line["activity_id"] = id_activity_run

        # copy used files not subruns not RFs not mergedDL2
        if (
            filepath
            and content_type != "application/x-spss-sav"
            and name != "DL2MergedFile"
            and not name.startswith("DL1Check")
            and not remove
        ):
            copy_used_file(filepath, out)
        if session_id and osa_cfg and not osa_config_copied:
            copy_used_file(osa_cfg, out)
            osa_config_copied = True

        if not remove:
            working_lines.append(line)

    # append collections used and generated at endtime line of last activity
    if end_time_line:
        working_lines.append(end_time_line)
        if r0filepath_str and filter_step == "r0_to_dl1":
            r0_entity_id = get_file_hash(r0filepath_str + "r0", buffer="path")
            r0filepath_str = r0filepath_str.replace(PurePath(r0filepath_str).name, "")
            used = {"entity_id": r0_entity_id}
            used.update({"name": "R0Collection"})
            used.update({"type": "SetCollection"})
            used.update({"size": size})
            used.update({"filepath": r0filepath_str})
            working_lines.append(used)
            used = {"activity_id": id_activity_run}
            used.update({"used_id": r0_entity_id})
            used.update({"used_role": "R0 Collection"})
            working_lines.append(used)
        if dl1filepath_str:
            dl1filepath_str = dl1filepath_str.replace(PurePath(dl1filepath_str).name, "")
            dl1_entity_id = get_file_hash(dl1filepath_str + "dl1", buffer="path")
            dl1 = {"entity_id": dl1_entity_id}
            dl1.update({"name": "DL1Collection"})
            dl1.update({"type": "SetCollection"})
            dl1.update({"size": size})
            dl1.update({"filepath": dl1filepath_str})
            working_lines.append(dl1)
        if mufilepath_str:
            mufilepath_str = mufilepath_str.replace(PurePath(mufilepath_str).name, "")
            mu_entity_id = get_file_hash(mufilepath_str + "muons", buffer="path")
            muons = {"entity_id": mu_entity_id}
            muons.update({"name": "MuonsCollection"})
            muons.update({"type": "SetCollection"})
            muons.update({"size": size})
            muons.update({"filepath": mufilepath_str})
            working_lines.append(muons)
        if mufilepath_str and filter_step == "r0_to_dl1":
            generated = {"activity_id": id_activity_run}
            generated.update({"generated_id": mu_entity_id})
            generated.update({"generated_role": "Muons Collection"})
            working_lines.append(generated)
        if dl1filepath_str and filter_step in ["r0_to_dl1", "dl1ab"]:
            generated = {"activity_id": id_activity_run}
            generated.update({"generated_id": dl1_entity_id})
            generated.update({"generated_role": "DL1 Collection"})
            working_lines.append(generated)
        if dl1filepath_str and filter_step in ["dl1_to_dl2", "dl1ab"]:
            used = {"activity_id": id_activity_run}
            used.update({"used_id": dl1_entity_id})
            used.update({"used_role": "DL1 Collection"})
            working_lines.append(used)
        if dl1filepath_str and filter_step == "dl1_datacheck":
            used = {"activity_id": id_activity_run}
            used.update({"used_id": dl1_entity_id})
            used.update({"used_role": "DL1 Collection"})
            working_lines.append(used)
        if mufilepath_str and filter_step == "dl1_datacheck":
            used = {"activity_id": id_activity_run}
            used.update({"used_id": mu_entity_id})
            used.update({"used_role": "Muons Collection"})
            working_lines.append(used)
        if ckfilepath_str and filter_step == "dl1_datacheck":
            ckfilepath_str = ckfilepath_str.replace(PurePath(ckfilepath_str).name, "")
            chk_entity_id = get_file_hash(ckfilepath_str + "check", buffer="path")
            dl1check = {"entity_id": chk_entity_id}
            dl1check.update({"name": "DL1CheckCollection"})
            dl1check.update({"type": "SetCollection"})
            dl1check.update({"size": size})
            dl1check.update({"filepath": ckfilepath_str})
            working_lines.append(dl1check)
            generated = {"activity_id": id_activity_run}
            generated.update({"generated_id": chk_entity_id})
            generated.update({"generated_role": "DL1Checks Collection"})
            working_lines.append(generated)
        if dl2filepath_str and filter_step == "dl1_to_dl2":
            dl2_entity_id = get_file_hash(dl2filepath_str + "dl2", buffer="path")
            dl2filepath_str = dl2filepath_str.replace(PurePath(dl2filepath_str).name, "")
            used = {"entity_id": dl2_entity_id}
            used.update({"name": "DL2Collection"})
            used.update({"type": "SetCollection"})
            used.update({"size": size})
            used.update({"filepath": dl2filepath_str})
            working_lines.append(used)
            used = {"activity_id": id_activity_run}
            used.update({"generated_id": dl2_entity_id})
            used.update({"generated_role": "DL2 Collection"})
            working_lines.append(used)

    else:
        working_lines = []

    return working_lines


def define_paths(grain, start_path, end_path, base_filename):
    """Define target folders according to granularity."""
    paths = {}

    # check destination folder exists
    step_path = Path(start_path) / options.date / options.prod_id / end_path
    if not step_path.exists():
        log.error(f"Path {step_path} does not exist")

    # make folder log/ if does not exist
    paths["out_path"] = step_path / "log"
    paths["out_path"].mkdir(parents=True, exist_ok=True)

    # define paths for prov products
    paths["log_path"] = paths["out_path"] / f"{grain}_{base_filename}.log"
    paths["json_filepath"] = paths["out_path"] / f"{grain}_{base_filename}.json"
    paths["graph_filepath"] = paths["out_path"] / f"{grain}_{base_filename}.pdf"

    return paths


def produce_provenance_files(processed_lines, paths):
    """Create provenance products as JSON logs and graphs."""
    with open(paths["log_path"], "w") as f:
        for line in processed_lines:
            f.write(f"{line}\n")
    log.info(f"creating {paths['log_path']}")
    provdoc = provlist2provdoc(processed_lines)

    # make json
    try:
        provdoc2json(provdoc, str(paths["json_filepath"]))
        log.info(f"creating {paths['json_filepath']}")
    except Exception as ex:
        log.exception(f"problem while creating json: {ex}")
    # make graph
    try:
        provdoc2graph(provdoc, str(paths["graph_filepath"]), "pdf")
        log.info(f"creating {paths['graph_filepath']}")
    except Exception as ex:
        log.exception(f"problem while creating graph: {ex}")


def produce_provenance(session_log_filename, base_filename):
    """
    Create run-wise provenance products as JSON logs
    and graphs according to granularity.
    """

    if options.filter == "calibration" or not options.filter:
        paths_calibration = define_paths(
            "calibration_to_dl1", PATH_DL1, options.dl1_prod_id, base_filename
        )
        plines_drs4 = parse_lines_run(
            "drs4_pedestal",
            read_prov(filename=session_log_filename),
            str(paths_calibration["out_path"]),
        )
        plines_calib = parse_lines_run(
            "calibrate_charge",
            read_prov(filename=session_log_filename),
            str(paths_calibration["out_path"]),
        )
        calibration_lines = plines_drs4 + plines_calib[1:]

    # TODO
    # create calibration prov files only if filtering
    if options.filter == "calibration":
        pass

    if options.filter == "r0_to_dl1" or not options.filter:
        paths_r0_dl1 = define_paths("r0_to_dl1", PATH_DL1, options.dl1_prod_id, base_filename)
        plines_r0 = parse_lines_run(
            "r0_to_dl1",
            read_prov(filename=session_log_filename),
            str(paths_r0_dl1["out_path"]),
        )
        plines_ab = parse_lines_run(
            "dl1ab",
            read_prov(filename=session_log_filename),
            str(paths_r0_dl1["out_path"]),
        )
        plines_check = parse_lines_run(
            "dl1_datacheck",
            read_prov(filename=session_log_filename),
            str(paths_r0_dl1["out_path"]),
        )
        dl1_lines = plines_r0 + plines_ab[1:] + plines_check[1:]

    # create r0_to_dl1 prov files only if filtering
    if options.filter == "r0_to_dl1":
        produce_provenance_files(plines_r0 + plines_ab[1:] + plines_check[1:], paths_r0_dl1)

    if options.filter == "dl1_to_dl2" or not options.filter:
        if not options.no_dl2:
            paths_dl1_dl2 = define_paths("dl1_to_dl2", PATH_DL2, options.dl2_prod_id, base_filename)
            plines_dl2 = parse_lines_run(
                "dl1_to_dl2",
                read_prov(filename=session_log_filename),
                str(paths_dl1_dl2["out_path"]),
            )
            dl1_dl2_lines = plines_dl2

    # create dl1_to_dl2 prov files only if filtering
    if options.filter == "dl1_to_dl2":
        if not options.no_dl2:
            produce_provenance_files(plines_dl2, paths_dl1_dl2)

    # create calibration_to_dl1 and calibration_to_dl2 prov files
    if not options.filter:
        calibration_to_dl1 = define_paths(
            "calibration_to_dl1", PATH_DL1, options.dl1_prod_id, base_filename
        )
        calibration_to_dl1_lines = calibration_lines + dl1_lines[1:]
        lines_dl1 = copy.deepcopy(calibration_to_dl1_lines)
        produce_provenance_files(lines_dl1, calibration_to_dl1)

        if not options.no_dl2:
            calibration_to_dl2 = define_paths(
                "calibration_to_dl2", PATH_DL2, options.dl2_prod_id, base_filename
            )
            calibration_to_dl2_lines = calibration_to_dl1_lines + dl1_dl2_lines[1:]
            lines_dl2 = copy.deepcopy(calibration_to_dl2_lines)
            produce_provenance_files(lines_dl2, calibration_to_dl2)


def main():
    """Extract the provenance information."""
    provprocessparsing()

    # Logging
    if options.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # check LOG_FILENAME exists
    if not Path(LOG_FILENAME).exists():
        log.error(f"file {LOG_FILENAME} does not exist")

    # check LOG_FILENAME is not empty
    if not Path(LOG_FILENAME).stat().st_size:
        log.warning(f"file {LOG_FILENAME} is empty")
        sys.exit(1)

    # build base_filename
    base_filename = f"{options.run}_prov"
    session_log_filename = f"{base_filename}.log"

    # parse LOG_FILENAME content for a specific run / process
    calib_runs = f"{options.drs4_pedestal_run_id}-{options.pedcal_run_id}"
    parsed_content = parse_lines_log(options.filter, calib_runs, options.run)

    # create temporal session log file
    with open(session_log_filename, "w") as f:
        for line in parsed_content:
            f.write(line)

    try:
        # create run-wise JSON logs and graphs for each
        produce_provenance(session_log_filename, base_filename)
    finally:
        # remove temporal session log file
        remove_session_log_file = Path(session_log_filename)
        remove_session_log_file.unlink()

    # remove LOG_FILENAME
    if options.quit:
        remove_log_file = Path(LOG_FILENAME)
        remove_log_file.unlink()


if __name__ == "__main__":
    main()
