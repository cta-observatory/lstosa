"""Provenance capture functions."""

import datetime
import hashlib
import logging
import logging.config
import os
import platform
import sys
import uuid
from functools import wraps
from pathlib import Path

import pkg_resources
import psutil
import yaml

from osa.provenance.utils import get_log_config, parse_variables

# gammapy specific
# from gammapy.scripts.info import (
#    get_info_dependencies,
#    get_info_envvar,
#    get_info_version,
# )

__all__ = ["trace", "get_file_hash", "get_activity_id"]

from osa.utils.logging import myLogger

_interesting_env_vars = [
    "CONDA_DEFAULT_ENV",
    "CONDA_PREFIX",
    "CONDA_PYTHON_EXE",
    "CONDA_EXE",
    "CONDA_PROMPT_MODIFIER",
    "CONDA_SHLVL",
    "PATH",
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "USER",
    "HOME",
    "SHELL",
]

CONFIG_PATH = Path(__file__).resolve().parent / "config"
SCHEMA_FILE = CONFIG_PATH / "definition.yaml"
# LOGGER_FILE = CONFIG_PATH / "logger.yaml"
definition = yaml.safe_load(SCHEMA_FILE.read_text())
provconfig = yaml.safe_load(get_log_config())
logger = logging.getLogger("provLogger")
LOG_FILENAME = provconfig["handlers"]["provHandler"]["filename"]
PROV_PREFIX = provconfig["PREFIX"]
SUPPORTED_HASH_METHOD = ["md5"]
SUPPORTED_HASH_BUFFER = ["content", "path"]
REDUCTION_TASKS = ["r0_to_dl1", "dl1ab", "dl1_datacheck", "dl1_to_dl2"]

# global variables
traced_entities = {}
session_name = ""
session_tag = ""


def setup_logging():
    """Setup logging configuration."""
    log = myLogger(logging.getLogger(__name__))

    try:
        logging.config.dictConfig(provconfig)
    except Exception as ex:
        log.warning(ex)
        log.warning("Failed to set up the logger.")


# def provenance(cls):
#     """A function decorator which decorates the methods of a class with trace function."""
#
#     setup_logging()
#     for attr in cls.__dict__:
#         if attr in definition["activities"].keys() and callable(getattr(cls, attr)):
#             setattr(cls, attr, trace(getattr(cls, attr)))
#     return cls


def trace(func):
    """Trace and capture provenance info inside a method /function."""
    setup_logging()

    @wraps(func)
    def wrapper(*args, **kwargs):

        activity = func.__name__
        activity_id = get_activity_id()
        # class_instance = args[0]
        class_instance = func
        class_instance.args = args
        class_instance.kwargs = kwargs

        # OSA specific
        # variables parsing
        global session_name, session_tag
        class_instance = parse_variables(class_instance)
        if class_instance.__name__ in REDUCTION_TASKS:
            session_tag = f"{activity}:{class_instance.ObservationRun}"
            session_name = f"{class_instance.ObservationRun}"
        else:
            session_tag = f"{activity}:{class_instance.PedestalRun}-{class_instance.CalibrationRun}"
            session_name = f"{class_instance.PedestalRun}-{class_instance.CalibrationRun}"
        # OSA specific
        # variables parsing

        # provenance capture before execution
        derivation_records = get_derivation_records(class_instance, activity)
        parameter_records = get_parameters_records(class_instance, activity, activity_id)
        usage_records = get_usage_records(class_instance, activity, activity_id)

        # activity execution
        start = datetime.datetime.now().isoformat()
        result = func(*args, **kwargs)
        end = datetime.datetime.now().isoformat()

        # no provenance logging
        if not log_is_active(class_instance, activity):
            return result
        # provenance logging only if activity ends properly
        session_id = log_session(class_instance, start)
        for log_record in derivation_records:
            log_prov_info(log_record)
        log_start_activity(activity, activity_id, session_id, start)
        for log_record in parameter_records:
            log_prov_info(log_record)
        for log_record in usage_records:
            log_prov_info(log_record)
        log_generation(class_instance, activity, activity_id)
        log_finish_activity(activity_id, end)
        return result

    return wrapper


def log_is_active(class_instance, activity):
    """Check if provenance option is enabled in configuration settings."""
    active = True
    if activity not in definition["activities"].keys():
        active = False
    if not class_instance:
        active = False
    if "capture" not in provconfig or not provconfig["capture"]:
        active = False
    return active


def get_activity_id():
    # uuid example: ea5caa9f-0a76-42f5-a1a7-43752df755f0
    # uuid[-12:]: 43752df755f0
    # uuid[-6:]: f755f0
    return str(uuid.uuid4())[-6:]


def get_hash_method():
    """Helper function that returns hash method used."""
    try:
        method = provconfig["HASH_METHOD"]
    except KeyError:
        method = "md5"
    if method not in SUPPORTED_HASH_METHOD:
        logger.warning(f"Hash method {method} not supported.")
        method = "md5"
    return method


def get_hash_buffer():
    """Helper function that returns buffer content to be used in hash method used."""
    try:
        buffer = provconfig["HASH_TYPE"]
    except KeyError:
        buffer = "path"
    if buffer not in SUPPORTED_HASH_BUFFER:
        logger.warning(f"Hash buffer {buffer} not supported.")
        buffer = "path"
    return buffer


def get_file_hash(str_path, buffer=get_hash_buffer(), method=get_hash_method()):
    """Helper function that returns hash of the content of a file."""
    file_hash = ""
    full_path = Path(str_path)
    hash_func = getattr(hashlib, method)()
    if buffer == "content":
        if full_path.is_file():
            with open(full_path, "rb") as f:
                block_size = 65536
                buf = f.read(block_size)
                while len(buf) > 0:
                    hash_func.update(buf)
                    buf = f.read(block_size)
            file_hash = hash_func.hexdigest()
            logger.debug(f"File entity {str_path} has {method} hash {file_hash}")
            return file_hash

        logger.warning(f"File entity {str_path} not found")
        return str_path

    if not file_hash:
        hash_func.update(str(full_path).encode())
        return hash_func.hexdigest()


def get_entity_id(value, item):
    """Helper function that makes the id of an entity, depending on its type."""
    try:
        entity_name = item["entityName"]
        entity_type = definition["entities"][entity_name]["type"]
    except Exception as ex:
        logger.warning(f"Not found in model {ex} in {item}")
        entity_name = ""
        entity_type = ""

    # gammapy specific
    # if entity_type == "FileCollection":
    #     filename = value
    #     index = definition["entities"][entity_name].get("index", "")
    #     if Path(filename).is_dir() and index:
    #         filename = Path(value) / index
    #     return get_file_hash(filename)
    if entity_type == "File":
        # osa specific hash path
        # async calls does not allow for hash content
        return get_file_hash(value, buffer="path")
        # osa specific hash path
        # async calls does not allow for hash content
    try:
        entity_id = abs(hash(value) + hash(str(value)))
        if hasattr(value, "entity_version"):
            entity_id += getattr(value, "entity_version")
        return entity_id
    except TypeError:
        # remark: two different objects may use the same memory address
        # so use hash(entity_name) to avoid issues
        return abs(id(value) + hash(entity_name))


def get_nested_value(nested, branch):
    """Helper function that gets a specific value in a nested dictionary or class."""
    list_branch = branch.split(".")
    leaf = list_branch.pop(0)
    # return value of leaf
    if not nested:
        return globals().get(leaf, None)
    # get value of leaf
    if isinstance(nested, dict):
        val = nested.get(leaf, None)
    elif isinstance(nested, object):
        if "(" in leaf:  # leaf is a function
            leaf_elements = leaf.replace(")", "").replace(" ", "").split("(")
            leaf_arg_list = leaf_elements.pop().split(",")
            leaf_func = leaf_elements.pop()
            leaf_args = []
            leaf_kwargs = {}
            for arg in leaf_arg_list:
                if "=" in arg:
                    k, v = arg.split("=")
                    leaf_kwargs[k] = v.replace('"', "")
                elif arg:
                    leaf_args.append(arg.replace('"', ""))
            val = getattr(nested, leaf_func, lambda *args, **kwargs: None)(
                *leaf_args, **leaf_kwargs
            )
        else:  # leaf is an attribute
            val = getattr(nested, leaf, None)
    else:
        raise TypeError
    # continue to explore branch
    if len(list_branch):
        str_branch = ".".join(list_branch)
        return get_nested_value(val, str_branch)
    # return value of leaf
    if not val:
        val = globals().get(leaf, None)
    return val


def get_item_properties(nested, item):
    """Helper function that returns properties of an entity or member."""
    try:
        entity_name = item["entityName"]
        entity_type = definition["entities"][entity_name]["type"]
    except Exception as ex:
        logger.warning(f"{ex} in {item}")
        entity_name = ""
        entity_type = ""
    properties = {}
    if "id" in item:
        item_id = str(get_nested_value(nested, item["id"]))
        item_ns = item.get("namespace", None)
        if item_ns:
            item_id = item_ns + ":" + item_id
        properties["id"] = item_id
    if "location" in item:
        properties["location"] = get_nested_value(nested, item["location"])
    value = get_nested_value(nested, item["value"]) if "value" in item else ""
    if not value and "location" in properties:
        value = properties["location"]
    if "overwrite" in item:
        # add or increment entity_version to make value a different entity
        if hasattr(value, "entity_version"):
            version = getattr(value, "entity_version")
            version += 1
            setattr(value, "entity_version", version)
        else:
            try:
                setattr(value, "entity_version", 1)
            except AttributeError as ex:
                logger.warning(f"{ex} for {value}")
    if value and "id" not in properties:
        properties["id"] = get_entity_id(value, item)
        if "File" in entity_type:
            properties["filepath"] = value
            if properties["id"] != value:
                method = get_hash_method()
                properties["hash"] = properties["id"]
                properties["hash_type"] = method

    if entity_name:
        properties["name"] = entity_name
        for attr in ["type", "contentType"]:
            if attr in definition["entities"][entity_name]:
                properties[attr] = definition["entities"][entity_name][attr]
    return properties


def get_python_packages():
    """Return the collection of dependencies available for importing."""
    return [
        {"name": p.project_name, "version": p.version, "path": p.module_path}
        for p in sorted(pkg_resources.working_set, key=lambda p: p.project_name)
    ]


def log_prov_info(prov_dict):
    """Write a dictionary to the logger."""
    # OSA specific session tag used in merging prov from parallel sessions
    prov_dict["session_tag"] = session_tag
    #
    record_date = datetime.datetime.now().isoformat()
    logger.info(f"{PROV_PREFIX}{record_date}{PROV_PREFIX}{prov_dict}")


def log_session(class_instance, start):
    """Log start of a session."""
    # OSA specific
    # prov session is outside scripting and is run-wise
    # we may have different sessions/runs in the same log file
    # session_id = abs(hash(class_instance))
    if class_instance.__name__ in REDUCTION_TASKS:
        session_id = f"{class_instance.ObservationDate}{class_instance.ObservationRun}"
    else:
        session_id = f"{class_instance.PedestalRun}{class_instance.CalibrationRun}"
    # OSA specific
    # prov session is outside scripting and is run-wise
    # we may have different sessions/runs in the same log file

    system = get_system_provenance()
    log_record = {
        "session_id": session_id,
        "name": session_name,
        "startTime": start,
        "system": system,
        # OSA specific
        "observation_date": class_instance.ObservationDate,
        # OSA specific
        "software_version": class_instance.SoftwareVersion,
        "config_file": class_instance.ProcessingConfigFile,
        "config_file_hash": get_file_hash(class_instance.ProcessingConfigFile, buffer="path"),
        "config_file_hash_type": get_hash_method(),
    }
    if class_instance.__name__ in REDUCTION_TASKS:
        log_record["observation_run"] = class_instance.ObservationRun  # a session is run-wise
    else:
        log_record["pedestal_run"] = class_instance.PedestalRun
        log_record["calibration_run"] = class_instance.CalibrationRun
    log_prov_info(log_record)
    return session_id


def log_start_activity(activity, activity_id, session_id, start):
    """Log start of an activity."""
    log_record = {
        "activity_id": activity_id,
        "name": activity,
        "startTime": start,
        "in_session": session_id,
        "agent_name": os.getenv("USER", "Anonymous"),
        "script": sys.argv[0],
    }
    log_prov_info(log_record)


def log_finish_activity(activity_id, end):
    """Log end of an activity."""
    log_record = {"activity_id": activity_id, "endTime": end}
    log_prov_info(log_record)


def get_derivation_records(class_instance, activity):
    """Get log records for potentially derived entity."""
    records = []
    for var, pair in traced_entities.items():
        entity_id, item = pair
        value = get_nested_value(class_instance, var)
        if value:
            new_id = get_entity_id(value, item)
            if new_id != entity_id:
                log_record = {"entity_id": new_id, "progenitor_id": entity_id}
                records.append(log_record)
                traced_entities[var] = (new_id, item)
                logger.warning(f"Derivation detected in {activity} for {var}. ID: {new_id}")
    return records


def get_parameters_records(class_instance, activity, activity_id):
    """Get log records for parameters of the activity."""
    records = []
    parameter_list = definition["activities"][activity]["parameters"] or []
    if parameter_list:
        parameters = {}
        for parameter in parameter_list:
            if "name" in parameter and "value" in parameter:
                parameter_value = get_nested_value(class_instance, parameter["value"])
                if parameter_value:
                    parameters[parameter["name"]] = parameter_value
        if parameters:
            log_record = {"activity_id": activity_id, "parameters": parameters}
            records.append(log_record)
    return records


def get_usage_records(class_instance, activity, activity_id):
    """Get log records for each usage of the activity."""
    records = []
    usage_list = definition["activities"][activity]["usage"] or []
    for item in usage_list:
        props = get_item_properties(class_instance, item)
        if "id" in props:
            entity_id = props.pop("id")
            # record usage
            log_record = {
                "activity_id": activity_id,
                "used_id": entity_id,
            }
            if "role" in item:
                log_record.update({"used_role": item["role"]})
            # record entity
            log_record_ent = {
                "entity_id": entity_id,
            }
            if "entityName" in item:
                log_record_ent.update({"name": item["entityName"]})
            for prop in props:
                log_record_ent.update({prop: props[prop]})
            records.append(log_record_ent)
            records.append(log_record)
    return records


def log_generation(class_instance, activity, activity_id):
    """Log generated entities."""
    generation_list = definition["activities"][activity]["generation"] or []
    for item in generation_list:
        props = get_item_properties(class_instance, item)
        if "id" in props:
            entity_id = props.pop("id")
            # record generation
            if "value" in item:
                traced_entities[item["value"]] = (entity_id, item)
            log_record = {
                "activity_id": activity_id,
                "generated_id": entity_id,
            }
            if "role" in item:
                log_record.update({"generated_role": item["role"]})
            # record entity
            log_record_ent = {"entity_id": entity_id}
            if "entityName" in item:
                log_record_ent.update({"name": item["entityName"]})
            for prop in props:
                log_record_ent.update({prop: props[prop]})
            log_prov_info(log_record_ent)
            log_prov_info(log_record)
            if "has_members" in item:
                log_members(entity_id, item["has_members"], class_instance)
            if "has_progenitors" in item:
                log_progenitors(entity_id, item["has_progenitors"], class_instance)


def log_members(entity_id, subitem, class_instance):
    """Log members of and entity."""
    if "list" in subitem:
        member_list = get_nested_value(class_instance, subitem["list"]) or []
    else:
        member_list = [class_instance]
    for member in member_list:
        props = get_item_properties(member, subitem)
        if "id" in props:
            mem_id = props.pop("id")
            # record membership
            log_record = {
                "entity_id": entity_id,
                "member_id": mem_id,
            }
            # record entity
            log_record_ent = {"entity_id": mem_id}
            if "entityName" in subitem:
                log_record_ent.update({"name": subitem["entityName"]})
            for prop in props:
                log_record_ent.update({prop: props[prop]})
            log_prov_info(log_record_ent)
            log_prov_info(log_record)


def log_progenitors(entity_id, subitem, class_instance):
    """Log progenitors of and entity."""
    if "list" in subitem:
        progenitor_list = get_nested_value(class_instance, subitem["list"]) or []
    else:
        progenitor_list = [class_instance]
    for entity in progenitor_list:
        props = get_item_properties(entity, subitem)
        if "id" in props:
            progen_id = props.pop("id")
            # record progenitor link
            log_record = {
                "entity_id": entity_id,
                "progenitor_id": progen_id,
            }
            # record entity
            log_record_ent = {"entity_id": progen_id}
            for prop in props:
                log_record_ent.update({prop: props[prop]})
            log_prov_info(log_record_ent)
            log_prov_info(log_record)


# def log_file_generation(str_path, entity_name="", used=None, role="", activity_name=""):
#     """Log properties of a generated file."""
#
#     if used is None:
#         used = []
#     if Path(str_path).isfile():
#         method = get_hash_method()
#         item = dict(
#             file_path=str_path,
#             entityName=entity_name,
#         )
#         entity_id = get_entity_id(str_path, item)
#         log_record = {
#             "entity_id": entity_id,
#             "name": entity_name,
#             "location": str_path,
#             "hash": entity_id,
#             "hash_type": method,
#         }
#         log_prov_info(log_record)
#         if activity_name:
#             activity_id = get_activity_id()
#             log_record = {
#                 "activity_id": activity_id,
#                 "name": activity_name,
#             }
#             log_prov_info(log_record)
#             for used_entity in used:
#                 used_id = get_entity_id(used_entity, {})
#                 log_record = {
#                     "activity_id": activity_id,
#                     "used_id": used_id,
#                 }
#                 log_prov_info(log_record)
#             log_record = {
#                 "activity_id": activity_id,
#                 "generated_id": entity_id,
#             }
#             if role:
#                 log_record.update({"generated_role": role})
#             log_prov_info(log_record)
#         else:
#             for used_entity in used:
#                 used_id = get_entity_id(used_entity, {})
#                 log_record = {
#                     "entity_id": entity_id,
#                     "progenitor_id": used_id,
#                 }
#                 log_prov_info(log_record)


# ctapipe inherited code
#
#
def get_system_provenance():
    """
    Return JSON string containing provenance for all
    things that are fixed during the runtime.
    """

    bits, linkage = platform.architecture()

    return dict(
        # gammapy specific
        # version=get_info_version(),
        # dependencies=get_info_dependencies(),
        # envvars=get_info_envvar(),
        executable=sys.executable,
        platform=dict(
            architecture_bits=bits,
            architecture_linkage=linkage,
            machine=platform.machine(),
            processor=platform.processor(),
            node=platform.node(),
            version=str(platform.version()),
            system=platform.system(),
            release=platform.release(),
            libcver=str(platform.libc_ver()),
            num_cpus=psutil.cpu_count(),
            boot_time=datetime.datetime.fromtimestamp(psutil.boot_time()).isoformat(),
        ),
        python=dict(
            version_string=sys.version,
            version=platform.python_version(),
            compiler=platform.python_compiler(),
            implementation=platform.python_implementation(),
            packages=get_python_packages(),
        ),
        environment=get_env_vars(),
        arguments=sys.argv,
        start_time_utc=datetime.datetime.now().isoformat(),
    )


def get_env_vars():
    """Return env vars defined at the main scope of the script."""
    return {var: os.getenv(var, None) for var in _interesting_env_vars}
