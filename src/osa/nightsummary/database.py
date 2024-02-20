"""Query the TCU database source name and astronomical coordinates."""
import logging
from datetime import datetime
from typing import Tuple

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from osa.configs.config import cfg
from osa.utils.logging import myLogger

__all__ = ["query", "db_available", "get_run_info_from_TCU"]


log = myLogger(logging.getLogger(__name__))


CACO_DB = cfg.get("database", "caco_db")
TCU_DB = cfg.get("database", "tcu_db")


def db_available():
    """Check the connection to the TCU database."""
    caco_client = MongoClient(CACO_DB, serverSelectionTimeoutMS=3000)
    tcu_client = MongoClient(TCU_DB, serverSelectionTimeoutMS=3000)
    try:
        caco_client.server_info()
        tcu_client.server_info()
    except ConnectionFailure:
        log.warning("TCU or CaCo database not available. No source info will be added.")
        return False
    else:
        log.debug("TCU and CaCo database are available. Source info will be added.")
        return True


def query(obs_id: int, property_name: str):
    """
    Query the source name and coordinates from TCU database.

    Parameters
    ----------
    obs_id : int
        Run number
    property_name : str
        Properties from drive information e.g. `DriveControl_SourceName`,
        `DriveControl_RA_Target`, `DriveControl_Dec_Target`

    Returns
    -------
    query_result : str or None
        Query result from database. It can be either the source name or its coordinates.

    Raises
    ------
    ConnectionFailure
    """

    # Avoid problems with numpy int64 encoding in MongoDB
    if not isinstance(obs_id, int):
        obs_id = int(obs_id)

    caco_client = MongoClient(CACO_DB)
    tcu_client = MongoClient(TCU_DB)

    with caco_client, tcu_client:
        run_info = caco_client["CACO"]["RUN_INFORMATION"]
        run = run_info.find_one({"run_number": obs_id})

        try:
            start = datetime.fromisoformat(run["start_time"].replace("Z", ""))
            end = datetime.fromisoformat(run["stop_time"].replace("Z", ""))
        except TypeError:
            return None

        bridges_monitoring = tcu_client["bridgesmonitoring"]
        property_collection = bridges_monitoring["properties"]
        chunk_collection = bridges_monitoring["chunks"]
        descriptors = property_collection.find(
            {"property_name": property_name},
        )

        entries = {"name": property_name, "time": [], "value": []}

        for descriptor in descriptors:
            query_property = {"pid": descriptor["_id"]}

            if start is not None:
                query_property["begin"] = {"$gte": start}

            if end is not None:
                query_property["end"] = {"$lte": end}

            chunks = chunk_collection.find(query_property)

            for chunk in chunks:
                for value in chunk["values"]:
                    entries["time"].append(value["t"])
                    entries["value"].append(value["val"])

                    source_name = entries["value"][0]
                    return source_name if source_name != "" else None


def get_run_info_from_TCU(run_id: int, tcu_server: str) -> Tuple:
    """
    Get type of run, start, end timestamps (in iso format)
    and elapsed time (in minutes) for a given run ID.

    Parameters
    ----------
    run_id: int
    tcu_server: str
        Host of the TCU database

    Returns
    -------
    run_id : int
    run_type : str
    tstart
    tstop
    elapsed : float
        Elapsed time in minutes

    Notes
    -----
    TCU Monitoring database can be accessed at 'lst101-int' server
    from lstanalyzer@tcs06 and cp's
    """

    client = MongoClient(tcu_server)
    collection = client["lst1_obs_summary"]["camera"]
    summary = collection.find_one({"run_number": run_id})

    if summary is not None:
        run_type = summary["kind"]
        tstart = datetime.fromtimestamp(summary["tstart"]).isoformat(sep=" ", timespec="seconds")
        tstop = datetime.fromtimestamp(summary["tstop"]).isoformat(sep=" ", timespec="seconds")
        elapsed = (summary["tstop"] - summary["tstart"]) / 60  # minutes

        return run_id, run_type, tstart, tstop, elapsed

    return run_id, None, None, None, 0.0
