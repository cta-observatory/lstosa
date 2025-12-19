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

TCU_DB = cfg.get("database", "tcu_db")

def db_available():
    """Check the connection to the TCU database."""
    tcu_client = MongoClient(TCU_DB, serverSelectionTimeoutMS=3000)
    try:
        tcu_client.server_info()
    except ConnectionFailure:
        log.warning("TCU database not available. No source info will be added.")
        return False
    else:
        log.debug("TCU database is available. Source info will be added.")
        return True

def query(obs_id: int):
    """
    Query the source name and coordinates from TCU database.

    Parameters
    ----------
    obs_id : int
        Run number


    Returns
    -------
    query_result : Dict
        Query result from database. It can be either the source name or its coordinates.

    Raises
    ------
    ConnectionFailure
    """

    # Avoid problems with numpy int64 encoding in MongoDB
    if not isinstance(obs_id, int):
        obs_id = int(obs_id)

    try:
        tcu_client = MongoClient(TCU_DB, serverSelectionTimeoutMS=3000)
        db = tcu_client["lst1_obs_summary"]
        camera_col = db["camera"]

        run_info = camera_col.find_one({"run_number": obs_id})

        if not run_info:
            log.info(f"Run {obs_id} not found 'lst1_obs_summary.camera'")
        else:

            tstart = run_info.get("tstart")
            tstop = run_info.get("tstop")
            run_type = run_info.get("kind")

            tstart_iso = datetime.fromtimestamp(tstart).isoformat(sep=" ", timespec="seconds")

            log.info(f"Run {obs_id} ({run_type}) found.")
            log.info(f"Time: {tstart_iso} (Timestamp: {tstart})")

            telescope_col = db["telescope"]
            query = {
                "tstart": {"$lte": tstop},
                "tstop": {"$gte": tstart}
            }

            tel_doc = telescope_col.find_one(query, sort=[("tstart", -1)])

            if tel_doc:

                config = tel_doc.get("data", {}).get("structure", [])[0]
                target = config.get("target", {})
                source_name = target.get("name", "Desconocido")
                ra = target.get("source_ra", "N/A")
                dec = target.get("source_dec", "N/A")
                return {"source_name": source_name, "ra": ra, "dec": dec}
            else:
                log.info("\nNo information found for that time range in 'lst1_obs_summary.telescope'.")

    except Exception as e:
        log.info(f"ERROR: {e}")

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
