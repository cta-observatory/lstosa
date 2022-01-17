"""Query the TCU database source name and astronomical coordinates."""

from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

__all__ = ['query']


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

    caco_client = MongoClient("tcs01")
    tcu_client = MongoClient("tcs05")

    try:
        caco_client.admin.command('ping')
        tcu_client.admin.command('ping')
    except ConnectionFailure:
        raise ConnectionFailure("Databases not available")

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
            {'property_name': property_name},
        )

        entries = {
            'name': property_name,
            'time': [],
            'value': []
        }

        for descriptor in descriptors:
            query_property = {'pid': descriptor['_id']}

            if start is not None:
                query_property.update({"begin": {"$gte": start}})

            if end is not None:
                query_property.update({"end": {"$lte": end}})

            chunks = chunk_collection.find(query_property)

            for chunk in chunks:
                for value in chunk['values']:
                    entries['time'].append(value['t'])
                    entries['value'].append(value['val'])

                    source_name = entries["value"][0]
                    if source_name != "":
                        return source_name
                    else:
                        return None
