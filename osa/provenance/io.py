"""Provenance i/o conversion functions."""

import datetime
from pathlib import Path

import yaml
from prov.dot import prov_to_dot
from prov.model import ProvDocument

from osa.provenance.utils import get_log_config

# config
CONFIG_PATH = Path(__file__).resolve().parent / "config"
# LOGGER_FILE = CONFIG_PATH / "logger.yaml"
provconfig = yaml.safe_load(get_log_config())
PROV_PREFIX = provconfig["PREFIX"]
DEFAULT_NS = "id"  # "logprov"

__all__ = ["provlist2provdoc", "provdoc2graph", "provdoc2json", "read_prov"]


def provlist2provdoc(provlist):
    """
    Convert a list of provenance dictionaries to
    a provdoc W3C PROV compliant structure.
    """

    pdoc = ProvDocument()
    pdoc.set_default_namespace("param:")
    pdoc.add_namespace(DEFAULT_NS, DEFAULT_NS + ":")
    records = {}
    for provdict in provlist:
        if "session_id" in provdict:
            sess_id = DEFAULT_NS + ":" + str(provdict.pop("session_id"))
            if sess_id in records:
                sess = records[sess_id]
            else:
                sess = pdoc.entity(sess_id)
                records[sess_id] = sess
            sess.add_attributes(
                {
                    "prov:label": provdict.pop("name"),
                    "prov:type": "ExecutionSession",
                    "prov:generatedAtTime": provdict.pop("startTime"),
                    "software_version": provdict.pop("software_version"),
                    "observation_date": provdict.pop("observation_date"),
                    "observation_run": provdict.pop("observation_run"),
                    "config_file": provdict.pop("config_file"),
                    "config_file_hash": provdict.pop("config_file_hash"),
                    "config_file_hash_type": provdict.pop("config_file_hash_type"),
                }
            )
        # activity
        if "activity_id" in provdict:
            act_id = DEFAULT_NS + ":" + str(provdict.pop("activity_id")).replace("-", "")
            if act_id in records:
                act = records[act_id]
            else:
                act = pdoc.activity(act_id)
                records[act_id] = act
            # activity name
            if "name" in provdict:
                act.add_attributes({"prov:label": provdict.pop("name")})
            if "script" in provdict:
                act.add_attributes({"script": provdict.pop("script")})
            # activity start
            if "startTime" in provdict:
                act.set_time(startTime=datetime.datetime.fromisoformat(provdict.pop("startTime")))
            # activity end
            if "endTime" in provdict:
                act.set_time(endTime=datetime.datetime.fromisoformat(provdict.pop("endTime")))
            # in session?
            # if "in_session" in provdict:
            #     sess_id = DEFAULT_NS + ":" + str(provdict.pop("in_session"])
            #     pdoc.wasInfluencedBy(
            #         act_id, sess_id
            #     )  # , other_attributes={'prov:type': "Context"})
            # activity configuration
            if "agent_name" in provdict:
                agent_id = str(provdict.pop("agent_name"))
                if ":" not in agent_id:
                    agent_id = DEFAULT_NS + ":" + agent_id
                else:
                    new_ns = agent_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if agent_id in records:
                    agent = records[agent_id]
                else:
                    agent = pdoc.agent(agent_id)
                    records[agent_id] = agent
                # act.wasAssociatedWith(agent, attributes={"prov:role": "Creator"})
            if "parameters" in provdict:
                params_record = provdict.pop("parameters")
                params = {k: str(params_record[k]) for k in params_record}
                par = pdoc.entity(act_id + "_parameters", other_attributes=params)
                par.add_attributes({"prov:type": "Parameters"})
                par.add_attributes({"prov:label": "Parameters"})
                act.used(par, attributes={"prov:type": "Setup"})
            # usage
            if "used_id" in provdict:
                ent_id = str(provdict.pop("used_id"))
                if ":" not in ent_id:
                    ent_id = DEFAULT_NS + ":" + ent_id
                else:
                    new_ns = ent_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if ent_id in records:
                    ent = records[ent_id]
                else:
                    ent = pdoc.entity(ent_id)
                    records[ent_id] = ent
                rol = provdict.pop("used_role", None)
                # if rol:
                #     ent.add_attributes({'prov:label': rol})
                act.used(ent, attributes={"prov:role": rol})
            # generation
            if "generated_id" in provdict:
                ent_id = str(provdict.pop("generated_id"))
                if ":" not in ent_id:
                    ent_id = DEFAULT_NS + ":" + ent_id
                else:
                    new_ns = ent_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if ent_id in records:
                    ent = records[ent_id]
                else:
                    ent = pdoc.entity(ent_id)
                    records[ent_id] = ent
                rol = provdict.pop("generated_role", None)
                # if rol:
                #     ent.add_attributes({'prov:label': rol})
                ent.wasGeneratedBy(act, attributes={"prov:role": rol})
            for k, v in provdict.items():
                if k != "session_tag":
                    act.add_attributes({k: str(v)})
        # entity
        if "entity_id" in provdict:
            ent_id = str(provdict.pop("entity_id"))
            if ":" not in ent_id:
                ent_id = DEFAULT_NS + ":" + ent_id
            else:
                new_ns = ent_id.split(":").pop(0)
                pdoc.add_namespace(new_ns, new_ns + ":")
            if ent_id in records:
                ent = records[ent_id]
            else:
                ent = pdoc.entity(ent_id)
                records[ent_id] = ent
            if "name" in provdict:
                ent.add_attributes({"prov:label": provdict.pop("name")})
            if "type" in provdict:
                ent.add_attributes({"prov:type": provdict.pop("type")})
            if "value" in provdict:
                ent.add_attributes({"prov:value": str(provdict.pop("value"))})
            if "location" in provdict:
                ent.add_attributes({"prov:location": str(provdict.pop("location"))})
            # member
            if "member_id" in provdict:
                mem_id = str(provdict.pop("member_id"))
                if ":" not in mem_id:
                    mem_id = DEFAULT_NS + ":" + mem_id
                else:
                    new_ns = mem_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if mem_id in records:
                    mem = records[mem_id]
                else:
                    mem = pdoc.entity(mem_id)
                    records[mem_id] = mem
                ent.hadMember(mem)
            if "progenitor_id" in provdict:
                progen_id = str(provdict.pop("progenitor_id"))
                if ":" not in progen_id:
                    progen_id = DEFAULT_NS + ":" + progen_id
                else:
                    new_ns = progen_id.split(":").pop(0)
                    pdoc.add_namespace(new_ns, new_ns + ":")
                if progen_id in records:
                    progen = records[progen_id]
                else:
                    progen = pdoc.entity(progen_id)
                    records[progen_id] = progen
                ent.wasDerivedFrom(progen)
            for k, v in provdict.items():
                if k not in ["session_tag", "hash", "hash_type"]:
                    ent.add_attributes({k: str(v)})
        # agent
    return pdoc


def provdoc2graph(provdoc, filename, fmt):
    """Create a graph of a provenance workflow session."""
    dot = prov_to_dot(
        provdoc,
        use_labels=True,
        show_element_attributes=True,
        show_relation_attributes=False,
    )
    content = dot.create(format=fmt)
    with open(filename, "wb") as f:
        f.write(content)


def provdoc2json(provdoc, filename):
    json_stream = provdoc.serialize(indent=4)
    with open(filename, "w") as f:
        f.write(json_stream)


def read_prov(filename="prov.log", start=None, end=None):
    """Read a list of provenance dictionaries from the logfile."""
    start_dt = datetime.datetime.fromisoformat(start) if start else None
    end_dt = datetime.datetime.fromisoformat(end) if end else None
    prov_list = []
    with open(filename, "r") as f:
        for line in f.readlines():
            ll = line.split(PROV_PREFIX)
            if len(ll) >= 2:
                prov_str = ll.pop()
                prov_dt = datetime.datetime.fromisoformat(ll.pop())
                keep = True
                if prov_dt:
                    if start and prov_dt < start_dt:
                        keep = False
                    if end and prov_dt > end_dt:
                        keep = False
                if keep:
                    prov_dict = yaml.safe_load(prov_str)
                    prov_list.append(prov_dict)
    return prov_list
