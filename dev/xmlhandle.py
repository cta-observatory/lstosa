import logging

from osa.utils.standardhandle import error, gettag

log = logging.getLogger(__name__)


def xmlhandleData(dom):
    tag = gettag()

    jobs = dom.getElementsByTagName("Job")
    return xmlhandleJobs(jobs)


def xmlhandleJobs(jobs):
    tag = gettag()

    return [xmlhandleJob(job) for job in jobs]


def xmlhandleJob(job):
    global name
    tag = gettag()

    try:
        name = xmlhandlejobkey(job.getElementsByTagName("Job_Name")[0])
    except IndexError as e:
        error(tag, "Could not get first element with Job_Name tag, {0}".format(e), 5)
    else:
        jobid = int(xmlhandlejobkey(job.getElementsByTagName("Job_Id")[0]).split(".", 1)[0])
        jobhost = None
        state = xmlhandlejobkey(job.getElementsByTagName("job_state")[0])
        cputime = None
        walltime = None
        exit = None
        if state in ["C", "R"]:
            try:
                jobhost = xmlhandlejobkey(job.getElementsByTagName("exec_host")[0])
            except IndexError as Error:
                if state == "C":
                    log.warning("Job {0} found not having an exec_host, {1}".format(jobid, Error))
                else:
                    error(tag, "Job {0} found not having an exec_host, {1}".format(jobid, Error), 6)
            else:
                # The cputime appears after some feedback from the pbs_mom within the resources_used tag
                # an initial period of indetermination is expected
                try:
                    cputime = xmlhandlejobkey(job.getElementsByTagName("cput")[0])
                except IndexError as Error:
                    log.warning("CPU Time not yet found for {0}, {1}".format(name, Error))
                    cputime = "00:00:00"
                try:
                    walltime = xmlhandlejobkey(job.getElementsByTagName("walltime")[0])
                except IndexError as Error:
                    log.warning("Wall time not yet found for {0}, {1}".format(name, Error))
                    wallime = "00:00:00"
                if state == "C":
                    try:
                        exit = int(xmlhandlejobkey(job.getElementsByTagName("exit_status")[0]))
                    except IndexError as Error:
                        log.warning(
                            "Malformed exit status in xml for jobid {0}, {1}".format(jobid, Error)
                        )
                        exit = None
    return {
        "name": name,
        "jobid": jobid,
        "state": state,
        "jobhost": jobhost,
        "cputime": cputime,
        "walltime": walltime,
        "exit": exit,
    }


def xmlhandlejobkey(name):
    tag = gettag()
    return "%s" % getText(name.childNodes)


def getText(nodelist):
    tag = gettag()
    rc = [node.data for node in nodelist if node.nodeType == node.TEXT_NODE]
    return "".join(rc)
