import logging

from osa.configs import options
from osa.utils.utils import stringify

log = logging.getLogger(__name__)


def send_stream(stream):
    """ TODO: use the email package from python """

    import subprocess

    commandargs = ["/usr/lib/sendmail", "-t", "-O", "ErrorMode=m"]
    if not options.simulate:
        try:
            subprocess.check_call(commandargs, stdin=stream)
        except subprocess.CalledProcessError as error:
            log.error(error)
    else:
        log.debug(f"Simulating: {stringify(commandargs)}")


def send_assignments(assignments):
    import tempfile

    content = set_content(assignments)
    if content:
        i = tempfile.SpooledTemporaryFile(mode="w+")
        i.write(content)
        i.seek(0, 0)
        send_stream(i)
    else:
        log.error(f"Empty assignments {assignments}")


def send_email(assignments):
    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(assignments["Content"])
    msg["Subject"] = assignments["Subject"]
    msg["From"] = "OSA Daemon <analysis@ana7.magic.iac.es>"
    msg["To"] = "<magic-onsite@gae.ucm.es>"
    try:
        s = smtplib.SMTP("localhost")
        s.sendmail("analysis@ana7.magic.iac.es", ["magic-onsite@gae.ucm.es"], msg.as_string())
        return True
    except smtplib.SMTPException:
        return False


def set_content(assignments):
    """ TODO: use the email package from python """

    content = "MIME-Version: 1.0\n"
    content += "Content-type: text/plain; charset=utf-8\n"
    content += "Content-Transfer-Encoding: quoted-printable\n"
    keys = ["From", "To", "Cc", "Bcc", "Reply-to", "Subject", "Content", "Signature"]
    if len(assignments) == 0:
        return None
    for key in keys:
        if key in assignments.keys():
            if key in ["Content", "Signature"]:
                content += "\n{1}".format(key, assignments[key])
            else:
                content += "{0}: {1}\n".format(key, assignments[key])
    return content
