from standardhandle import output, verbose, error, gettag, errornonfatal,\
 stringify
import options
##############################################################################
#
# send_command_file
#
##############################################################################
def send_stream(stream):
    tag = gettag()
    """ TODO: use the email package from python """

    import subprocess
    commandargs = ['/usr/lib/sendmail', '-t', '-O', 'ErrorMode=m']
    if not options.simulate:
        try:
            subprocess.check_call(commandargs, stdin=stream)
        except subprocess.CalledProcessError as e:
            errornonfatal(tag, e.output)
    else:
        verbose(tag, "Simulating: {0}".stringify(commandargs))
##############################################################################
#
# send_file
#
##############################################################################
def send_file(file):
    tag = gettag()
    with open(file, 'r') as i:
        send_command_stream(i)
##############################################################################
#
# send_content
#
##############################################################################
def send_assignments(assignments):
    tag = gettag()
    import tempfile
    content = set_content(assignments)
    if content:
        i = tempfile.SpooledTemporaryFile('w+')
        i.write(content)
        i.seek(0, 0)
        send_stream(i)
    else:
        errornonfatal(tag, "Empty assignments {0}".format(assignments))
##############################################################################
#
# send_email
#
##############################################################################
def send_email(assignments):
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(assignments['Content'])
    msg['Subject'] = assignments['Subject']
    msg['From'] = 'OSA Daemon <analysis@ana7.magic.iac.es>'
    msg['To']   = '<magic-onsite@gae.ucm.es>'
    try:
        s = smtplib.SMTP('localhost')
        s.sendmail('analysis@ana7.magic.iac.es', ['magic-onsite@gae.ucm.es'],
                   msg.as_string())
        return True
    except smtplib.SMTPException:
        return False
##############################################################################
#
# set_headers 
#
##############################################################################
def set_content(assignments):
    tag = gettag()
    """ TODO: use the email package from python """

    content = "MIME-Version: 1.0\n"
    content += "Content-type: text/plain; charset=utf-8\n"
    content += "Content-Transfer-Encoding: quoted-printable\n"
    keys = ['From', 'To', 'Cc', 'Bcc', 'Reply-to', 'Subject', 'Content', 'Signature']
    if len(assignments) != 0:
        for key in keys:
            if key in assignments.keys():
                if key == 'Content' or key == 'Signature':
                    content += "\n{1}".format(key, assignments[key])
                else:
                    content += "{0}: {1}\n".format(key, assignments[key])
        return content
    else:
        return None
