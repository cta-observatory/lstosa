#!/usr/bin/env python2.7

from subprocess import call, check_output
import xml.dom.minidom
import xmlhandle
import sys

""" Read the argument """

if len(sys.argv) != 2 or (sys.argv[1] != 'suspend' and sys.argv[1] != 'resume' and sys.argv[1] != 'kill'):
    sys.stderr.write("Error. Usage osa_jobs.py <action>\n   where action: suspend, resume, kill\n")
    sys.exit(1)
    
argument = sys.argv[1]

""" Check the jobs in queue """

commandargs = ['qstat', '-x']
xml_output = check_output(commandargs)

""" We have to parse the xml """
document = xml.dom.minidom.parseString(xml_output)
queue_list = xmlhandle.xmlhandleData(document)

""" 
The queue list contains element like
{'name': u'STDIN', 'jobhost': u'fcana2/0', 'cputime': u'00:00:00', 'jobid': 9412, 'state': u'C', 'exit': 0, 'walltime': u'00:00:17'}
"""

for job in queue_list:
    if job['state']:
        if job['state'] != 'C':
            if (argument == 'suspend' and job['state'] != 'S') or (argument == 'resume' and job['state'] == 'S'):
                print "{0}ing jobid {1}, {2}...".format(argument, job['jobid'], job['name']),
                commandargs = ['qsig', '-s', argument, str(job['jobid'])]
                rc = call(commandargs)
                if rc == 0:
                    print "Success!"
                else:
                    print "Failed!"
    else:
        print "Warning, no state for jobid {0}".format(job['jobid'])
