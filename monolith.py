#!/usr/bin/env python2.7
from standardhandle import output, warning, error, stringify
##############################################################################
#
# monolith
#
##############################################################################
def monolith():
    tag = monolith.__name__
    import datetime
    import report
    import utils
    import options, cliopts

    # Initiating report
    now = datetime.datetime.utcnow()
    report.header("Starting {0} at {1} UTC for MAGIC, Telescope: {2}".format(tag, now.strftime("%Y-%m-%d %H:%M:%S"), options.tel_id))


    # Building the set of days
    datadays = None
    if options.tel_id == 'M1' or options.tel_id == 'M2':
        datadays = utils.getrawdatadays()
    else:
        """ The days in which we have stereo data sequences """
        datadays = utils.getstereodatadays()
    finisheddays = utils.getfinisheddays()
    unfinisheddays = utils.sorted_nicely(datadays - finisheddays)

    # Finalizing report
    report.rule()
    loopaskingsequencer(unfinisheddays)
##############################################################################
#
# loopaskingsequencer
#
##############################################################################
def loopaskingsequencer(unfinisheddays):
    tag = loopaskingsequencer.__name__
    for day in unfinisheddays:
        if options.noninteractive:
            launchsequencer(day)
        else:
            answercheck = True
            while (answercheck):
                try:
                    answer = raw_input("OSA {0} {1} is not finished. Run sequencer? (y/n): ".format(options.tel_id, day))
                except KeyboardInterrupt:
                    import sys
                    print ''
                    warning(tag, "Program quitted by user not willing to answer")
                    sys.exit(1)
                except EOFError as e:
                    error(tag, "End of file not expected", e)
                else:           
                    answercheck = False
                    if answer == 'y' or answer == 'Y':
                        launchsequencer(day)
                    elif answer == 'n' or answer == 'N':
                        continue
                    else:
                        warning(tag, "Answer not understood, please type y or n")
                        answercheck = True
##############################################################################
#
# launchsequencer
#
##############################################################################
def launchsequencer(day):
    tag = launchsequencer.__name__
    import subprocess
    from os.path import join
    import config
    commandargs = [join(config.cfg.get('OSA', 'PYTHONDIR'), 'sequencer.py')]
    if options.configfile:
        commandargs.append('-c')
        commandargs.append(options.configfile)
    if options.simulate:
        commandargs.append('-s')
    if options.warning:
        commandargs.append('-w')
    if options.verbose:
        commandargs.append('-v')
    commandargs.append('-d')
    commandargs.append(day)
    commandargs.append(options.tel_id)
    if options.simulate:
        output(tag, "{0}".format(stringify(commandargs)))
    else:
        try:
            stdout = subprocess.check_output(commandargs)
        except OSError as (ValueError, NameError):
            error(tag, "Calling {0} failed, {1}".format(stringify(commandargs), NameError), ValueError)
        else:
            output(tag, stdout)
##############################################################################
#
# MAIN
#
##############################################################################
if __name__ == '__main__':

    import sys
    import os.path
    tag = os.path.basename(__file__)
    import options, cliopts
    # Set the options through cli parsing
    cliopts.monolithcliparsing(sys.argv[0])

    # Define the telescope array
    tel_array = []
    if options.tel_id:
        tel_array.append(options.tel_id)
    else:
        tel_array = ['M1', 'M2', 'ST']
    # Run the routine as many times as required in the telescope array
    for tel in tel_array:
        options.tel_id = tel
        monolith()
