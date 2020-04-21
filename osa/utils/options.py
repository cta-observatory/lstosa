# We share all these command line options as variables across different modules
# In order to modify them, import cliopts right after import options in the code
#
# This is the recmmended way to share variables according to
# http://docs.python.org/faq/programming.html#how-do-i-share-global-variables-across-modules
#
configfile = 'sequencer.cfg'
stdout = None
stderr = None
date = None
mode = None
tel_id = None
nightsum = 'True'
reason = None
directory = None
log_directory = None
simulate = None
noninteractive = None
verbose = None
warning = None
nocheck = None
compressed = None
lstchain_version = None
prod_id = None
