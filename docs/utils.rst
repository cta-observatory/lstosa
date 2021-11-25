.. _utils:

Utilities
=========
Functions to deal with command lines argument and options, dates, production IDs, directories and moving files.

Command line arguments and options
----------------------------------
Input arguments and options for LSTOSA scripts are set in ``osa.utils.cliopts``.

Reference/API
+++++++++++++
.. automodule:: osa.utils.cliopts
   :members:
   :undoc-members:

Moving files and DB register
----------------------------
Move analysis products (calibration, DL1, data-checks, muons and DL2 files) to their final destinations
and register them in the data base (the latter actions is not implemented yet).

Reference/API
+++++++++++++
.. automodule:: osa.utils.register
   :members:

Utility functions
-----------------
Handling of dates, directories and production IDs.

Reference/API
+++++++++++++
.. automodule:: osa.utils.utils
   :members:

I/O
-----------------
Reading and writing files.

Reference/API
+++++++++++++
.. automodule:: osa.utils.iofile
   :members:
