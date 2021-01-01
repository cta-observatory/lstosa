.. _howtouseit:

*****************
How to use LSTOSA
*****************

The main script is ``osa/scripts/sequencer.py``, which reads the Night Summary, creates the analysis directory, extract
run sequences and launches them using ``sbatch``.

First, activate ``osa`` environment and go to the  then run sequencer.py specifying the date and the telescope, for the
time being only ``LST1`` is available.

.. code-block:: bash

    (osa) [daniel.morcuende@cp02 lstosa]$ python osa/scripts/sequencer.py -c cfg/mysequencer.cfg -d 2020_01_27 LST1
    ============================== Starting sequencer.py at 2021-01-01 20:10:32 UTC for LST, Telescope: LST1, Night: 2020_01_27 ==============================
    Tel   Seq  Parent  Type  Run   Subruns  Source  Wobble  Action  Tries  JobID  State  Host  CPU_time  Walltime  Exit  DL1%  DATACHECK%  MUONS%  DL2%
    LST1    0  None    CALI  1873        5  None    None    None        0  None   None   None  None      None      None  None  None        None    None
    LST1    1       0  DATA  1874      194  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    2       0  DATA  1875      209  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    3       0  DATA  1876      225  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    4       0  DATA  1877      202  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    5       0  DATA  1878       74  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    6       0  DATA  1879      207  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    7       0  DATA  1880      203  None    None    None        0  None   None   None  None      None      None     0           0       0     0
    LST1    8       0  DATA  1881      207  None    None    None        0  None   None   None  None      None      None     0           0       0     0

A table containing all the sequences (or runs) for a given day is shown. You can also monitor the status of the
processing of the file, stated in the last columns. When launching the sequencer script, jobs are submitted for each
run listed in the table. First a ``CALIBRATION`` sequence is executed (it takes around 1 hour) and then rest of the
``DATA`` sequences (they also take around 1 hour in total to go from R0 to DL2). Data sequences depend on the
calibration sequence and they only start whenever the parent calibration sequence finishes without problems.

If you want to use LSTOSA from your personal account @ LST-IT container, you just need to set the proper paths in
the ``sequencer.cfg`` config file.

Useful options:

* Before executing sequencer to submit any job, it is recommended to use first the simulate option ``-s``. With
  this option enabled, the sequencer is just simulated and no jobs are launched.

* To know more about what happens internally when you run sequencer.py use the verbose mode ``-v``. An example output
  is shown below:

.. code-block:: bash

    (osa) [daniel.morcuende@cp02 lstosa]$ python osa/scripts/sequencer.py -v -c cfg/mysequencer.cfg -s -d 2020_01_27 LST1
    ============================== Starting sequencer.py at 2021-01-01 20:10:32 UTC for LST, Telescope: LST1, Night: 2020_01_27 ==============================
    2021-01-01 20:10:32,473 DEBUG [osa.utils.utils] (utils.getnightdirectory): Getting analysis path for tel_id LST1
    2021-01-01 20:10:32,473 DEBUG [osa.utils.utils] (utils.get_prod_id): Getting the prod ID for the running analysis directory: v0.6.3_v05
    2021-01-01 20:10:32,473 DEBUG [osa.utils.utils] (utils.getnightdirectory): Analysis directory: /fefs/aswg/workspace/daniel.morcuende/data/real/running_analysis/20200127/v0.6.3_v05
    2021-01-01 20:10:32,474 DEBUG [osa.nightsummary.nightsummary] (nightsummary.read_nightsummary): Night summary file path: /home/daniel.morcuende/lstosa/NightSummary/NightSummary_20200127.txt
    2021-01-01 20:10:32,474 DEBUG [osa.nightsummary.nightsummary] (nightsummary.read_nightsummary): Night summary:
     01872    5 DRS4  2020-01-27 19:51:44 0001 1580154753739954334 5739954100 0001 1580154753739954334 5739951300
     01873    5 CALI  2020-01-27 20:23:43 0001 1580156670887160057 1887159800 0001 1580156670887160057 1887158800
     01874  194 DATA  2020-01-27 20:44:13 0003 1580157904186709543 5186709300 0003 1580157904186709543 5186708700
     01875  209 DATA  2020-01-27 21:05:44 0003 1580159197411578464 7411578200 nan nan nan
     01876  225 DATA  2020-01-27 21:27:20 0001 1580160490575729635 7575729400 nan nan nan
     01877  202 DATA  2020-01-27 21:51:28 0001 1580161935735383476 1735383200 nan nan nan
     01878   74 DATA  2020-01-27 22:13:34 0001 1580163263237149740 2237149500 nan nan nan
     01879  207 DATA  2020-01-27 22:33:06 0003 1580164436408793971 3408793700 nan nan nan
     01880  203 DATA  2020-01-27 22:55:31 0003 1580165786211720504 7211720200 nan nan nan
     01881  207 DATA  2020-01-27 23:17:52 0001 1580167122989548546 3989548300 nan nan nan

    2021-01-01 20:10:32,477 DEBUG [osa.nightsummary.extract] (extract.extractsubruns): Subrun list extracted
    2021-01-01 20:10:32,477 DEBUG [osa.nightsummary.extract] (extract.extractruns): Run list extracted
    2021-01-01 20:10:32,477 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new PED run 1872 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new CAL run 1873 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1874 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1875 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1876 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1877 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1878 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1879 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1880 for None
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Detected a new DATA run 1881 for None
    ...
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): The storage contains 8 data sequences
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1872, type DRS4 to sequence 0
    2021-01-01 20:10:32,478 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1873, type CALI to sequence 0
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): Sequence 0 assigned to run 1873 whose parent is None with run 1872
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1874, type DATA to sequence 1
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): Sequence 1 assigned to run 1874 whose parent is 0 with run 1873
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1875, type DATA to sequence 2
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): Sequence 2 assigned to run 1875 whose parent is 0 with run 1873
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1876, type DATA to sequence 3
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): Sequence 3 assigned to run 1876 whose parent is 0 with run 1873
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1877, type DATA to sequence 4
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): Sequence 4 assigned to run 1877 whose parent is 0 with run 1873
    2021-01-01 20:10:32,479 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): trying to assign run 1878, type DATA to sequence 5
    ...
    2021-01-01 20:10:32,480 DEBUG [osa.nightsummary.extract] (extract.generateworkflow): Workflow completed
    2021-01-01 20:10:32,480 DEBUG [osa.nightsummary.extract] (extract.extractsequences): Sequence list extracted
    2021-01-01 20:10:32,480 DEBUG [osa.jobs.job] (job.preparejobs): Creating sequence.txt and sequence.py for sequence 0
    2021-01-01 20:10:32,480 DEBUG [osa.jobs.job] (job.createsequencetxt): SIMULATE Creating sequence txt /fefs/aswg/workspace/daniel.morcuende/data/real/running_analysis/20200127/v0.6.3_v05/sequence_LST1_01873.txt
    2021-01-01 20:10:32,480 DEBUG [osa.jobs.job] (job.preparejobs): Creating sequence.txt and sequence.py for sequence 1
    ...
    2021-01-01 20:10:32,596 DEBUG [osa.jobs.job] (job.submitjobs): SIMULATE Launching scripts
    2021-01-01 20:10:32,596 DEBUG [osa.jobs.job] (job.submitjobs): ['sbatch', '--parsable', '--export=ALL,MPLBACKEND=Agg', '/fefs/aswg/workspace/daniel.morcuende/data/real/running_analysis/20200127/v0.6.3_v05/sequence_LST1_01873.py']
    2021-01-01 20:10:32,596 DEBUG [osa.jobs.job] (job.submitjobs): Adding dependencies to job submission


