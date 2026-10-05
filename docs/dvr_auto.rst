.. _dvr_auto:

========
DVR-AUTO
========

Automation of the Data Volume Reduction (R0G -> R0V) for LST-1
===============================================================

.. contents:: Table of contents
   :local:
   :depth: 2


Overview
--------

``dvr_auto`` turns the manual DVR procedure described in section 6.6.2 of the
documentation into a single command. Previously, the operator had to:

* create ``date_list.txt`` by hand with the dates that have DL1 data,
* run ``find_runs.py`` to generate ``all_runs.txt``,
* launch ``run_dvr_settings.sh`` and ``run_dvr_pixmask.sh``,
* check the logs with ``ugrep`` and move the ``Pixel_selection_LST*.h5`` files,
* create the monthly date file by hand and launch ``data_reduction_lstchain``,
* run ``count_subruns.sh`` for R0G and R0V, ``diff`` the results and copy any
  missing files.

All of this is now a sequence of six stages that run in order::

    dates -> find_runs -> settings -> pixmask -> r0v -> verify

Usage
~~~~~

.. code-block:: console

    $ python -m dvr_auto.cli START_DATE [END_DATE] [options]

Dates use the ``YYYYMMDD`` format. If the end date is omitted, the last date
available in the input data is used.

Design principles
~~~~~~~~~~~~~~~~~

* **Everything is configurable.** All paths and parameters live in
  ``config.yaml``; nothing is hard-coded. *Profiles* (``cp02`` and
  ``lstanalyzer``) allow switching machines without touching the code.
* **Every stage is idempotent.** Each stage inspects the disk and only does
  what is still missing, so the pipeline can be relaunched without repeating
  work. There is no state file that could get out of sync.
* **Jobs are tracked with** ``sacct``. Unlike ``squeue``, ``sacct`` can
  distinguish a failed job from one that finished correctly (a failed job
  simply disappears from the queue).
* **Failures are explicit.** If anything fails, the pipeline stops, reports it
  clearly on screen and in ``pipeline.log``, and exits with code ``1``.
* **Incomplete data is never trusted.** Copies are written to a ``.part`` file
  and renamed on completion; partial outputs of failed jobs are deleted so
  they can be redone.


Project layout
--------------

.. code-block:: text

    dvr_auto/                     <- project directory
    |-- config.yaml               paths, environments, SLURM parameters
    |-- README.md                 usage summary
    |-- output/                   outputs of the tests on cp02
    `-- dvr_auto/                 <- the Python package
        |-- __init__.py           (empty, makes it a package)
        |-- cli.py                entry point, arguments, logging
        |-- config.py             YAML reading and profiles
        |-- common.py             shared functions
        |-- slurm.py              job submission and waiting with sacct
        |-- pipeline.py           stage orchestration
        `-- stages/
            |-- __init__.py       (empty)
            |-- dates.py          stage 1
            |-- find_runs.py      stage 2
            |-- masks.py          stages 3 and 4 (settings and pixmask)
            |-- r0v.py            stage 5
            `-- verify.py         stage 6


Configuration (``config.yaml``)
-------------------------------

This is the only file that needs to be edited to change paths or environments.
It has a common section and a ``profiles`` block with the paths that differ
between machines. At run time, the common section is taken and the values of
the selected profile are overlaid on top of it (selected with ``--profile``,
or taken from the ``profile:`` entry on the first line).

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Section
     - Description
   * - ``profile``
     - Default profile (``cp02`` or ``lstanalyzer``).
   * - ``paths``
     - Input and output paths (see below).
   * - ``env``
     - Job environment: ``conda_sh`` (path to ``conda.sh``, needed for
       ``conda activate``), ``lstchain_env`` (lstchain environment activated by
       each job) and ``extra_lines`` (extra lines at the start of each job,
       e.g. ``LD_LIBRARY_PATH``).
   * - ``slurm``
     - ``account``, ``poll_seconds`` (seconds between ``sacct`` queries),
       ``max_queued_jobs`` (cap on queued jobs) and, for each job type
       (``settings``, ``pixmask``, ``r0v``), the partition and extra memory
       arguments.
   * - ``logs``
     - ``success_marker``: word that must appear in a log for it to be
       considered successful (like ``ugrep -L success`` in the manual
       procedure). ``check_masks``: also require that word in the settings /
       pixmask ``.out`` files (default ``false``: ``sacct`` is trusted).
       ``check_r0v``: require it in the ``dvr_<run>_<subrun>.log`` logs
       (default ``true``).
   * - ``r0v``
     - ``chunk_threshold`` / ``chunk_size``: runs with many subruns are split
       into several jobs (defaults: 190 and 100), as the official launcher
       does.
   * - ``pipeline``
     - ``max_retries``: number of times R0V is retried, only for the missing
       subruns (default ``0``: no retries).
   * - ``profiles``
     - One block per machine with the output paths. ``cp02`` writes to your
       workspace without touching production; ``lstanalyzer`` writes to the
       real production paths.

Paths
~~~~~

**Inputs** (read-only; nothing is ever written to them):

``dl1_root``
    Root of the DL1 data (``/fefs/onsite/data/lst-pipe/LSTN-01/DL1``).
``dl1_version``
    Version subfolder inside each day (``v0.11``).
``run_summary_dir``
    Location of the ``RunSummary_YYYYMMDD.ecsv`` files.
``r0v_input_root``
    Root of the raw R0G data. It must contain one folder per day
    (``root/20260909/...``).

**Outputs** (defined in each profile):

``r0v_output_root``
    ``R0V/<date>/`` is created here with the reduced files, and
    ``R0V/log/<date>/`` with the logs.
``pixmask_dir``
    Where the newly produced ``Pixel_selection_LST*.h5`` files are moved.
``pixmask_extra_dirs``
    **Read-only** directories where already existing masks are looked up
    (e.g. the production ones).
``workdir``
    Where the pipeline stores, for each execution, its date/run lists,
    scripts, logs and ``pipeline.log``.


Command-line interface (``cli.py``)
-----------------------------------

``cli.py`` is the entry point (``python -m dvr_auto.cli``). In order, it:

1. Reads the arguments:

   .. list-table::
      :widths: 25 75

      * - ``start``
        - Start date (mandatory).
      * - ``end``
        - End date (optional).
      * - ``--config FILE``
        - Configuration file; defaults to ``config.yaml``.
      * - ``--profile NAME``
        - ``cp02`` or ``lstanalyzer``.
      * - ``--only STAGE``
        - Run only that stage.
      * - ``--from-stage STAGE``
        - Run from that stage until the end.
      * - ``--dry-run``
        - Write scripts but do not launch jobs or copy files.
      * - ``--force``
        - Ignore already existing masks/outputs.

2. Loads the configuration and applies the profile.
3. If no end date was given, uses the last date available in the input.
4. Creates the working directory ``<workdir>/<start>_<end>/`` and configures
   logging (to screen and to ``<workdir>/<start>_<end>/pipeline.log``).
5. Builds the context (config, dates, slurm, flags) and calls
   ``run_pipeline`` with the selected stages.
6. If a stage raises a controlled error (``StageError``), it is reported as
   ``PIPELINE FAILED`` and the program exits with code ``1``. If everything
   goes well, ``PIPELINE DONE`` is printed and the exit code is ``0``. This
   exit code makes it possible to run the pipeline from a cron job.


Modules
-------

``config.py``
~~~~~~~~~~~~~

Defines the ``Config`` class, which:

* reads ``config.yaml``;
* extracts the ``profiles`` block and overlays the chosen profile on the
  common section (recursive merge: only what the profile defines is
  overridden);
* checks that the mandatory output paths (``r0v_output_root``,
  ``pixmask_dir``, ``workdir``) are defined, aborting with a clear message
  instead of failing later;
* provides helpers: ``path("key")`` (returns a ``Path``), ``dl1_version`` and
  ``job_preamble()``, which returns the first lines of every job script:

  .. code-block:: bash

      #!/bin/bash
      source <conda_sh>
      conda activate <lstchain_env>
      export LD_LIBRARY_PATH=/usr/lib64/

Thanks to this, each job activates its own lstchain environment and the
orchestrator does not need to switch conda environments between steps.

``common.py``
~~~~~~~~~~~~~

Shared functions and classes:

.. list-table::
   :widths: 25 75

   * - ``StageError``
     - Exception for "controlled" stage errors.
   * - ``Context``
     - Groups what every stage needs: config, start/end dates, working
       directory, slurm object, and the ``dry_run`` and ``force`` flags.
   * - ``list_dates``
     - Lists the ``YYYYMMDD``-named folders of a directory within a range
       (optionally requiring a subfolder, such as the DL1 version).
   * - ``run_id_from_text``
     - Extracts the run number from a text (``"Run12345"``).
   * - ``read_run_types``
     - Reads ``RunSummary_<date>.ecsv`` and returns ``{run: type}`` (``DATA``,
       ``PEDCALIB``, ...). Returns ``None`` if the file does not exist.
   * - ``find_subruns``
     - Walks a raw-data directory and returns
       ``{run: {subrun: [files of each stream]}}`` from file names of the form
       ``LST-1.<stream>.Run<run>.<subrun>.fits.fz``.
   * - ``copy_files``
     - Safe copy: first to ``*.part``, then rename. If the destination already
       exists, does nothing.
   * - ``mask_dirs``
     - List of directories in which to look for masks: ``pixmask_dir`` plus
       ``pixmask_extra_dirs``.
   * - ``has_pixmask``
     - Tells whether a run already has any mask in those directories.
   * - ``find_pixmask``
     - Returns the path of the mask for a given ``(run, subrun)``, or ``None``
       if it does not exist.
   * - ``has_marker``
     - Tells whether a log file contains the success word (case-insensitive).

``slurm.py``
~~~~~~~~~~~~

The ``Slurm`` class is a thin wrapper around ``sbatch`` and ``sacct``.

``submit(...)``
    Launches a script with ``sbatch --parsable`` and returns the job ID
    directly (without depending on any output printed by other scripts).
    Before submitting, if the user already has ``max_queued_jobs`` in the
    queue, it waits (to avoid saturating SLURM, as the commented-out
    ``check_job_status_and_wait`` line in the official launcher intended). In
    ``--dry-run`` mode nothing is submitted: it logs what it would do and
    returns fake identifiers (``DRY1``, ``DRY2``, ...).

``wait(...)``
    Queries ``sacct`` every ``poll_seconds`` until all jobs leave the
    ``PENDING``/``RUNNING``/etc. states. Returns ``{job_id: final_state}``
    (``COMPLETED``, ``FAILED``, ``TIMEOUT``, ``OUT_OF_MEMORY``,
    ``CANCELLED``, ...). If a job takes a while to show up in ``sacct``
    (accounting delay) it is given some margin; after 10 queries without
    appearing it is marked ``UNKNOWN`` so the pipeline does not wait forever.

``pipeline.py``
~~~~~~~~~~~~~~~

Defines the stage order (``ORDER``) and runs the stages:

``dates, find_runs, settings, pixmask, r0v, verify``

Special case: when ``r0v`` and ``verify`` run together, they are placed in a
loop. ``r0v`` is executed, then verified, and if DATA subruns are missing,
``r0v`` is repeated (it only processes what is missing, since it is
idempotent) until ``max_retries`` is exhausted. If something is still missing
after that, a ``StageError`` is raised and the pipeline ends with failure. In
``--dry-run`` mode nothing is verified or retried, because nothing real has
been generated.


Stages
------

All stages live in ``dvr_auto/stages/``.

Stage 1: ``dates.py``
~~~~~~~~~~~~~~~~~~~~~

*Replaces creating* ``date_list.txt`` *by hand with vim.*

:Inputs: ``dl1_root``, ``r0v_input_root``, RunSummary.
:Output: ``<workdir>/<start>_<end>/date_list.txt``

* Lists the days of the range that have DL1 (``dl1_root/<day>/<version>``).
* Writes those days to ``date_list.txt``.
* Automates the manual check from the documentation: for every day with raw
  data but no DL1, it consults the RunSummary and warns:

  - if there are no DATA runs, this is expected (``INFO``);
  - if there are DATA runs, or there is no RunSummary, it writes a
    ``WARNING`` so that you can review it.

Stage 2: ``find_runs.py``
~~~~~~~~~~~~~~~~~~~~~~~~~

*Replaces the original* ``find_runs.py``.

:Inputs: ``date_list.txt``, DL1, RunSummary.
:Output: ``<workdir>/<start>_<end>/all_runs.txt``

* For each day in ``date_list.txt``, reads its RunSummary and keeps the
  ``DATA`` runs.
* For each run, finds the first ``tailcut*`` directory containing any DL1 file
  of that run and writes one line with the pattern
  ``.../tailcutXX/dl1_LST-1.Run<run>.????.h5``.
* The format of ``all_runs.txt`` is the same as the current one and is what
  ``lstchain_dvr_pixselector`` consumes.
* Fixes with respect to the original script: the DL1 version is no longer
  hard-coded; the tailcut where the run was actually found is used (before,
  the last one in the list was written); and subruns are no longer iterated
  one by one.

Stages 3 and 4: ``masks.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Replaces* ``run_dvr_settings.sh``, ``run_dvr_pixmask.sh``, *the* ``ugrep``
*check and the* ``mv``.

:Input: ``all_runs.txt``.
:Outputs: scripts, logs and h5 files in ``<workdir>/<...>/pixmask_work/``, and
          final masks in ``pixmask_dir``.

Common to both stages:

* Computes which runs are *pending*: those in ``all_runs.txt`` that do not yet
  have a mask in any of the mask directories (unless ``--force`` is used).
* For each pending run, writes a script (with the preamble that activates
  lstchain) and submits it with ``sbatch``. One job per run.
* Waits for all jobs with ``sacct``. If any does not end in ``COMPLETED``, the
  stage fails and lists the affected runs and logs. If ``check_masks`` is
  ``true``, the success word is additionally required in the job's ``.out``.
* SLURM logs are named ``slurm_<stage>_<run>_<jobid>.out``.

**settings stage** (``run_settings``)
    Runs ``lstchain_dvr_pixselector -n 1 -f "<DL1 pattern>"`` in the
    ``short`` partition with 8G per CPU. It generates the settings file (h5)
    in the ``pixmask_work`` run directory.

**pixmask stage** (``run_pixmask``)
    Runs ``lstchain_dvr_pixselector --action create_pixel_masks -f
    "<pattern>"`` in the ``long`` partition with 16G per CPU. It uses the
    settings generated earlier (which is why both stages share the same run
    directory). When all jobs finish, it moves the
    ``Pixel_selection_LST*.h5`` files to ``pixmask_dir``.

Stage 5: ``r0v.py``
~~~~~~~~~~~~~~~~~~~

*Replaces* ``data_reduction_lstchain``.

:Inputs: raw data (``r0v_input_root/<day>``), RunSummary, masks.
:Outputs: ``R0V/<day>/`` with the files, ``R0V/log/<day>/`` with scripts and
          logs, and ``<workdir>/.../r0v_dates.txt`` with the processed days.

For each day in the range:

* Reads the RunSummary (if missing, warns and skips that day).
* Walks the runs present in the raw-data directory.
* For each subrun that is not yet in the output:

  - if it belongs to a ``DATA`` run and has a mask, it is reduced (goes to a
    job);
  - if it is not ``DATA`` (calibration) or it is ``DATA`` without a mask, it
    is copied as is, all streams.

  (This is the same logic as the official launcher.)
* The subruns to reduce for a run are grouped into a single job per run; if
  there are 190 or more, they are split into chunks of 100.
* The script of each job runs, per subrun:

  .. code-block:: bash

      lstchain_r0g_to_r0v -f <input> -o <output> \
          --pixselection-file <mask> --log <logdir>/dvr_<run>_<sub>.log

  and keeps track of whether any subrun failed, so that the job exits with
  code ``1`` (``sacct`` then flags it as ``FAILED`` and no failure goes
  unnoticed).
* Waits for the jobs. For each subrun it decides whether it succeeded: the job
  is ``COMPLETED`` and (if ``check_r0v`` is ``true``) its log contains the
  success word and belongs to this execution. If not, the partial output of
  that subrun is deleted and the subrun is recorded as a problem.
* Problems are written to the log as errors; the final decision is taken by
  ``verify``.

Stage 6: ``verify.py``
~~~~~~~~~~~~~~~~~~~~~~

*Replaces* ``count_subruns.sh`` *+* ``diff`` *+ copy.*

Compares, day by day, the ``(run, subrun)`` pairs of the input with those of
the R0V output, without parsing the text output of other scripts.

* If a subrun of a run that is **not** ``DATA`` according to the RunSummary is
  missing, it is copied (this is what the documentation allows: copying the
  missing files).
* If a subrun of a ``DATA`` run is missing, it is a real processing failure.
  The raw file is **not** copied (that would put unreduced data in R0V
  without anyone knowing); it is recorded as an error.
* If there is no RunSummary for the day, it cannot be classified and is
  flagged as an error.
* If nothing is missing, it writes ``input == R0V``.


Files generated by an execution
-------------------------------

In ``<workdir>/<start>_<end>/``:

.. list-table::
   :widths: 25 75

   * - ``pipeline.log``
     - Complete log of the execution.
   * - ``date_list.txt``
     - Days with DL1.
   * - ``all_runs.txt``
     - DL1 patterns of the DATA runs.
   * - ``r0v_dates.txt``
     - Days processed in R0V.
   * - ``pixmask_work/``
     - ``.sh`` scripts, ``slurm_*.out`` files and settings h5 files of the
       mask stages.
   * - ``dry_run/<day>/``
     - R0V scripts generated with ``--dry-run``.

In ``<r0v_output_root>/``:

.. list-table::
   :widths: 25 75

   * - ``<day>/``
     - R0V files (and copies of calibration / unmasked data).
   * - ``log/<day>/``
     - ``dvr_reduction_<run>.sh``, SLURM ``pixel_selection_*.log`` and
       lstchain ``dvr_<run>_<subrun>.log``.

In ``<pixmask_dir>/``:

* ``Pixel_selection_LST-1.Run<run>.<subrun>.h5``
