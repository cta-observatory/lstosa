.. _jobs:

Jobs
****

The ``osa.job`` module manages all SLURM job-related operations: job script generation,
submission, state queries, and dependency handling.

.. _job_naming_reference:

Job naming
==========

Job names are constructed to be unique per run and to enable duplicate detection.
The sequencer queries ``squeue`` and ``sacct`` to find active jobs by name before
submitting new ones.

**R0 to DL1 array**

.. code-block:: text

    LST1_<run>

Example: ``LST1_01234``

This SLURM array job processes one subrun per array task. Produced by
``osa.job.r0_jobname()``.

**Cat-B calibration and tailcuts pilot**

.. code-block:: text

    LST1_catB_tailcuts_<run>

Example: ``LST1_catB_tailcuts_01234``

This single (non-array) per-run job is produced by ``osa.job.catb_jobname()``.

**DL1 to DL1ab array**

.. code-block:: text

    LST1_dl1ab_<run>

Example: ``LST1_dl1ab_01234``

This SLURM array job is produced by ``osa.job.dl1ab_jobname()``.

.. _sbatch_headers:

SBATCH headers and script generation
====================================

Job scripts are generated on demand with ``osa.job`` functions:

- ``osa.job.write_r0_script(sequence)`` → ``sequence_LST1_<run>.py``
- ``osa.job.write_dl1ab_script(sequence)`` → ``sequence_LST1_<run>_dl1ab.py``
- ``osa.job.write_catb_pilot_script(run_id)`` → ``sequence_LST1_<run>_catb_tailcuts.py``

Each script contains:

1. A shebang line: ``#!/usr/bin/env python3``
2. ``#SBATCH`` directives for SLURM configuration
3. Python code that builds a subprocess command and runs it

The SBATCH headers are constructed by ``osa.job.scheduler_env_variables()`` and include:

- Job name (``--job-name``)
- Time limit (``--time``)
- Working directory (``--chdir``)
- Output and error log paths
- Array specification (for array jobs only)
- Partition, memory, and account information

Array jobs receive log specifications with ``%4a`` (array task ID) and ``%A``
(array job ID) for per-task logging.

.. _submission:

Job submission
==============

The ``osa.job.sbatch_submit()`` function submits scripts via SLURM:

.. code-block:: python

    jobid = sbatch_submit(script_path, dependency=parent_jobid)

It returns the submitted job ID or ``None`` if simulated/tested.

When a dependency is provided, ``--dependency=afterok:<parent_jobid>`` is passed
to ``sbatch``.

Duplicate detection is performed before submission by ``osa.job.job_is_active()``
and ``osa.job.get_active_jobid()``.

.. _job_queries:

Job state queries
=================

``osa.job`` provides functions to query SLURM using ``squeue`` and ``sacct``:

- ``osa.job.run_squeue()`` → runs ``squeue`` and returns the output
- ``osa.job.get_squeue_output()`` → parses squeue output into a DataFrame
- ``osa.job.run_sacct()`` → runs ``sacct`` and returns the output
- ``osa.job.get_sacct_output()`` → parses sacct output into a DataFrame
- ``osa.job.get_closer_sacct_output()`` → filters sacct output for autocloser jobs

Job state predicates:

- ``osa.job.job_is_active(jobname)`` → True if the job is queued/running
- ``osa.job.get_active_jobid(jobname)`` → returns the job ID of an active job
- ``osa.job.get_last_job_state(jobname)`` → returns the most recent job state
- ``osa.job.array_job_status(sacct_df, jobname)`` → overall status of an array job

.. _history_levels:

History levels and resumption
==============================

The ``osa.job.historylevel()`` function reads a per-subrun history file and
returns the next processing level:

For DATA sequences:

.. code-block:: text

    4: R0 to DL1 pending
    3: Cat-A datacheck pending
    2: DL1ab pending
    1: DL1b datacheck pending
    0: Complete

For PEDCALIB sequences:

.. code-block:: text

    2: DRS4 baseline pending
    1: Charge calibration pending
    0: Complete

The function also returns the exit code of the last program in the history.
If the exit code is non-zero, the sequence is considered to have failed at that stage.

Helper functions:

- ``osa.job.r0_job_completed(run_id)`` → True if all subruns have completed r0_to_dl1
  and Cat-A datacheck
- ``osa.job.run_fully_processed(run_id)`` → True if all subruns have completed dl1ab
  and DL1b datacheck

.. _completion_markers:

Completion markers
==================

Global run-level completion is tracked in files like:

.. code-block:: text

    LST1_01234.history

These files contain per-run summary lines added when array stages complete.

Example entries:

.. code-block:: text

    01234 R0_ARRAY v0.10.0 2026-09-28 14:40 None None 0
    01234 DL1AB_ARRAY v0.10.0 2026-09-28 15:15 None None 0
    01234 CATB_CLOSED v0.10.0 2026-09-28 14:50 None None 0

The ``catB_<run>.closed`` marker file is created by the Cat-B/tailcuts pipeline
on successful completion.

.. _cat_a_datacheck_dir:

Cat-A datacheck directory
==========================

Per-subrun and per-run Cat-A datacheck files are stored in a dedicated directory:

.. code-block:: text

    datacheck_cat_a/

This directory path is available as:

.. code-block:: python

    from osa.job import CAT_A_DATACHECK_DIR
    cat_a_dir = options.directory / CAT_A_DATACHECK_DIR

.. _conditional_logic:

Conditional submission logic
============================

The sequencer uses several helper functions to determine whether jobs are needed:

``osa.job.catb_tailcuts_needed(run_id)`` returns a tuple of (need_catb, need_tailcuts):

.. code-block:: python

    need_catb, need_tailcuts = catb_tailcuts_needed(run_id)

These are determined by:

- ``need_catb``: ``apply_catB_calibration`` is True AND no ``catB_<run>.closed`` exists
- ``need_tailcuts``: ``apply_standard_dl1b_config`` is False AND the tailcuts JSON config doesn't exist

.. _job_statistics:

Job statistics
==============

The ``osa.job.save_job_information()`` and ``osa.job.plot_job_statistics()`` functions
record and visualize SLURM accounting information such as elapsed time, memory usage,
and job state distribution.

.. _api_reference:

API Reference
=============

.. automodule:: osa.job
   :members:
   :undoc-members:
   :exclude-members: PYTHON_IMPORTS, TAB, SHEBANG, FORMAT_SLURM, ACTIVE_STATES, BAD_STATES
