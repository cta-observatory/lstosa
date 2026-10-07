.. _components:

Components
**********

LSTOSA is a collection of Python modules, command-line scripts, and cron jobs
that connect the different analysis stages of ``lstchain``. It uses SLURM as
resource manager and is intended to run in the LST IT container at La Palma,
using the ``lstanalyzer`` account.

.. _lstchain_section:

lstchain
========

`lstchain`_ is the analysis library developed for the commissioning and
operation of the LST-1 prototype. It is heavily based on the Prototype CTA
Pipeline Framework `ctapipe`_. LSTOSA builds the onsite workflow around the
lstchain commands for calibration, reconstruction, datachecks, and merging.

.. _`lstchain`: https://github.com/cta-observatory/cta-lstchain
.. _`ctapipe`: https://github.com/cta-observatory/ctapipe

.. _slurm:

SLURM
=====

SLURM executes the computationally expensive analysis stages and provides
parallelization through job arrays. LSTOSA also uses SLURM dependencies to
connect the stages of a run:

.. code-block:: text

   PEDCALIB -> R0/DL1 -> Cat-B/tailcuts -> DL1ab -> datacheck

The Cat-B/tailcuts stage is optional. If it is not needed, the DL1ab job can
depend directly on the R0/DL1 job. The ``osa.job`` module generates SBATCH
scripts, submits jobs, queries ``squeue`` and ``sacct``, detects active jobs,
and interprets array-job status.

More information: https://slurm.schedmd.com/

.. _cron_jobs:

Cron jobs
=========

Cron jobs automate the daily execution of LSTOSA. The exact schedule is
site-dependent, but the workflow contains the following responsibilities:

1. **Night summary:** the data-check system or a related service produces the
   NightSummary file containing the runs of the night.
2. **Sequencer:** builds the processing jobs and submits the calibration, R0/DL1,
   Cat-B/tailcuts, and DL1ab stages when their inputs are available.
3. **Autocloser:** checks completed jobs, merges and moves products, records
   provenance, and starts the long-term datachecks.
4. **Datacheck copy:** copies the available datacheck products, including the
   long-term Cat-A datacheck, to the web server.

.. _sequencer:

Sequencer
=========

``sequencer.py`` is the top-level orchestration script. It takes a
configuration file, a date, and a telescope identifier. It reads the
NightSummary and builds calibration and DATA sequences.

The sequencer delegates job generation and submission to ``osa.job``. For a
DATA run it can create three different jobs:

* ``LST1_<run>``: an R0/DL1 SLURM array. It runs ``r0_to_dl1`` and the Cat-A
  datacheck for each subrun.
* ``LST1_catB_tailcuts_<run>``: a per-run pilot that merges Cat-A datachecks,
  creates Cat-B calibration products, and finds tailcuts when required.
* ``LST1_dl1ab_<run>``: a DL1ab SLURM array. It resolves the DL1b configuration
  at runtime so that products created by the pilot are available.

The sequencer uses ``squeue`` and ``sacct`` to avoid duplicate submissions,
and it uses per-subrun history files to resume incomplete processing. The
``--simulate`` and ``--test`` modes are available for development and
validation; ``--force-submit`` can be used when the normal dependency checks
need to be overridden.

Cat-B and tailcuts pipeline
===========================

``catb_tailcuts_pipeline`` performs the per-run operations between the DL1a
and DL1b stages. It:

1. waits for all per-subrun Cat-A datachecks;
2. merges them into a per-run Cat-A datacheck;
3. runs the Cat-B calibration if enabled;
4. runs the tailcuts finder if a standard DL1b configuration is not selected;
5. writes the ``catB_<run>.closed`` marker after successful completion.

The Cat-A files are kept in the ``datacheck_cat_a`` directory below the
analysis directory. This separate location prevents the Cat-A products from
being confused with the DL1b datachecks.

.. _closer:

Autocloser
==========

The ``autocloser`` is responsible for the finalization of the processing. It
checks the job and sequence state, merges subrun products, moves files to their
final destinations, and captures provenance. It also launches the long-term
Cat-A datacheck once the per-run Cat-A products are available.

.. _provenance_component:

Provenance
==========

The data-analysis steps used to create DL1 and DL2-level data are captured for
each run, together with the configuration parameters, input files, and
intermediate products. This information is serialized in ``.json`` files,
following the `IVOA Provenance Model Recommendation`_. Provenance graphs are
also provided in ``.pdf`` format, giving a detailed view of the analysis
process and improving reproducibility.

.. _`IVOA Provenance Model Recommendation`: https://www.ivoa.net/documents/ProvenanceDM/

.. _highlevel:

High-level analysis
===================

The production of DL3 files and higher-level results is not yet part of the
standard onsite workflow. Significance estimates, sky maps, spectra, and
light curves are planned for future high-level analysis stages.

.. _database:

Database
========

Database-backed provenance queries are not currently implemented in LSTOSA.
