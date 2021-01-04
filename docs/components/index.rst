.. _components:

Components
**********

LSTOSA is a set of python scripts and cron jobs that pipes the different analysis steps of lstchain, using SLURM as
resource manager. It is thought to run on the LST IT container at La Palma using the ``lstanalyzer`` account.

.. _lstchain_section:

lstchain
========

`lstchain`_ is the analysis library developed for the commissioning of the LST-1 prototype.
It is heavily based on the the Prototype CTA Pipeline Framework `ctapipe`_.

.. _`lstchain`: https://github.com/cta-observatory/cta-lstchain
.. _`ctapipe`: https://github.com/cta-observatory/ctapipe

.. _slurm:

SLURM
=====

SLURM is the resource manager used at the LST IT container. Most LSTOSA scripts, specially those devoted to analyze
data (heavy duty ones) run through SLURM. Its capabilities are used to send chained jobs, that are executed only after
the *parent* job finished. This is used for calibration sequences, whose end triggers the analysis sequences for the
``DATA`` runs. The code in ``job.py`` interacts with the resource manager.

More information: https://slurm.schedmd.com/

.. _Cron Jobs:

Cron Jobs
=========

A set of cron jobs automatize LSTOSA execution. 

1. Production of *NightSummary* file, launched each morning at 7 UTC. They are currently not produced by LSTOSA, but
   generated independently by the Data-Check software being developed by I. Aguado. These files sometimes need to be
   modified to assure that only one DRS4 and CALIBRATION runs are present each day. Therefore next steps cannot be
   fully automatized until runs are properly and reliably tagged.

2. Sequencer: Cron job for sequencer script to be implemented.

3. Closer: Cron job for closer script to be implemented.

.. _sequencer:

Sequencer
=========

``sequencer.py`` is the main script controlling LSTOSA execution. It takes as input a configuration file and a
``NightSummary.txt`` file. The first one contains all the needed parameters and paths. The ``NightSummary`` file
contains the list of runs to process, tagged as ``DRS4``, ``CALIBRATION`` or ``DATA`` based on the percentage of
pedestal events in each sub-run file. Each run forms a so-called sequence. Sequencer builds a job for each run,
depending on its type the job will call either ``calibrationsequence.py`` or ``datasequence.py``. Each job is an array
job which processes all the sub-runs contained in the run in parallel.

It uses ``nightsummary.py`` and ``extract.py`` to read the ``NightSummary.txt`` file and extract the sequences.

.. _closer:

Closer
======

The ``closer`` is an error handler and closer for LSTOSA. It processes the sequencer table and *closes* the
successfully analyzed sequences or tries to solve known issues in an automatic way. For details see ``closer.py``.

.. _provenance_component:

Provenance
==========

The data analysis steps executed to create DL1 and DL2 level data are captured for each run, together with the
configuration parameters and files needed as well as intermediate files produced. This information is serialized in
``.json`` formatted files, following the *IVOA Provenance Model Recommendation* [IVOAProvenance]_. Provenance
graphs are also provided in ``.pdf`` formatted files, rendering a detailed complete view of the data analysis
process which improves process inspection and helps achieving reproducibility. Tracking of the calibration steps
will be implemented shortly, and a more detailed provenance query tool is also foreseen, which would need to store
the provenance information in a database.

.. _highlevel:

HighLevel
=========

The production of DL3 file is not implemented yet. Further high-level analysis as obtaining the significance level
of the source detection, sky maps, spectra and light curves are to be implemented.

.. _database:

MySQL Database
==============

Not implemented yet.
