.. _components:

Components
**********

LSTOSA is a set of python scripts and cron jobs based on lstchain and using SLURM as
resource manager. It it thought to run on the LST IT container at La Palma
using the lstanalyzer account.

.. _lstchain:

lstchain
====

lstchain is the analysis chain developed for the commissioning of the LST1 prototype.
It is open software that can be accesed at https://github.com/cta-observatory/cta-lstchain

.. _slurm:

SLURM
==========

SLURM is the resource manager used at the LST IT container. Most LSTOSA scripts, specially those devoted to 
analyze data (heavy duty ones) run through SLURM. Its capabilities are used to send "chained"
jobs, that are executed only a previous job finished. This is used for calibration sequences, whose
end triggers analysis sequences.
 
* the code in  ``job.py`` interacts with the resource manager


.. _Cron Jobs:

Cron Jobs
=========

A set of cron jobs automatize LSTOSA execution. 

1. Production of  *NightSummary* file, launched each moning at 7 UTC. They are currently not produced by LSTOSA, but they are 
generated independently by the Data-check software beign developed by I. Aguado.

.. _sequencer:

Sequencer
=========

* consists of datasequence, calibrationsequence ...
* uses :ref:`nightsummary.py` and :ref:`extract.py` to read the NightSummary
  txt file and extract the sequences.


Closer
======

TBC

The *Closer* is an error handler and closer for LSTOSA.
It processes the sequencer table and *closes* the successfully analyzed
sequences or tries to solve known issues in an automatic way.
For details see :ref:`Closer`.


.. _highlevel:

HighLevel
=========

TBC

.. _database:

MySQL Database
==============

TBC
