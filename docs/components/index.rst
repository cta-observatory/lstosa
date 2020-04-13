Components
**********

LSTOSA consists of the lstchain, a set of python scripts, cron jobs and SLURM as
resource manager. It runs on the LST IT server at La Palma
using the lstanalyzer account.

.. _lstchain:

lstchain
====

TBC

.. _nightsummary:

NightSummary
============

The *NightSummary* files are currently not produced by LSTOSA, but they are 
generated independently by Isidro's Data-check software.

.. _sequencer:

Sequencer
=========

* consists of datasequence, calibrationsequence ...
* uses :ref:`nightsummary.py` and :ref:`extract.py` to read the NightSummary
  txt file and extract the sequences.

.. _slurm:

SLURM
==========

TBC

Torque is the resource manager used at the LST IT container. Most LSTOSA scripts, specially those devoted to 
analyze data (heavy duty ones) run through SLURM. Its capabilities are used to send "chained"
jobs, that are executed only a previous job finished. This is used for calibration sequences, whose
end triggers analysis sequences.
 
* the ``job.py`` interacts with the resource manager


Closer
==========

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
