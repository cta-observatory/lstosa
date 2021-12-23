.. _scripts:

Scripts
=======
Scrips to be executed from the command line are described below:

* `sequencer.py`_
* `calibration_pipeline.py`_
* `datasequence.py`_
* `closer.py`_
* `provprocess.py`_
* `copy_datacheck.py`_
* `simulate_processing.py`_


.. _sequencer.py:

sequencer.py
++++++++++++

The sequencer is the main script of ``lstosa``. Right now can only executed
only for ``LST1`` telescope. It triggers the whole analysis chain,
creating the **analysis folders** and sending jobs to the SLURM queue system.
For each run/sequence it sends a job to the working nodes.

In the analysis folders you will find several types of ``sequence_*`` files:

``sequence_*.sh``
   File submitted to the working nodes.
   It calls either the :ref:`calibration_pipeline.py` and the
   :ref:`datasequence.py` depending on the arguments given to
   the sequencer and the type of sequence/run. You can submit
   these jobs manually by executing ``sbatch sequence_*.py``.

``sequencer_*.txt``
   DEPRECATED. Specify the subruns of a sequence/run that will be analyzed by the sequencer.

``sequence_*.history``
   This file keeps tracks of the :func:`execution history<osa.report.history>` of a sequence/run.

``sequence_*.{err,log,out}``
   These files are the logs of the job executed on the working nodes.
   You can find the output of the ``lstchain`` executables in the
   ``sequence_*.out`` file.

``sequence_*.veto``
   This file is just a flag for a vetoed sequence/run that will not be analyzed by the sequencer.

``sequence_*.closed``
   This file is just a flag for an already closed sequence/run that will not be analyzed by the sequencer.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: sequencer_argparser
   :prog: sequencer.py

API/References
--------------

.. automodapi:: osa.scripts.sequencer
    :no-heading:


.. _calibration_pipeline.py:

calibration_pipeline.py
+++++++++++++++++++++++

It produces the calibration products.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: calibration_pipeline_argparser
   :prog: calibration_pipeline.py

API/References
--------------

.. automodapi:: osa.scripts.calibration_pipeline
    :no-heading:


.. _datasequence.py:

datasequence.py
++++++++++++++++++++++

It processes the raw R0 data producing the DL1 and DL2 files.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: data_sequence_argparser
   :prog: datasequence.py

API/References
--------------

.. automodapi:: osa.scripts.datasequence
    :no-heading:


.. _closer.py:

closer.py
+++++++++

Checks that all sequences are finished and completed, extract the
provenance from the ``prov.log`` file and merge the DL1 data-check files.
It also moves the analysis products to their final destinations.

.. warning::

   The usage of this script will be overcome by ``autocloser.py``.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: closer_argparser
   :prog: closer.py

API/References
--------------

.. automodapi:: osa.scripts.closer
    :no-heading:


.. _provprocess.py:

provprocess.py
++++++++++++++

Extract the provenance information logged in to the ``prov.log`` file.
It is executed within `closer.py`_. It produces the provenance graphs
and ``.json`` files run-wise.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: provprocess_argparser
   :prog: provprocess.py

API/References
--------------

.. automodapi:: osa.scripts.provprocess
    :no-heading:


.. _copy_datacheck.py:

copy_datacheck.py
+++++++++++++++++

Copy the calibration and DL1 data-check files to the *datacheck* web server.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: copy_datacheck_argparser
   :prog: copy_datacheck.py

API/References
--------------

.. automodapi:: osa.scripts.copy_datacheck
    :no-heading:


simulate_processing.py
++++++++++++++++++++++

It simulates the processing of the data sequence, generating the
provenance products in the ``prov.log`` file.

Usage
-----
.. argparse::
   :module: osa.utils.cliopts
   :func: simproc_argparser
   :prog: simulate_processing.py

API/References
--------------

.. automodapi:: osa.scripts.simulate_processing
    :no-heading:
