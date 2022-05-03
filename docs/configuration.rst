.. _configuration:

Configuration
=============
Modules that handle the data model and ``lstosa`` configuration parameters.

Options
-------
It set the global variables shared across all the modules from the command line options.
To modify them, import ``cliopts`` right after import options in the code.

Data Model
----------
It defines the ``RunObj``, ``SubrunObj``, ``Sequence``, ``SequenceCalibration``,
``SequenceData`` objects that are used across ``lstosa``.


Config
------
It reads the ``.cfg`` file containing the paths and configuration parameters. An example
of this LSTOSA config file can be found in the repository `sequencer.cfg`_.

.. _`sequencer.cfg`: https://github.com/cta-observatory/lstosa/blob/main/osa/configs/sequencer.cfg


Reference/API
-------------

.. automodapi:: osa.configs.config
.. automodapi:: osa.configs.datamodel
