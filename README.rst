lstosa |ci| |docs| |coverage| |precommit| |quality|
============================================================

.. |docs| image:: https://readthedocs.org/projects/lstosa-test2/badge/?version=latest 
  :target: https://lstosa-test2.readthedocs.io/en/latest/?badge=latest 
  :alt: Documentation Status
  
.. |ci| image:: https://github.com/gae-ucm/lstosa/actions/workflows/ci.yml/badge.svg?branch=main
  :target: https://github.com/gae-ucm/lstosa/actions/workflows/ci.yml

.. |coverage| image:: https://codecov.io/gh/gae-ucm/lstosa/branch/main/graph/badge.svg
  :target: https://codecov.io/gh/gae-ucm/lstosa
  
.. |precommit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white

.. |quality| image:: https://app.codacy.com/project/badge/Grade/5b660e2b9de84a839085923a2b052d47
  :target: https://www.codacy.com/gh/gae-ucm/lstosa/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=gae ucm/lstosa&amp;utm_campaign=Badge_Grade


On-Site analysis pipeline of the the Large Size Telescope prototype of CTA_ (Cherenkov Telescope Array).

This code is a prototype data processing framework under development.

* Code: https://github.com/gae-ucm/lstosa
* Docs: https://lstosa.readthedocs.io/
* License: BSD-3-Clause_

.. _CTA: https://www.cta-observatory.org/
.. _BSD-3-Clause: https://github.com/morcuended/lstosa-test/blob/main/LICENSE


Install
-------
* Install miniconda first.
* Create and activate the conda environment including lstchain_ and ctapipe_io_lst_:

.. code-block:: bash

   git clone https://github.com/gae-ucm/lstosa.git
   cd lstosa
   conda env create -n osa -f environment.yml
   conda activate osa
   

In case you want to install the lstchain master version instead of a fixed tag you can run with the `osa` environment activated:

.. code-block:: bash

   pip install git+https://github.com/cta-observatory/cta-lstchain


* To update the environment (provided dependencies get updated) use:

.. code-block:: bash

   conda env update -n osa -f environment.yml

* Install `lstosa`:

.. code-block:: bash

   pip install -e .

.. _lstchain: https://github.com/cta-observatory/cta-lstchain
.. _ctapipe_io_lst: https://github.com/cta-observatory/ctapipe_io_lst
