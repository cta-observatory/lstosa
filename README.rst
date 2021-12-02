lstosa |ci| |docs| |coverage| |precommit| |quality|
============================================================

.. |docs| image:: https://readthedocs.org/projects/lstosa/badge/?version=latest
  :target: https://lstosa.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status
  
.. |ci| image:: https://github.com/cta-observatory/lstosa/actions/workflows/ci.yml/badge.svg?branch=main
  :target: https://github.com/cta-observatory/lstosa/actions/workflows/ci.yml

.. |coverage| image:: https://codecov.io/gh/cta-observatory/lstosa/branch/main/graph/badge.svg?token=Zjk1U1ytaG
  :target: https://codecov.io/gh/cta-observatory/lstosa

.. |quality| image:: https://app.codacy.com/project/badge/Grade/a8743a706e7c45fc989d5ebc4d61d54f
  :target: https://www.codacy.com/gh/cta-observatory/lstosa/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=cta-observatory/lstosa&amp;utm_campaign=Badge_Grade

On-Site processing pipeline for the the Large Size Telescope prototype of CTA_ (Cherenkov Telescope Array).

This code is a prototype data processing framework based on cta-lstchain_ under development.

* Code: https://github.com/cta-observatory/lstosa
* Docs: https://lstosa.readthedocs.io/
* License: BSD-3-Clause_

.. _CTA: https://www.cta-observatory.org/
.. _BSD-3-Clause: https://github.com/cta-observatory/lstosa/blob/main/LICENSE

Install
-------
* Install miniconda first.
* Create and activate the conda environment including cta-lstchain_ and ctapipe_io_lst_:

.. code-block:: bash

   git clone https://github.com/cta-observatory/lstosa.git
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

.. _cta-lstchain: https://github.com/cta-observatory/cta-lstchain
.. _ctapipe_io_lst: https://github.com/cta-observatory/ctapipe_io_lst
