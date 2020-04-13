.. _howtocontribute:

How to contribute
*****************

The LSTOSA code and docs reside on a private `Gitlab`_ repository.
If you want to get access to this repository ask its developers.

If you know nothing about git, we recomment to follow this `guide`_.

.. _`Gitlab`: https://gitlab.com/contrera/lstosa
.. _`guide`: https://cta-observatory.github.io/ctapipe/getting_started/index.html

How to build the docs
=====================

You can find these docs in the ``docs`` folder inside the repository.
They are build via the `Sphinx`_ tool.
To build the docs you need following python packages:

* python >=2.7
* sphinx 1.8.2
* numpydoc 0.8.0
* sphinx_rtd_theme 0.4.2
* sphinx-argparse 0.2.5
* sphinx-automodapi 0.9

To build them, run following command inside the ``docs`` folder:

.. code:: bash

    make html

or for a clean rebuild:

.. code:: bash

    make clean html

.. _`Sphinx`: http://www.sphinx-doc.org/en/stable/

Python Docstrings
=================

For the docstrings in the python code we use the `NumPy Style
<https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html#example-numpy>`_.
These docstrings are then interpreted by Sphinx and used to build the API
references in the :ref:`osa_code` section.

