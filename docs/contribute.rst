.. _howtocontribute:

How to contribute
*****************

The LSTOSA code and docs reside on this `this GitHub repository`_.
If you want to contribute or have any question on how to use it ask its developers.
You can also use `GitHub Issues`_ to ask or report any problem you may find.

If you know nothing about git, we recommend to follow this `guide`_.

.. _`this GitHub repository`: https://github.com/cta-observatory/lstosa
.. _`guide`: https://cta-observatory.github.io/ctapipe/getting_started/index.html
.. _`GitHub Issues`: https://github.com/cta-observatory/lstosa/issues

How to build the docs
=====================

You can find these docs in the ``docs`` folder inside the repository.
They are build via the `Sphinx`_ package, deployed and published using `Read The Docs`_.

To build the docs locally on your machine, you need to activate the ``osa-dev``
environment first. This conda environment can be created using the ``environment.yml``
file from the repository containing packages needed to generate the documentation:

- sphinx
- numpydoc
- sphinx_rtd_theme
- sphinx-argparse
- sphinx-automodapi

Once the environment is activated just run following command inside the ``docs`` folder:

.. code:: bash

    make clean html

.. _`Sphinx`: https://www.sphinx-doc.org/
.. _`Read the Docs`: https://readthedocs.org/

Python Docstrings
=================

For the docstrings in the python code we use the `NumPy Style`_.
These docstrings are then interpreted by Sphinx and used to build the API
references in the LSTOSA code section.

.. _`NumPy Style`: https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html#example-numpy

