:html_theme.sidebar_secondary.remove: true
:html_theme.sidebar_primary.remove: true

.. _lstosa:

On-site analysis pipeline for the LST-1
=======================================

.. currentmodule:: lstosa

**version**: |version| **Date**: |today|

.. image:: _static/logo_lstosa.png
   :align: center
   :width: 70%

``lstosa`` is the on-site data processing pipeline of the CTA Large-Sized Telescope prototype (LST-1)
making use of the `cta-lstchain`_ analysis library.

* Source repository: https://github.com/cta-observatory/lstosa
* License: BSD-3
* Python: |python_requires|
* Authors: Daniel Morcuende, Lab Saha, José Enrique Ruiz, José Luis Contreras, Andrés Baquero, María Láinez

.. _`cta-lstchain`: https://github.com/cta-observatory/cta-lstchain

.. _lstosa_guide:

.. toctree::
   :caption: LSTOSA Guide
   :maxdepth: 1

   introduction/index
   components/index
   workflow/index
   howto/index
   documents/index
   contribute
   troubleshooting/index
   references
   authors

.. _lstosa_api_docs:

.. toctree::
  :maxdepth: 1
  :caption: API Documentation
  :name: _lstosa_api_docs

  configuration
  jobs
  nightsummary
  provenance
  reports
  scripts/index
  utils
  veto



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
