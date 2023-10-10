# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import datetime
from importlib.metadata import metadata

import osa


# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

suppress_warnings = ["ref.citation"]  # ignore citation not referenced warnings

# -- Project information -----------------------------------------------------

project = "lstosa"
author = "lstosa developers"
copyright = f"{author}.  Last updated {datetime.datetime.now().strftime('%d %b %Y %H:%M')}"

version = osa.__version__
# The full version, including alpha/beta/rc tags
release = osa.__version__
python_requires = metadata('lstosa')["Requires-Python"]

# make some variables available to each page (from ctapipe conf.py)
rst_epilog = f"""
.. |python_requires| replace:: {python_requires}
"""

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx_automodapi.automodapi",
    "sphinxarg.ext",
    "numpydoc"
]

numpydoc_show_class_members = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
html_static_path = ["_static"]

html_context = {
    "default_mode": "light",
    "github_user": "cta-observatory",
    "github_repo": "lstosa",
    "github_version": "main",
    "doc_path": "docs",
}

html_title = project

# Output file base name for HTML help builder.
htmlhelp_basename = f"{project}doc"

# Refer figures
numfig = True

html_theme_options = {
    "github_url": "https://github.com/cta-observatory/lstosa",
    "navbar_end": ["navbar-icon-links"],
    "icon_links_label": "Quick Links",
    "use_edit_page_button": True,
}
