#!/usr/bin/env python
# Licensed under a 3-clause BSD style license - see LICENSE.rst
# import sys
from setuptools import setup, find_packages
import os
from version import get_version, update_release_version

update_release_version()
version = get_version()



entry_points = {}
entry_points['console_scripts'] = sequencer/sequencer.py

setup(name='lstchain',
      version=version,
      description="DESCRIPTION",  # these should be minimum list of what is needed to run
      packages=find_packages(),
      tests_require=['pytest', 'pytest-ordering'],
      author='LST OSA ',
      author_email='',
      license='',
      url='',
      long_description='',
      classifiers=[],
      entry_points=entry_points
      )
© 2020 GitHub, Inc.