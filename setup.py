#!/usr/bin/env python

from setuptools import setup, find_packages


setup(name='darc',
      version='0.1',
      description='Data Analysis of Real-time Candidates from ARTS',
      url='http://github.com/loostrum/darc',
      author='Leon Oostrum',
      author_email='oostrum@astron.nl',
      license='GPLv3',
      packages=find_packages(),
      zip_safe=False,
      scripts=['bin/darc',
               'bin/stream_files_to_port'])
