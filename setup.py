#!/usr/bin/env python3

import os
from setuptools import setup, find_packages

with open(os.path.join('darc', '__version__.py')) as version_file:
    version = {}
    exec(version_file.read(), version)
    project_version = version['__version__']


setup(name='darc',
      version=project_version,
      description='Data Analysis of Real-time Candidates from ARTS',
      url='http://github.com/loostrum/darc',
      author='Leon Oostrum',
      author_email='l.oostrum@esciencecenter.nl',
      license='Apache2.0',
      packages=find_packages(),
      zip_safe=False,
      install_requires=['numpy',
                        'astropy',
                        'pyyaml',
                        'h5py<3',  # bug in 3.0 causes error on keras model load
                        'pytz',
                        'voevent-parse',
                        'scipy',
                        'matplotlib',
                        'PyPDF4'],
      extras_require={'psrdada': ['psrdada-python', 'single_pulse_ml']},
      include_package_data=True,
      entry_points={'console_scripts': ['darc=darc.control:main',
                                        'darc_service=darc.darc_master:main']},
      scripts=['bin/darc_start_all_services',
               'bin/darc_stop_all_services',
               'bin/darc_start_master',
               'bin/darc_stop_master',
               'bin/darc_kill_all'])
