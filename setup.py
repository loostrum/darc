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
        install_requires=['numpy',
                          'astropy',
                          'pyyaml',
                          'pytz'],
        include_package_data=True,
        entry_points={'console_scripts': ['darc=darc.control:main',
                                          'darc_service=darc.darc_master:main']},
        scripts=['bin/stream_files_to_port',
                 'bin/darc_start_all_services',
                 'bin/darc_stop_all_services',
                 'bin/darc_kill_all_services'])
