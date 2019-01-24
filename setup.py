#!/usr/bin/env python

import os
from subprocess import check_call
from setuptools import setup, find_packages
from setuptools.command.build_py import build_py


class update_submodules(build_py):
    def run(self):
        if os.path.exists('.git'):
            print "Updating submodules"
            check_call(['git', 'submodule', 'update', '--init', '--recursive'])
        build_py.run(self)


setup(name='darc',
        version='0.1',
        description='Data Analysis of Real-time Candidates from ARTS',
        url='http://github.com/loostrum/darc',
        author='Leon Oostrum',
        author_email='oostrum@astron.nl',
        license='GPLv3',
        packages=find_packages(exclude=['external']),
        zip_safe=False,
        cmdclass={"build_py": update_submodules},
        install_requires=['numpy',
                          'astropy',
                          'pyyaml'],
        include_package_data=True,
        entry_points={'console_scripts': ['darc=darc.control:main',
                                          'darc_service=darc.darc_master:main']},
        scripts=['bin/stream_files_to_port',
                 'bin/darc_start_all_services',
                 'bin/darc_stop_all_services',
                 'bin/darc_kill_all_services'])
