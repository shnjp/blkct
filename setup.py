#!/usr/bin/env python
# -*- coding: utf-8 -*-

# from pipenv.project import Project
# from pipenv.utils import convert_deps_to_pip
from setuptools import find_packages, setup

# pfile = Project(chdir=False).parsed_pipfile
# requirements = convert_deps_to_pip(pfile['packages'], r=False)
# test_requirements = convert_deps_to_pip(pfile['dev-packages'], r=False)

setup(
    name='blkct',
    version='0.1',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'aiohttp', 'beautifulsoup4', 'boto3', 'feedparser', 'lxml', 'python-dateutil',
        'werkzeug'
    ],
    entry_points={
        'console_scripts': [
            'blkct=blkct.__main__:main',
        ],
    },
    extras_require={
        'dev': [
            'coverage',
            'flake8',
            'flake8-import-order',
            'pytest',
            'yapf',
            'pipenv',
            'autopep8',
            'isort',
        ],
    }
)
