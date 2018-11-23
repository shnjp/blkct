#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

setup(
    name='blkct',
    version='0.1',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'aiodns',
        'aiohttp',
        'beautifulsoup4',
        'boto3',
        'chardet',
        'click',
        'feedparser',
        'lxml',
        'python-dateutil',
        'uwsgi',
    ],
    entry_points={
        'console_scripts': [
            'blkct=blkct.__main__:cli_main',
        ],
    },
    extras_require={
        'test': [
            'coverage',
            'flake8',
            'flake8-import-order',
            'pytest',
            'yapf',
        ],
    })
