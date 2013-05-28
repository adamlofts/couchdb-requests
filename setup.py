
import os
import sys
from imp import load_source

if not hasattr(sys, 'version_info') or sys.version_info < (2, 6, 0, 'final'):
    raise SystemExit("couchdbreq requires Python 2.6 or later.")

from setuptools import setup, find_packages

version = load_source("version", os.path.join("couchdbreq", "version.py"))

setup(
    name = 'couchdb-requests',
    version = version.__version__,

    description = 'Robust CouchDB Python Interface',
    long_description = file(
        os.path.join(
            os.path.dirname(__file__),
            'README.rst'
        )
    ).read(),
    author = 'Adam Lofts',
    author_email = 'adam.lofts@gmail.com',
    license = 'Apache License 2',
    url = 'https://github.com/adamlofts/couchdb-requests',

    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages = find_packages(exclude=['tests']),

    zip_safe = False,

    install_requires = [
        'requests>=0.14.1',
    ]
)
