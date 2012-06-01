#!/usr/bin/env python
"""pybackup - Installation Script

"""

import os
from setuptools import setup, find_packages

__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


def read_file(filename):
    """Read a file into a string"""
    path = os.path.abspath(os.path.dirname(__file__))
    filepath = os.path.join(path, filename)
    try:
        return open(filepath).read()
    except IOError:
        return ''


setup(
    name='pybackup',
    version=__version__,
    author=__author__,
    author_email=__email__,
    packages=find_packages(),
    include_package_data=True,
    url='http://aouyar.github.com/pybackup',
    license=__license__,
    description=u'Python Module for developing backup scripts.',
    classifiers=[
        'Topic :: System :: Archiving :: Backup',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python',
        'Development Status :: 3 - Alpha',
        'Operating System :: OS Independent',
    ],
    long_description=read_file('README.markdown'),
    entry_points={'console_scripts': u"pybackup = pybackup.jobmgr:main"},
    install_requires=["PyMunin",],
)
