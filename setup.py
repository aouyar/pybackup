import os
from setuptools import setup, find_packages
import pybackup.jobmgr


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
    version=pybackup.jobmgr.__version__,
    author=pybackup.jobmgr.__author__,
    author_email=pybackup.jobmgr.__email__,
    packages=find_packages(),
    include_package_data=True,
    url='http://aouyar.github.com/pybackup',
    license=pybackup.jobmgr.__license__,
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
)
