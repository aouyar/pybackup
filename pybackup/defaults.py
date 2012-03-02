"""pybackup - Global Defaults

"""


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


globalConf = {'cmd_compress': 'gzip', 
              'suffix_compress': 'gz',
              'cmd_tar': 'tar',
              'suffix_tar': 'tar',
              'suffix_tgz': 'tgz',}

configPaths = ['./pybackup.conf', '/etc/pybackup.conf']
bufferSize=4096