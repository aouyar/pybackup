"""pybackup - Logging Manager

"""


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


class BackupError(Exception):
    pass

class BackupConfigError(BackupError):
    pass

class BackupFatalConfigError(BackupConfigError):
    pass

class BackupEnvironmentError(BackupError):
    pass

class BackupCmdError(BackupError):
    pass

class BackupFileCreateError(BackupError):
    pass
