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
    desc = 'Error in execution of backup job.'
    fatal = False
    trace = False
    
class BackupStartupError(BackupError):
    desc = "Fatal error on startup."
    fatal = True

class BackupConfigError(BackupError):
    desc = 'Configuration error in backup job.'

class BackupFatalConfigError(BackupConfigError):
    desc = 'Fatal configuration error in backup job.'
    fatal = True

class BackupEnvironmentError(BackupError):
    desc = 'Error in backup job environment.'

class BackupFatalEnvironmentError(BackupEnvironmentError):
    desc = 'Fatal error in backup job environment.'
    fatal = True

class BackupCmdError(BackupError):
    pass

class BackupFileCreateError(BackupError):
    pass

class BackupBadPluginError(BackupError):
    desc = 'Plugin does not conform to standards.'
    fatal = True


def set_trace():
    BackupError.trace = True
    
def unset_trace():
    BackupError.trace = False
    