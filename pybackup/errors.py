"""pybackup - Exception Classes for handling of errors in execution of backups.

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
    """Base exception for errors in backup processes.
    
    """
    desc = 'Error in execution of backup job.'
    fatal = False
    trace = False
    
class BackupStartupError(BackupError):
    """Exception for fatal errors in initialization of the backup process.
    
    """
    desc = "Fatal error on startup."
    fatal = True

class BackupConfigError(BackupError):
    """Exception for errors in configuration of backup process, usually caused 
    by errors in configuration file.
    
    """
    desc = 'Configuration error in backup job.'

class BackupFatalConfigError(BackupConfigError):
    """Exception for fatal errors in configuration of backup process, usually caused 
    by errors in configuration file.
    
    """
    desc = 'Fatal configuration error in backup job.'
    fatal = True

class BackupEnvironmentError(BackupError):
    """Exception for errors caused by problems in backup environment.
    Ex: User privileges, missing directories, etc.
    
    """
    desc = 'Error in backup job environment.'

class BackupFatalEnvironmentError(BackupEnvironmentError):
    """Exception for errors caused by problems in backup environment.
    Ex: User privileges, missing directories, etc.
    
    """
    desc = 'Fatal error in backup job environment.'
    fatal = True

class BackupCmdError(BackupError):
    """Exception for errors in execution in external backup commands.
    
    """
    desc = 'Error in execution of backup command.'

class ExternalCmdError(BackupError):
    """Exception for errors in execution of external general or job, 
    pre / post execution scripts.
    
    """
    desc = 'Error in execution of external script.'

class BackupFileCreateError(BackupError):
    """Exception for errors in creation of backup files.
    
    """
    desc = "Error in creation of backup file."

class BackupBadPluginError(BackupError):
    """Exception for plugins that do not implement the required standard
    interfaces for backup plugins.
    
    """
    desc = 'Plugin does not conform to standards.'
    fatal = True


def setTrace():
    """Function for enabling the generation of a full trace of errors.
    
    """
    BackupError.trace = True
    
def unsetTrace():
    """Function for disabling the generation of a full trace of errors.
    
    """
    BackupError.trace = False
    