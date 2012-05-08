"""pybackup - Logging Manager

"""

import logging


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


# Defaults
defaultLogLevel = logging.INFO
"""Default logging level."""

defaultLogFormat = '%(asctime)s:%(context)s:%(levelname)-8s %(message)s'
"""Default log entry format."""

defaultDateFormat = '%Y-%m-%d %H:%M:%S'
"""Default date format for log entries."""


class LogContext(logging.Filter):
    
    def __init__(self, context, test_run=None):
        if test_run is None:
            test_run = False
        self.setContext(context, test_run)
        logging.Filter.__init__(self)
    
    def setContext(self, context, test_run=None):
        if test_run is not None:
            self._testRun = test_run
        if self._testRun:
            self._context = "TEST-%s" % context
        else:
            self._context = context    
    
    def filter(self, record):
        record.context = self._context
        return True


class LogManager:
    
    def __init__(self):
        self._logger = logging.getLogger()
        self._logger.setLevel(defaultLogLevel)
        self._formatter = logging.Formatter(defaultLogFormat, defaultDateFormat)
        self._handlerConsole = logging.StreamHandler()
        self._handlerConsole.setLevel(defaultLogLevel)
        self._handlerConsole.setFormatter(self._formatter)
        self._logger.addHandler(self._handlerConsole)
        self._handlerLogFile = None
        self._logContext = LogContext('INIT')
        self._logger.addFilter(self._logContext)
        self._minLevel = defaultLogLevel
        
    def getLogLevel(self, level_name):
        return logging._levelNames.get(str(level_name).upper())
        
    def setContext(self, context, test_run=None):
        self._logContext.setContext(context, test_run)
    
    def configConsole(self, level):
        if level < self._minLevel:
            self._minLevel = level
            self._logger.setLevel(level)
        self._handlerConsole.setLevel(level)
        
    def configLogFile(self, level, path=None):
        if level < self._minLevel:
            self._minLevel = level
            self._logger.setLevel(level)
        if self._handlerLogFile is None and path is not None:
            self._handlerLogFile = logging.FileHandler(path)
            self._handlerLogFile.setLevel(level)
            self._handlerLogFile.setFormatter(self._formatter)
            self._logger.addHandler(self._handlerLogFile)
        elif self._handlerLogFile is not None:
            self._handlerLogFile.setLevel(level)
            
    
   

# Initialize Logger
logmgr = LogManager()
logger = logging.getLogger()







        