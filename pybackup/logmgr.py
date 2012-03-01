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
defaultLogFormat = '%(asctime)s:%(job)s:%(levelname)-8s %(message)s'
defaultDateFormat = '%Y-%m-%d %H:%M:%S'


# Logging Configuration
logging.basicConfig(format=defaultLogFormat,
                    datefmt=defaultDateFormat)
logger = logging.getLogger()
logger.setLevel(defaultLogLevel)


class LogContext(logging.Filter):
    
    def __init__(self, job_name):
        self._jobName = job_name
        logging.Filter.__init__(self)
    
    def setJob(self, job_name):
        self._jobName = job_name
    
    def filter(self, record):
        record.job = self._jobName
        return True
        
logContext = LogContext('INIT')
logger.addFilter(logContext)
