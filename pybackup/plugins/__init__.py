"""pybackup - Base Classes for Backup Plugins

"""


from pybackup import defaults
from pybackup import errors


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"



class BackupPluginRegistry():
    
    def __init__(self):
        self._methodDict = {}
        
    def register(self, name, method):
        self._methodDict[name] = method
        
    def getList(self):
        return self._methodDict.keys()
    
    def hasMethod(self, name):
        return self._methodDict.has_key(name)
    
    def runMethod(self, name, conf):
        self._methodDict[name](conf)
            

backupPluginRegistry = BackupPluginRegistry()



class BackupPluginBase():
    
    _optList = ()
    _requiredOptList = ('job_name', 'backup_path')
    _defaults = {}
    
    def __init__(self, **kwargs):
        self._conf = {}
        for k in self._optList:
            self._conf[k] = (kwargs.get(k) or self._defaults.get(k) 
                             or defaults.globalConf.get(k)) 
        for k in self._requiredOptList:
            if self._conf[k] is None:
                raise errors.BackupEnvironmentError("Mandatory configuration "
                                                    "option %s not defined." % k)
                
