"""pybackup - Base Classes for Backup Plugins

"""


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

