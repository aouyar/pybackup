"""pybackup - Base Classes for Backup Plugins

"""


import types
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
        
    def register(self, name, func, cls=None):
        if cls is None:
            if isinstance(func, types.FunctionType):
                self._methodDict[name] = (None, func)
                return True
            else:
                raise AttributeError("Function registered by plugin for "
                                     "backup method %s is invalid." % name)
        else:
            try:
                if issubclass(cls, BackupPluginBase):
                    try:
                        attr = getattr(cls, func)
                        if not isinstance(attr, types.UnboundMethodType):
                            raise
                    except:
                        raise AttributeError("Function registered by plugin for "
                                             "backup method %s is not a valid "
                                             "method name for class %s."
                                             % (name, cls.__name__))
                    else:
                        self._methodDict[name] = (cls, func)
                else:
                    raise AttributeError("Class registered by plugin for "
                                         "backup method %s is not a subclass "
                                         "of BackupPluginBase class." %  name)
            except TypeError:
                raise TypeError("The cls argument in registeration of plugin for "
                                "backup method %s is not a valid class." %  name)
        
    def getList(self):
        return self._methodDict.keys()
    
    def hasMethod(self, name):
        return self._methodDict.has_key(name)
    
    def runMethod(self, name, conf):
        if self._methodDict.has_key(name):
            cls, func = self._methodDict[name]
            if cls is None:
                func(conf)
            else:
                obj = cls(**conf)
                getattr(obj, func)()
        else:
            raise errors.BackupConfigError("Invalid backup method name: %s"
                                           % name)
            
backupPluginRegistry = BackupPluginRegistry()


class BackupPluginBase():
    
    _optList = ()
    _requiredOptList = ('job_name', 'backup_path')
    _defaults = {}
    
    def __init__(self, **kwargs):
        self._conf = {}
        self._methodDict = {}
        for k in self._optList:
            self._conf[k] = (kwargs.get(k) or self._defaults.get(k) 
                             or defaults.globalConf.get(k)) 
        for k in self._requiredOptList:
            if self._conf[k] is None:
                raise errors.BackupConfigError("Mandatory configuration "
                                               "option %s not defined." % k)
            
