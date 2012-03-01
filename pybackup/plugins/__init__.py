"""pybackup - Base Classes for Backup Plugins

"""

import os
import types
import subprocess
from pybackup import defaults
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger

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
    
    _baseOptList = ('job_name', 'backup_path',
                    'cmd_compress', 'suffix_compress',)
    _optList = ()
    _baseReqOptList = ('job_name', 'backup_path',
                       'cmd_compress', 'suffix_compress',)
    _reqOptList = ()
    _defaults = {}
    
    def __init__(self, **kwargs):
        self._conf = {}
        self._methodDict = {}
        for k in self._baseOptList + self._optList:
            self._conf[k] = (kwargs.get(k) or self._defaults.get(k) 
                             or defaults.globalConf.get(k)) 
        for k in self._baseReqOptList + self._reqOptList:
            if self._conf[k] is None:
                raise errors.BackupConfigError("Mandatory configuration "
                                               "option %s not defined." % k)
    
    def _execBackupCmd(self, args, out_path=None, out_compress=False):
        out_fp = None
        if out_path is not None:
                try:
                    out_fp = os.open(out_path, os.O_WRONLY | os.O_CREAT)
                except Exception, e:
                    raise errors.BackupFileCreateError(
                        "Failed creation of backup file: %s" % out_path,
                        "Error Message: %s" % str(e))
        if out_path is not None and out_compress:
            logger.debug("Executing command: %s", ' '.join(args))
            try:
                cmd = subprocess.Popen(args, 
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, 
                                       bufsize=defaults.bufferSize,
                                       env = self._env)
            except Exception, e:
                raise errors.BackupCmdError("Backup command execution failed.",
                                            "Command: %s" % ' '.join(args),
                                            "Error Message: %s" % str(e))
            args_comp = [self._conf['cmd_compress'], '-c']
            try:
                cmd_comp = subprocess.Popen(args_comp,
                                            stdin=cmd.stdout,
                                            stdout=out_fp,
                                            stderr=subprocess.PIPE,
                                            bufsize=defaults.bufferSize)
                cmd.stdout.close()
            except Exception, e:
                raise errors.BackupCmdError("Backup compression command failed.",
                                            "Command: %s" % ' '.join(args_comp),
                                            "Error Message: %s" % str(e))
            comp_out, comp_err = cmd_comp.communicate(None) #@UnusedVariable
            err = cmd.stderr.read()
            cmd.wait()
            if cmd_comp.returncode == 0:
                return (cmd.returncode, '', err)
            else:
                raise errors.BackupError("Compression of backup failed "
                                         "with error code %s." % cmd_comp.returncode,
                                         utils.split_msg(comp_err))
        else:
            logger.debug("Executing command: %s", ' '.join(args))
            try:
                cmd = subprocess.Popen(args,
                                       stdout=(out_fp or subprocess.PIPE), 
                                       stderr=subprocess.PIPE, 
                                       bufsize=defaults.bufferSize,
                                       env = self._env)
            except Exception, e:
                raise errors.BackupCmdError("Backup command execution failed.",
                                            "Command: %s" % ' '.join(args),
                                            "Error Message: %s" % str(e))
            out, err = cmd.communicate(None)
            return (cmd.returncode, out, err)