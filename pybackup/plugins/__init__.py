"""pybackup - Base Classes for Backup Plugins

"""

import imp
import sys
import os
import types
import subprocess
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


# Defaults
bufferSize = 8192



def loadModule(module, parent=None, path=None):
    (mod, sep, tail) = module.partition('.') #@UnusedVariable
    if parent is not None:
        modname = '%s.%s' % (parent, mod)
    else:
        modname = mod
    if not sys.modules.has_key(modname):
        fp = None
        try:
            try:
                fp, pathname, description = imp.find_module(mod, path)
                modobj = imp.load_module(modname, fp, pathname, description)
            except ImportError:
                raise
        finally:
            if fp:
                fp.close()
    else:
        modobj = sys.modules[modname]
    if tail == '':
        return modobj
    else:
        return loadModule(tail, modname, modobj.__path__)

    
    
class BackupPluginRegistry:
    
    def __init__(self):
        self._plugins = {}
        self._methodDict = {}
        
    def loadPlugin(self, plugin, module):
        if not self._plugins.has_key(plugin):
            # Fast path: see if the module has already been imported.
            if sys.modules.has_key(plugin):
                modobj = sys.modules[plugin]
            else:
                try:
                    modobj = loadModule(module)
                except ImportError, e:
                    raise errors.BackupConfigError(
                        "Failed loading backup plugin: %s   Module: %s" 
                        % (plugin, module), str(e))
            self._plugins[plugin] = {'module': module,
                                     'desc': '', 
                                     'methods': []}
            logger.debug("Backup plugin loaded: %s    Module: %s" % (plugin, 
                                                                     module))
            if hasattr(modobj, 'methodList'):
                for (name, cls, func) in modobj.methodList:
                    try:
                        if issubclass(cls, BackupPluginBase):
                            try:
                                attr = getattr(cls, func)
                                if not isinstance(attr, types.UnboundMethodType):
                                    raise
                            except:
                                raise errors.BackupBadPluginError(
                                    "Function for backup method %s is not a valid "
                                    "method name for class %s in plugin %s (%s)."
                                    % (name, cls.__name__, plugin, module))
                            self._methodDict[name] = (cls, func)
                            self._plugins[plugin]['methods'].append(name)
                            logger.debug("Registered backup method %s from" 
                                         " plugin %s.", name, plugin)
                        else:
                            raise errors.BackupBadPluginError(
                                "Class for backup method %s in plugin %s (%s)"
                                " is not a subclass of BackupPluginBase." 
                                %  (name, plugin, module))
                    except TypeError:
                        raise errors.BackupBadPluginError(
                                "The class for backup method %s in plugin"
                                " %s (%s) is not a valid class." 
                                %  (name, plugin, module))
            else:
                raise errors.BackupBadPluginError("Plugin %s (%s) does not define"
                                                  " any methods to be registered."
                                                  % (plugin, module))
            if hasattr(modobj, 'description'):
                self._plugins[plugin]['description'] = modobj.description
            return modobj
        
    def getPluginList(self):
        return self._plugins.keys()
    
    def getPluginDesc(self, plugin):
        if self._plugins.has_key(plugin):
            return self._plugins[plugin]['description']
        else:
            raise errors.BackupConfigError("Invalid backup plugin name: %s"
                                           % plugin)
        
    def getMethodList(self, plugin):
        if self._plugins.has_key(plugin):
            return self._plugins[plugin]['methods']
        else:
            raise errors.BackupConfigError("Invalid backup plugin name: %s"
                                           % plugin)
    
    def hasMethod(self, name):
        return self._methodDict.has_key(name)
    
    def runMethod(self, name, global_conf, job_conf):
        if self._methodDict.has_key(name):
            (cls, func) = self._methodDict[name]
            obj = cls(global_conf, job_conf)
            getattr(obj, func)()
        else:
            raise errors.BackupConfigError("Invalid backup method name: %s"
                                           % name)
    
    def helpMethod(self, name):
        if self._methodDict.has_key(name):
            (cls, func) = self._methodDict[name] #@UnusedVariable
            return cls.getHelpText()
        else:
            None
            
backupPluginRegistry = BackupPluginRegistry()


class BackupPluginBase:
    
    _baseOpts = {'job_name': 'Job name.', 
                 'job_path': 'Backup path for job.',
                 'active': 'Enable / disable backups job. (yes / no)',
                 'method': 'Backup plugin method name.',
                 'user': 'If defined, check if script is being run by user.',
                 'job_pre_exec': 'Script to be executed before backup job.',
                 'job_post_exec': 'Script to be executed after backup job.',}
    _extOpts = {}
    _baseReqOptList = ('job_path',)
    _extReqOptList = ()
    _globalReqOptList = ('cmd_compress', 'suffix_compress',
                         'cmd_tar', 'suffix_tar', 'suffix_tgz',)
    _baseDefaults = {}
    _extDefaults = {}
    _methodDict = {}
    
    def __init__(self, global_conf, job_conf):
        self._conf = {}
        self._env = None
        self._dryRun = global_conf.get('dry_run', False)
        for k in self._globalReqOptList:
            if not global_conf.has_key(k):
                raise errors.BackupFatalConfigError("Required global configuration "
                                                    "option %s not defined." % k)
        for k in self._baseReqOptList + self._extReqOptList:
            if not job_conf.has_key(k):
                raise errors.BackupConfigError("Required job configuration "
                                               "option %s not defined." % k)
        for k in job_conf:
            if not (self._baseOpts.has_key(k) or self._extOpts.has_key(k)):
                raise errors.BackupConfigError("Invalid job option: %s" % k)
        self._conf.update(self._baseDefaults)
        self._conf.update(self._extDefaults)
        self._conf.update(global_conf)
        self._conf.update(job_conf)
        
    @classmethod
    def getHelpText(cls):
        lines = []
        lines.append("Base Options")
        for opt in sorted(cls._baseOpts.keys()):
            desc = cls._baseOpts[opt]
            lines.append("    %-24s: %s" % (opt, desc))
        lines.append("")
        lines.append("Plugin Options")
        for opt in sorted(cls._extOpts.keys()):
            desc = cls._extOpts[opt]
            lines.append("    %-24s: %s" % (opt, desc))
        lines.append("")
        return "\n".join(lines)
        
    def _execBackupCmd(self, args, env=None, out_path=None, out_compress=False, 
                       force_exec=False):
        out_fp = None
        if out_path is not None:
                try:
                    out_fp = os.open(out_path, os.O_WRONLY | os.O_CREAT, 0666)
                except Exception, e:
                    raise errors.BackupFileCreateError(
                        "Failed creation of backup file: %s" % out_path,
                        "Error Message: %s" % str(e))
        if not force_exec and self._dryRun:
            logger.debug("Fake execution of command: %s", ' '.join(args))
            return (0, '', '')
        logger.debug("Executing command: %s", ' '.join(args))
        if out_path is not None and out_compress:
            try:
                cmd = subprocess.Popen(args, 
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, 
                                       bufsize=bufferSize,
                                       env=env)
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
                                            bufsize=bufferSize)
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
                                         "with error code: %s" % cmd_comp.returncode,
                                         utils.splitMsg(comp_err))
        else:
            try:
                cmd = subprocess.Popen(args,
                                       stdout=(out_fp or subprocess.PIPE), 
                                       stderr=subprocess.PIPE, 
                                       bufsize=bufferSize,
                                       env = env)
            except Exception, e:
                raise errors.BackupCmdError("Backup command execution failed.",
                                            "Command: %s" % ' '.join(args),
                                            "Error Message: %s" % str(e))
            out, err = cmd.communicate(None)
            return (cmd.returncode, out, err)