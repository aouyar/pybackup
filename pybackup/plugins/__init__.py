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


def loadModule(module):
    """Loads module recursively.
        
        @param module: Module name.
        
    """
    
    def load_module(module, parent=None, path=None):
        """Function to implement recursive module loading.
        
        @param module: Module name.
        @param parent: Module parent name.
        @param path:   Path for module file. (Must be None for starting recursion.)
        @return:       Module object.
                
        """
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
            return load_module(tail, modname, modobj.__path__)
        
    return load_module(module)

    
    
class BackupPluginRegistry:
    """Class that implements registration and administration of backup plugins.
    
    """
    
    def __init__(self):
        """Constructor
        
        """
        self._plugins = {}
        self._methodDict = {}
        
    def loadPlugin(self, plugin, module):
        """
        
        @param plugin: Plugin name.
        @param module: Module name.
        @return:       Module object.
              
        """
        if not self._plugins.has_key(module):
            # Fast path: see if the module has already been imported.
            if sys.modules.has_key(module):
                modobj = sys.modules[module]
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
        """Returns list of registered plugins.
        
        @return: List of registered plugins.
        
        """
        return self._plugins.keys()
    
    def getPluginDesc(self, plugin):
        """Returns plugin description text.
        
        @param plugin: Plugin name.
        @return:       Description string.
        
        """
        if self._plugins.has_key(plugin):
            return self._plugins[plugin]['description']
        else:
            raise errors.BackupConfigError("Invalid backup plugin name: %s"
                                           % plugin)
        
    def getMethodList(self, plugin):
        """Returns list of methods registered for plugin.
        
        @param plugin: Plugin name.
        @return:       List of methods registered for plugin.
        """
        if self._plugins.has_key(plugin):
            return self._plugins[plugin]['methods']
        else:
            raise errors.BackupConfigError("Invalid backup plugin name: %s"
                                           % plugin)
    
    def hasMethod(self, name):
        """Returns True if method with name is registered.
        
        @param name: Name of method.
        @return:     Boolean
        
        """
        return self._methodDict.has_key(name)
    
    def runMethod(self, name, global_conf, job_conf):
        """Runs method.
        
        @param name:        Backup method name.
        @param global_conf: Dictionary of general configuration options.
        @param job_conf:    Dictionary of job configuration options.
        
        """
        if self._methodDict.has_key(name):
            (cls, func) = self._methodDict[name]
            obj = cls(global_conf, job_conf)
            getattr(obj, func)()
        else:
            raise errors.BackupConfigError("Invalid backup method name: %s"
                                           % name)
    
    def helpMethod(self, name):
        """
        
        @param name: Backup method name.
        @return:     Multi-line help text for method.
        
        """
        if self._methodDict.has_key(name):
            (cls, func) = self._methodDict[name] #@UnusedVariable
            return cls.getHelpText()
        else:
            None
            
backupPluginRegistry = BackupPluginRegistry()
"""Plugin registry instance. (Normally additional instances should not be needed.)"""



class BackupPluginBase:
    """Base class for backup plugins.
    
    Backup plugins must extend this class.
    
    """
    
    _baseOpts = {'job_name': 'Job name.', 
                 'job_path': 'Backup path for job.',
                 'active': 'Enable / disable backups job. (yes / no)',
                 'method': 'Backup plugin method name.',
                 'user': 'If defined, check if script is being run by user.',
                 'job_pre_exec': 'Script to be executed before backup job.',
                 'job_post_exec': 'Script to be executed after backup job.',}
    """Configuration options common to all plugins."""
    
    _extOpts = {}
    """To be overwritten in plugins to define plugin specific configuration 
    options."""
    
    _baseReqOptList = ('job_path',)
    """List of required options common to all plugins."""
    
    _extReqOptList = ()
    """To be overwritten in plugins to define plugin specific required options."""
    
    _globalReqOptList = ('cmd_compress', 'suffix_compress',
                         'cmd_tar', 'suffix_tar', 'suffix_tgz',)
    """List of required general configuration options common to all plugins."""
    
    _baseDefaults = {}
    """Dictionary of defaults for all plugins."""
    
    _extDefaults = {}
    """Dictionary of plugin specific defaults."""
    
    _methodDict = {}
    
    def __init__(self, global_conf, job_conf):
        """Constructor
        
        @param global_conf: Dictionary of general configuration options.
        @param job_conf:    Dictionary of job configuration options.
        
        """
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
        """Returns help text for plugin.
        
        @return: Multi-line help text.
        
        """
        lines = []
        lines.append("Plugin Generic Options")
        for opt in sorted(cls._baseOpts.keys()):
            desc = cls._baseOpts[opt]
            lines.append("    %-24s: %s" % (opt, desc))
        lines.append("")
        lines.append("Plugin Specific Options")
        for opt in sorted(cls._extOpts.keys()):
            desc = cls._extOpts[opt]
            lines.append("    %-24s: %s" % (opt, desc))
        return "\n".join(lines)
        
    def _execBackupCmd(self, args, env=None, out_path=None, out_compress=False, 
                       force_exec=False):
        """Executes backup command.
        
        @param args:         List of command arguments. The executable path must
                             be passed as first argument.
        @param env:          Dictionary of environment variables for running
                             backup command. The execution environment is not 
                             modified by default. 
        @param out_path:     Redirect output to file with path out_path if defined.
        @param out_compress: The output to file will be compressed if True.
        @param force_exec:   Force execution of command even for dry-run.
        @return:             Tuple of return code, standard output text,
                             standard error text.
        
        """
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