#!/usr/bin/env python

import imp
import sys
import os
import platform
import ConfigParser
import optparse
import logging
from datetime import date
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger, logmgr
from pybackup.plugins import backupPluginRegistry
from pysysinfo.util import parse_value

__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


# Defaults
defaultConfigPaths = ['./pybackup.conf', '/etc/pybackup.conf']


def parseCmdline(argv=None):
    """Parses commandline options."""
    parser = optparse.OptionParser()
    parser.add_option('-c', '--conf', help='Path for configuration file.',
                      dest='confPath', default=None, 
                      action='store')
    parser.add_option('-q', '--quiet', 
                      help='Minimal logging on console. Only print errors.',
                      dest='quiet', default=False, action='store_true')
    parser.add_option('-d', '--debug', help='Activate debugging mode.',
                      dest='debug', default=False, action='store_true')
    parser.add_option('-t', '--trace', help='Activate tracing of errors.',
                      dest='trace', default=False, action='store_true')
    parser.add_option('-a', '--all', 
                      help='Run all jobs listed in configuration file.',
                      dest='allJobs', default=False, action='store_true')
    if argv is None:
        (cmdopts, args) = parser.parse_args()
    else:
        (cmdopts, args) = parser.parse_args(argv[1:])
    if cmdopts.trace:
        errors.set_trace()
    opts = {}
    if cmdopts.debug:
        opts['console_loglevel'] = 'debug'
        opts['logfile_loglevel'] = 'debug'
    elif cmdopts.quiet:
        opts['console_loglevel'] = 'error'
    if cmdopts.confPath:
        opts['config_path'] = [cmdopts.confPath, ]
    else:
        opts['config_path'] = defaultConfigPaths
    if cmdopts.allJobs:
        jobs = None
    elif len(args) > 0:
        jobs = args
    else:
        raise errors.BackupStartupError("No backup job selected for execution.")
    return (opts, jobs)


class JobManager():
    
    _globalOpts = {'backup_root': 'Root directory for storing backups.',
                   'hostname_dir': 'Create subdirectory for each hostname. (yes/no)', 
                   'user': 'If defined, check if script is being run by user.',
                   'umask': 'Umask for file and directory creation.',
                   'console_loglevel': 'Logging level for console.', 
                   'logfile_loglevel': 'Logging level for log file.',
                   'filename_logfile': 'Filename for log file.',}
    _reqGlobalOpts = ('backup_root',)
    _globalConf = {'console_loglevel': 'info',
                   'logfile_loglevel': 'info',
                   'filename_logfile': 'backup.log',
                   'cmd_compress': 'gzip', 
                   'suffix_compress': 'gz',
                   'cmd_tar': 'tar',
                   'suffix_tar': 'tar',
                   'suffix_tgz': 'tgz',}
    
    def __init__(self, opts, jobs):
        self._jobsConf = None
        self._jobs = jobs
        self._cmdConf = opts
        self._globalConf.update(opts)

    def loggingInit(self):
        logmgr.setContext('STARTUP')
        level = logmgr.getLogLevel(self._globalConf['console_loglevel'])
        logmgr.configConsole(level)
        logger.info("Start Execution of Backup Jobs.")
        
    def loggingEnd(self):
        logmgr.setContext('FINAL')
        logger.info("Finished Execution of Backup Jobs.")
        
    def loggingConfig(self):
        console_level = logmgr.getLogLevel(self._globalConf['console_loglevel'])
        if console_level is None:
            raise errors.BackupFatalConfigError("Invalid log level set in "
                                                "configuration file for option: "
                                                "console_loglevel")
        logfile_level = logmgr.getLogLevel(self._globalConf['logfile_loglevel'])  
        if logfile_level is None:
            raise errors.BackupFatalConfigError("Invalid log level set in "
                                                "configuration file for option: "
                                                "logfile_loglevel")
        logmgr.configConsole(console_level)
        backup_path = self._globalConf['backup_path']
        self.createBaseDir()
        filename_logfile = self._globalConf['filename_logfile']
        log_path = os.path.join(backup_path, filename_logfile)
        logmgr.configLogFile(logfile_level, log_path)
        logger.debug("Activated logging to file: %s" % log_path)
            
    def parseConfFile(self):
        confmgr = ConfigParser.SafeConfigParser()
        read_paths = confmgr.read(self._globalConf['config_path'])
        if not read_paths:
            raise errors.BackupFatalConfigError("Configuration file not found in any"
                                                " of the following locations: %s" 
                                                % ' '.join(self._globalConf['config_path']))
        logger.debug("Parsing configuration file: %s" % ', '. join(read_paths))
        if not confmgr.has_section('general'):
            raise errors.BackupFatalConfigError("Missing mandatory section 'general' "
                                                "in configuration file(s): %s" 
                                                % ' '.join(self._configPath))
        self._jobsConf = {}
        for section in confmgr.sections():
            if section == 'general':
                options = confmgr.items('general')
                global_conf = dict(options)
            else:
                options = confmgr.items(section)
                self._jobsConf[section] = dict(options)
        if self._jobs is None:
            self._jobs = self._jobsConf.keys()
        for (k,v) in global_conf.items():
            if self._globalOpts.has_key(k):
                self._globalConf[k] = self._cmdConf.get(k) or v
            else:
                raise errors.BackupFatalConfigError("Invalid general option %s "
                                                    "in configuration file." % k)
        for k in self._reqGlobalOpts:
            if not self._globalConf.has_key(k):
                raise errors.BackupFatalConfigError("Required option %s is not"
                                                    "defined in configuration "
                                                    "file.", k)
        backup_path_elem = [self._globalConf['backup_root'], ]
        if self._globalConf.has_key('hostname_dir'):
            backup_path_elem.append(str(platform.node()).split('.')[0])
        backup_path_elem.append(date.today().strftime('%Y-%m-%d'))
        self._globalConf['backup_path'] = os.path.join(*backup_path_elem)
        
    def initUmask(self):
        umask = self._globalConf.get('umask')
        if umask is not None:
            os.umask(int(umask, 8))
            logger.debug("OS Umask set to: %s", umask)
        
    def checkUser(self):
        user = self._globalConf.get('user')
        if user is not None and not utils.checkUser(user):
            raise errors.BackupFatalEnvironmentError("Backup script must be run "
                                                     "as user %s." % user)
            
    def createBaseDir(self):
        backup_path = self._globalConf['backup_path']
        if not os.path.isdir(backup_path):
            try:
                os.makedirs(backup_path)
            except:
                raise errors.BackupFatalEnvironmentError("Creation of backup base "
                                                         "directory (%s) failed."
                                                         % backup_path)
        logger.debug("Backup base directory (%s) created.", backup_path)
        
    def runJobs(self):
        for job_name in self._jobs:
            logmgr.setContext(job_name)
            job_conf = self._jobsConf.get(job_name)
            try:
                if job_conf:
                    if job_conf.has_key('active'):
                        active = parse_value(job_conf['active'], True)
                        if not active:
                            logger.warn("Backup job disabled by configuration.")
                            continue
                    logger.info("Starting execution of backup job")
                    job = BackupJob(job_name, self._globalConf, job_conf)
                    job.run()
                    logger.info("Finished execution of backup job")
                else:
                    raise errors.BackupConfigError("No configuration found for "
                                                   "backup job.")
            except errors.BackupError, e:
                if e.trace or e.fatal:
                    raise
                else:
                    if e.fatal:
                        level = logging.CRITICAL
                    else:
                        level = logging.ERROR
                    logger.log(level, e.desc)
                    for line in e:
                        logger.log(level, "  %s" , line)
    
    def run(self):
        self.loggingInit()
        self.parseConfFile()
        self.checkUser()
        self.initUmask()
        self.createBaseDir()
        self.loggingConfig()
        self.runJobs()
        self.loggingEnd()
        

class BackupJob():
    
    _reqGlobalOpts = ('backup_path',)
    _reqJobOpts = ('plugin', 'method',)
    
    def __init__(self, name, global_conf, job_conf):
        self._globalConf = global_conf
        self._jobConf = {'job_name': name}
        self._jobConf.update(job_conf)
        for k in self._reqGlobalOpts:
            if not self._globalConf.has_key(k):
                raise errors.BackupConfigError("Required global configuration "
                                                "option %s not defined." % k)
        for k in self._reqJobOpts:
            if not self._jobConf.has_key(k):
                raise errors.BackupConfigError("Required job configuration "
                                                "option %s not defined." % k)
        self._jobConf['job_path'] = os.path.join(self._globalConf['backup_path'], 
                                                 name)

    def loadPlugin(self):
        plugin = self._jobConf.get('plugin')
        # Fast path: see if the module has already been imported.
        try:
            return sys.modules[plugin]
        except KeyError:
            pass
        fp = None
        try:
            fp, pathname, description = imp.find_module(plugin, ['./plugins',])
            return imp.load_module(plugin, fp, pathname, description)
        except ImportError, e:
            raise errors.BackupConfigError("Failed loading backup plugin: %s"
                                            % plugin, str(e))
        finally:
            if fp:
                fp.close()
        logger.debug("Backup plugin loaded: %s" % plugin)
    
    def checkUser(self):
        user = self._jobConf.get('user') or self._globalConf.get('user')
        if user is not None and not utils.checkUser(user):
            raise errors.BackupEnvironmentError("Backup job must be run as user %s." 
                                                % user)
    
    def initJobDir(self):
        path = self._jobConf['job_path']
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except:
                raise errors.BackupEnvironmentError("Creation of backup job "
                                                    "directory (%s) failed."
                                                    % path)
            logger.debug("Backup job directory (%s) created.", path)
    
    def runMethod(self):   
        method = self._jobConf.get('method')
        if backupPluginRegistry.hasMethod(method):
            backupPluginRegistry.runMethod(method,
                                           self._globalConf, 
                                           self._jobConf)
        else:
            raise errors.BackupConfigError("Invalid backup method. "
                                           "Backup method %s not registered." 
                                           % method)
    def run(self):
        self.checkUser()
        self.loadPlugin()
        self.initJobDir()
        self.runMethod()


    
def main(argv=None):
    try:
        (opts, jobs) = parseCmdline(argv)
        jobmgr = JobManager(opts, jobs)
        jobmgr.run()
    except errors.BackupError, e:
        if e.fatal:
            level = logging.CRITICAL
        else:
            level = logging.ERROR
        logger.log(level, e.desc)
        for line in e:
            logger.log(level, "  %s" , line)
        if e.trace:
            raise
        elif e.fatal:
            return 1
    return 0
            

if __name__ == "__main__":
    sys.exit(main())
