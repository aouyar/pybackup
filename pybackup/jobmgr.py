#!/usr/bin/env python

import imp
import sys
import os
import platform
import ConfigParser
import optparse
import logging
from datetime import date
from pybackup import defaults
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



def parseCmdline(argv=None):
    """Parses commandline options."""
    parser = optparse.OptionParser()
    parser.add_option('-c', '--conf', help='Path for configuration file.',
                      dest='confPath', default=None, action='store')
    parser.add_option('-v', '--verbose', help='Activate verbose logging.',
                      dest='verbose', default=False, action='store_true')
    parser.add_option('-d', '--debug', help='Activate debugging mode.',
                      dest='debug', default=False, action='store_true')
    parser.add_option('-t', '--trace', help='Activate tracing of errors.',
                      dest='trace', default=False, action='store_true')
    parser.add_option('-a', '--all', 
                      help='Run all jobs listed in configuration file.',
                      dest='allJobs', default=False, action='store_true')
    if argv is None:
        return parser.parse_args()
    else:
        return parser.parse_args(argv[1:])
    
def parseConfFile(conf_paths):
    confmgr = ConfigParser.SafeConfigParser()
    read_paths = confmgr.read(conf_paths)
    if not read_paths:
        raise errors.BackupFatalConfigError("Configuration file not found in any"
                                            " of the following locations: %s" 
                                            % ' '.join(conf_paths))
    if not confmgr.has_section('general'):
        raise errors.BackupFatalConfigError("Missing mandatory section 'general' "
                                            "in configuration file(s): %s" 
                                            % ' '.join(read_paths))
    jobs_conf = {}
    for section in confmgr.sections():
        if section == 'general':
            options = confmgr.items('general')
            global_conf = dict(options)
        else:
            options = confmgr.items(section)
            jobs_conf[section] = dict(options)
    return (global_conf, jobs_conf)


def initGlobals(global_conf):
    user = global_conf.get('user')
    if user is not None and not utils.checkUser(user):
        raise errors.BackupFatalEnvironmentError("Backup script must be run "
                                                 "as user %s." % user)
    umask = global_conf.get('umask')
    if umask is not None:
        os.umask(int(umask, 8))
    if global_conf.has_key('backup_root'):
        backup_path_elem = [global_conf['backup_root'], ]
        if global_conf.has_key('hostname_dir'):
            backup_path_elem.append(str(platform.node()).split('.')[0])
            backup_path_elem.append(date.today().strftime('%Y-%m-%d'))
            global_conf['backup_path'] = os.path.join(*backup_path_elem)
        else:
            raise errors.BackupFatalConfigError("Backup root directory "
                                                "(backup_root) not defined.")
    
def initLogging(global_conf, verbose=False, debug=False):
    console_level = None
    logfile_level = None
    if debug:
        console_level = logging.DEBUG
        logfile_level = logging.DEBUG
    elif verbose:
        console_level = logging.INFO
        logfile_level = logging.INFO
    elif global_conf is not None:
        console_level = logmgr.getLogLevel( 
                            global_conf.get('console_loglevel') or 
                            defaults.globalConf.get('console_loglevel'))
        if console_level is None:
            raise errors.BackupFatalConfigError("Invalid log level set in "
                                                "configuration file for option: "
                                                "console_loglevel")
        logfile_level = logmgr.getLogLevel( 
                            global_conf.get('logfile_loglevel') or 
                            defaults.globalConf.get('logfile_loglevel'))
        if logfile_level is None:
            raise errors.BackupFatalConfigError("Invalid log level set in "
                                                "configuration file for option: "
                                                "logfile_loglevel")
    if console_level is not None:
        logmgr.configConsole(console_level)
    if global_conf is not None:
        backup_path = global_conf.get('backup_path')
        if backup_path is not None:
            if not os.path.isdir(backup_path):
                try:
                    os.makedirs(backup_path)
                except:
                    raise errors.BackupEnvironmentError("Creation of backup base "
                                                        "directory (%s) failed."
                                                        % backup_path)
                logger.debug("Backup base directory (%s) created.", backup_path)
            filename_logfile = (global_conf.get('filename_logfile')
                                or defaults.globalConf.get('filename_logfile'))
            if filename_logfile is None:
                raise errors.BackupFatalConfigError("Backup log filename "
                                                "(filename_logfile) not defined.")
            log_path = os.path.join(backup_path, filename_logfile)
            logmgr.configLogFile(logfile_level, log_path)
        else:
            raise errors.BackupFatalConfigError("Backup base directory "
                                                "(backup_path) not defined.")
    

class BackupJob():
    
    def __init__(self, name, global_conf, job_conf):
        self._name = name
        self._conf = {'job_name': name}
        self._conf.update(global_conf)
        self._conf.update(job_conf)
        if self._conf.has_key('backup_path'):
            self._conf['job_path'] = os.path.join(self._conf['backup_path'], name)
        else:
            raise errors.BackupFatalConfigError("Backup base directory "
                                                "(backup_path) not defined.")

    def _loadPlugin(self, plugin):
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
    
    def _initBackupDir(self, path):
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except:
                raise errors.BackupEnvironmentError("Creation of backup job "
                                                    "directory (%s) failed."
                                                    % path)
            logger.debug("Backup job directory (%s) created.", path)
    
    def _runMethod(self, method, conf):   
        if backupPluginRegistry.hasMethod(method):
            backupPluginRegistry.runMethod(method, conf)
        else:
            raise errors.BackupConfigError("Invalid backup method. "
                                           "Backup method %s not registered." 
                                           % method)
    def run(self):
        user = self._conf.get('user') or None
        if user is not None and not utils.checkUser(user):
            raise errors.BackupEnvironmentError("Backup job must be run as user %s." 
                                                % user)
        plugin = self._conf.get('plugin')
        if plugin is not None:
            self._loadPlugin(plugin)
        logger.debug("Backup plugin loaded: %s" % plugin)
        self._initBackupDir(self._conf['job_path'])
        method = self._conf.get('method')
        if method is not None:
            self._runMethod(method, self._conf)
        else:
            raise errors.BackupConfigError("Backup method not defined.")

    
def main(argv=None):
    (cmdopts, args) = parseCmdline(argv)
    initLogging(None, cmdopts.verbose, cmdopts.debug)
    trace_errors = cmdopts.trace
    if cmdopts.confPath is not None:
        conf_path = cmdopts.confPath
    else:
        conf_path = defaults.configPaths
    try:
        (global_conf, jobs_conf) = parseConfFile(conf_path)
    except errors.BackupConfigError, msg:
        logger.critical(msg)
        if trace_errors:
            raise
        else:
            return 1
    if cmdopts.allJobs:
        jobs = jobs_conf.keys()
    elif len(args) > 0:
        jobs = args
    else:
        logger.warning("No backup job selected for execution.")
        return 1
    try:
        initGlobals(global_conf)
        initLogging(global_conf, cmdopts.verbose, cmdopts.debug)
    except errors.BackupError, e:
        if e.fatal:
            logger.critical(e.desc)
        else:
            logger.error(e.desc)
        for line in e:
            logger.error("  %s" , line)
        if trace_errors:
            raise
        elif e.fatal:
            return 1
    for job_name in jobs:
        try:
            logmgr.setContext(job_name)
            job_conf = jobs_conf.get(job_name)
            if job_conf:
                if job_conf.has_key('active'):
                    active = parse_value(job_conf['active'], True)
                    if not active:
                        logger.info("Backup job disabled by configuration.")
                        continue
                logger.info("Starting execution of backup job")
                job = BackupJob(job_name, global_conf, job_conf)
                job.run()
                logger.info("Finished execution of backup job")
            else:
                logger.error("No configuration found for backup job.")
        except errors.BackupError, e:
            if e.fatal:
                logger.critical(e.desc)
            else:
                logger.error(e.desc)
            for line in e:
                logger.error("  %s" , line)
            if trace_errors:
                raise
            elif e.fatal:
                return 1
                

if __name__ == "__main__":
    sys.exit(main())