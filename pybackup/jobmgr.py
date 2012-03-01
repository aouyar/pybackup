#!/usr/bin/env python

import imp
import sys
import os
import platform
import pwd
import ConfigParser
import optparse
import logging
from datetime import date
from pybackup import defaults
from pybackup import errors
from pybackup.logmgr import logger, logContext
from pybackup.plugins import backupPluginRegistry


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
    umask = global_conf.get('umask')
    if umask is not None:
        os.umask(int(umask, 8))
        

class BackupJob():
    
    def __init__(self, name, global_conf, job_conf):
        self._name = name
        self._conf = {'job_name': name}
        self._conf.update(global_conf)
        self._conf.update(job_conf)
        if self._conf.has_key('backup_root'):
            backup_path_elem = [self._conf['backup_root'], ]
            if self._conf.has_key('hostname_dir'):
                backup_path_elem.append(str(platform.node()).split('.')[0])
            backup_path_elem.append(date.today().strftime('%Y-%m-%d'))
            backup_path_elem.append(name)
            self._conf['backup_path'] = os.path.join(*backup_path_elem)
        else:
            raise errors.BackupFatalConfigError("Backup root directory "
                                                "(backup_root) not defined.")

    def _checkUser(self, user):
        return pwd.getpwnam(user).pw_uid == os.getuid()
    
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
                raise errors.BackupEnvironmentError("Creation of backup directory "
                                                    " (%s) failed."
                                                    % self._conf['backup_path'])
            logger.debug("Backup directory (%s) created.", 
                         self._conf['backup_path'])
    
    def _runMethod(self, method, conf):   
        if backupPluginRegistry.hasMethod(method):
            backupPluginRegistry.runMethod(method, conf)
        else:
            raise errors.BackupConfigError("Invalid backup method. "
                                           "Backup method %s not registered." 
                                           % method)
    def run(self):
        plugin = self._conf.get('plugin')
        if plugin is not None:
            self._loadPlugin(plugin)
        logger.debug("Backup plugin loaded: %s" % plugin)
        user = self._conf.get('user') or None
        if user is not None and not self._checkUser(user):
            raise errors.BackupEnvironmentError("Backup job must be run as user %s." 
                                                % user)
        self._initBackupDir(self._conf['backup_path'])
        method = self._conf.get('method')
        if method is not None:
            self._runMethod(method, self._conf)
        else:
            raise errors.BackupConfigError("Backup method not defined.")

    
def main(argv=None):
    (cmdopts, args) = parseCmdline(argv)
    if cmdopts.debug or cmdopts.verbose:
        logger.setLevel(logging.DEBUG)
    trace_errors = cmdopts.debug
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
    if len(args) == 0:
        logger.warning("No backup job selected for execution.")
        return 1
    initGlobals(global_conf)
    for job_name in args:
        logContext.setJob(job_name)
        try:
            job_conf = jobs_conf.get(job_name)
            if job_conf:
                logger.info("Starting execution of backup job")
                job = BackupJob(job_name, global_conf, job_conf)
                job.run()
                logger.info("Finished execution of backup job")
            else:
                logger.error("No configuration found for backup job.")
        except errors.BackupError, e:
            fatal = False
            if isinstance(e, errors.BackupFatalConfigError):
                logger.critical("Fatal configuration error in job.")
                fatal = True
            elif isinstance(e, errors.BackupConfigError):
                logger.error("Configuration error in backup job")
                fatal = False
            elif isinstance(e, errors.BackupEnvironmentError):
                logger.error("Error in backup job environment.")
                fatal = False
            else:     
                logger.error("Error in execution of backup job.")
                fatal = False
            for line in e:
                logger.error("  %s" , line)
            if trace_errors:
                raise
            elif fatal:
                return 1
                

if __name__ == "__main__":
    sys.exit(main())