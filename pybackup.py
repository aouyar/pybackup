#!/usr/bin/env python

import sys
import os
import platform
import pwd
import ConfigParser
import optparse
import logging
from datetime import date
import subprocess
from pysysinfo.util import parse_value
from pysysinfo.postgresql import PgInfo


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


# Defaults
defaultConfPaths = ['./pybackup.conf', '/etc/pybackup.conf']
logLevel = logging.INFO

# Logging Configuration
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class BackupError(Exception):
    pass

class BackupConfigError(BackupError):
    pass

class BackupFatalConfigError(BackupConfigError):
    pass

class BackupEnvironmentError(BackupError):
    pass



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
        raise BackupFatalConfigError("Configuration file not found in any of " 
                                     "the following locations: %s" 
                                      % ' '.join(conf_paths))
    if not confmgr.has_section('general'):
        raise BackupFatalConfigError("Missing mandatory section 'general' "
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


class BackupJob():
    
    def __init__(self, name, global_conf, job_conf):
        self._name = name
        self._conf = {}
        self._conf.update(global_conf)
        self._conf.update(job_conf)
        if self._conf.has_key('backup_root'):
            backup_path_elem = [self._conf['backup_root'], ]
            if (self._conf.has_key('hostname_dir') 
                and parse_value(self._conf['hostname_dir'])):
                backup_path_elem.append(platform.node())
            backup_path_elem.append(date.today().strftime('%Y-%m-%d'))
            backup_path_elem.append(name)
            self._conf['backup_path'] = os.path.join(*backup_path_elem)
        else:
            raise BackupFatalConfigError("Backup root directory (backup_root) "
                                         "not defined.")

    def _checkUser(self, user):
        return pwd.getpwnam(user).pw_uid == os.getuid()
    
    def _initBackupDir(self, path):
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except:
                raise BackupEnvironmentError("Creation of backup directory (%s) failed."
                                         % self._conf['backup_path'])
            logger.debug("Backup directory (%s) created.", 
                         self._conf['backup_path'])
    
    def _runMethod(self, method, conf):   
        func = backupMethodRegistry.get(method)
        if func is not None:
            func(conf)
        else:
            raise BackupConfigError("Invalid backup method. "
                                    "Backup method %s not registered." % method)
    def run(self):
        user = self._conf.get('user') or None
        if user is not None and not self._checkUser(user):
            raise BackupEnvironmentError("Backup job must be run as user %s." 
                                         % user)
        self._initBackupDir(self._conf['backup_path'])
        method = self._conf.get('method')
        if method is not None:
            self._runMethod(method, self._conf)
        else:
            raise BackupConfigError("Backup method not defined.")
        


class BackupMethodRegistry():
    
    def __init__(self):
        self._methodDict = {}
        
    def register(self, name, method):
        self._methodDict[name] = method
        
    def getList(self):
        return self._methodDict.keys()
    
    def get(self, method):
        return self._methodDict.get(method)
        
backupMethodRegistry = BackupMethodRegistry()



defaultCmdGzip = 'gzip'
defaultPgCmdDump = 'pg_dump'
defaultPgCmdDumpAll = 'pg_dumpall'
defaultPgFilenameDumpGlobals = 'pg_dump_globals.gz'
defaultPgFilenameDumpDB = 'pg_dump_db_%s'

defaultBufferSize=4096

class PgDumper():
    
    def __init__(self, **kwargs):
        self._cmdBackupPath = kwargs.get('backup_path')
        if self._cmdBackupPath is None:
            raise BackupEnvironmentError("Backup directory not defined.")
        self._cmdDump = (kwargs.get('cmd_pg_dump') 
                         or defaultPgCmdDump)
        self._cmdDumpAll = (kwargs.get('cmd_pg_dumpall') 
                            or defaultPgCmdDumpAll)
        self._cmdGzip = (kwargs.get('cmd_gzip') 
                            or defaultCmdGzip)
        self._fileDumpGlobals = (kwargs.get('filename_dump_globals') 
                                 or defaultPgFilenameDumpGlobals)
        self._fileDumpDB = (kwargs.get('filename_dump_db') 
                            or defaultPgFilenameDumpDB)
        self._dblist = (kwargs.get('db_list') or None)
    
    def pgDumpGlobals(self):
        dump_path = os.path.join(self._cmdBackupPath, self._fileDumpGlobals)
        args1 = [self._cmdDumpAll, '-g']
        args2 = [self._cmdGzip, '-c']
        logger.info("PgDumper: Start PostgreSQL Global Objects dump. "
                    "Backup: %s", dump_path)
        try:
            cmd1 = subprocess.Popen(args1, 
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, 
                                    bufsize=defaultBufferSize)
        except Exception, e:
            raise BackupError("PgDumper: Database dump command failed.",
                              "Command: %s" % ' '.join(args1),
                              "Error Message: %s" % str(e))
        try:
            outfile = os.open(dump_path, os.O_WRONLY | os.O_CREAT)
        except Exception, e:
            raise BackupError("PgDumper: Failed creation of backup file.",
                             "Error Message: %s" % str(e))
        try:
            cmd2 = subprocess.Popen(args2,
                                    stdin=cmd1.stdout,
                                    stdout=outfile,
                                    stderr=subprocess.PIPE,  
                                    bufsize=defaultBufferSize)
            cmd1.stdout.close()
        except Exception, e:
            raise BackupError("PgDumper: Backup compression command failed.",
                              "Command: %s" % ' '.join(args2),
                              "Error Message: %s" % str(e))
        errdata2 = cmd2.communicate(None)[1]
        errdata1 = cmd1.stderr.read()
        cmd1.wait()
        if cmd1.returncode == 0 and cmd2.returncode == 0:
            logger.info("PgDumper: Finished PostgreSQL Global Objects dump. "
                        "Backup: %s", dump_path)
        elif cmd1.returncode != 0:
            raise BackupError("PgDumper: Dump failed with error code %s." 
                              % cmd1.returncode,
                              *errdata1.splitlines())
        elif cmd2.returncode != 0:
            raise BackupError("PgDumper: Compression of dump failed "
                              "with error code %s." % cmd2.returncode,
                              *errdata2.splitlines())

    def pgDumpDatabase(self, db):
        pass
    
    def pgDumpAllDatabases(self):
        pass

    def pgDumpFull(self):
        pass

    
def pgDumpFull(job_conf):
    pg = PgDumper(**job_conf)
    pg.pgDumpGlobals()
    
backupMethodRegistry.register('pg_dump_full', pgDumpFull)


    
def main(argv=None):
    (cmdopts, args) = parseCmdline(argv)
    if cmdopts.debug or cmdopts.verbose:
        logger.setLevel(logging.DEBUG)
    trace_errors = cmdopts.debug
    if cmdopts.confPath is not None:
        conf_path = cmdopts.conf_path
    else:
        conf_path = defaultConfPaths
    try:
        (global_conf, jobs_conf) = parseConfFile(conf_path)
    except BackupConfigError, msg:
        logger.critical(msg)
        if trace_errors:
            raise
        else:
            return 1
    if len(args) == 0:
        logger.warning("No backup job selected for execution.")
        return 1
    for job_name in args:
        try:
            job_conf = jobs_conf.get(job_name)
            if job_conf:
                logger.info("Start executing backup job: %s", job_name)
                job = BackupJob(job_name, global_conf, job_conf)
                job.run()
                logger.info("Finished executing backup job: %s", job_name)
            else:
                logger.error("No configuration found for backup job %s." % 
                             job_name)
        except BackupError, e:
            fatal = False
            if isinstance(e, BackupFatalConfigError):
                logger.critical("Fatal configuration error. %s", job_name)
                fatal = True
            elif isinstance(e, BackupConfigError):
                logger.error("Configuration error in backup job %s. %s", 
                             job_name)
                fatal = False
            elif isinstance(e, BackupEnvironmentError):
                logger.error("Error in backup job %s environment.", job_name)
                fatal = False
            else:     
                logger.error("Error in execution of backup job %s.", job_name)
                fatal = False
            for line in e:
                logger.error("  %s" , line)
            if trace_errors:
                raise
            elif fatal:
                return 1
                

if __name__ == "__main__":
    sys.exit(main())