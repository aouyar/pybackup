"""pybackup - Backup Plugin for PostgreSQL Database

"""

import os
import subprocess
from pybackup import defaults
from pybackup import errors
from pybackup.logmgr import logger
from pybackup.plugins import BackupPluginBase, backupPluginRegistry
from pysysinfo.postgresql import PgInfo


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"



class PluginPostgreSQL(BackupPluginBase):
    
    _optList = ('job_name', 'backup_path',
                'cmd_pg_dump', 'cmd_pg_dumpall', 'cmd_gzip',
                'filename_dump_globals', 'filename_dump_db_prefix',
                'db_host', 'db_port', 'db_database', 'db_user', 'db_password',
                'db_list',)
    _requiredOptList = ('backup_path',)
    _defaults = { 'job_name': 'PostgreSQL Backup',
               'cmd_pg_dump': 'pg_dump',
               'cmd_pg_dumpall': 'pg_dumpall',
               'filename_dump_globals': 'pg_dump_globals.gz',
               'filename_dump_db_prefix': 'pg_dump_db',}
    
    def __init__(self, **kwargs):
        BackupPluginBase.__init__(self, **kwargs)
        self._connArgs = []
        for (opt, key) in (('-h', 'db_host'),
                           ('-p', 'db_port'),
                           ('-l', 'db_database'),
                           ('-U', 'db_user')):
            val = self._conf.get(key) 
            if val is not None:
                self._connArgs.extend([opt, val])
        self._env = os.environ.copy()
        db_password = self._conf.get('db_password')
        if db_password is not None:
            self._env['PGPASSWORD'] = db_password
        
    def pgDumpGlobals(self):
        dump_path = os.path.join(self._conf['backup_path'], 
                                 self._conf['filename_dump_globals'])
        args1 = [self._conf['cmd_pg_dumpall'], '-w', '-g']
        args1.extend(self._connArgs)
        args2 = [self._conf['cmd_gzip'], '-c']
        logger.info("Starting PostgreSQL Global Objects dump."
                    "  Backup: %s", dump_path)
        try:
            cmd1 = subprocess.Popen(args1, 
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, 
                                    bufsize=defaults.bufferSize,
                                    env = self._env)
        except Exception, e:
            raise errors.BackupError("Database dump command failed.", 
                                     "Command: %s" % ' '.join(args1),
                                     "Error Message: %s" % str(e))
        try:
            outfile = os.open(dump_path, os.O_WRONLY | os.O_CREAT)
        except Exception, e:
            raise errors.BackupError("Failed creation of backup file.",
                                     "Error Message: %s" % str(e))
        try:
            cmd2 = subprocess.Popen(args2,
                                    stdin=cmd1.stdout,
                                    stdout=outfile,
                                    stderr=subprocess.PIPE,  
                                    bufsize=defaults.bufferSize)
            cmd1.stdout.close()
        except Exception, e:
            raise errors.BackupError("Backup compression command failed.",
                                     "Command: %s" % ' '.join(args2),
                                     "Error Message: %s" % str(e))
        errdata2 = cmd2.communicate(None)[1]
        errdata1 = cmd1.stderr.read()
        cmd1.wait()
        if cmd1.returncode == 0 and cmd2.returncode == 0:
            logger.info("Finished PostgreSQL Global Objects dump."
                        "  Backup: %s", dump_path)
        elif cmd1.returncode != 0:
            raise errors.BackupError("Dump failed with error code %s." 
                                     % cmd1.returncode,
                                     *errdata1.splitlines())
        elif cmd2.returncode != 0:
            raise errors.BackupError("Compression of dump failed "
                                     "with error code %s." % cmd2.returncode,
                                     *errdata2.splitlines())

    def pgDumpDatabase(self, db):
        dump_filename = "%s_%s.dump" % (self._conf['filename_dump_db_prefix'], 
                                        db)
        dump_path = os.path.join(self._conf['backup_path'], dump_filename)
        args = [self._conf['cmd_pg_dump'], '-w', '-Fc']
        args.extend(self._connArgs)
        args.extend(['-f', dump_path, db])
        logger.info("Starting dump of PostgreSQL Database: %s"
                    "  Backup: %s", db, dump_path)
        try:
            cmd = subprocess.Popen(args, 
                                   stderr=subprocess.PIPE, 
                                   bufsize=defaults.bufferSize,
                                   env = self._env)
        except Exception, e:
            raise errors.BackupError("Database dump command failed.",
                                     "Command: %s" % ' '.join(args),
                                     "Error Message: %s" % str(e))
        errdata = cmd.communicate(None)[1]
        if cmd.returncode == 0:
            logger.info("Finished dump of PostgreSQL Database: %s"
                        "  Backup: %s", db, dump_path)
        else:
            raise errors.BackupError("Dump of database %s failed "
                                     "with error code %s." % (db, cmd.returncode),
                                     *errdata.splitlines())
    
    def pgDumpDatabases(self):
        if self._conf['db_list'] is None:
            try:
                pg = PgInfo(host=self._conf.get('db_host'),
                            port=self._conf.get('db_port'),
                            database=self._conf.get('db_database'),
                            user=self._conf.get('db_user'),
                            password=self._conf.get('db_password'))
                self._conf['db_list'] = pg.getDatabases()
                del pg
            except Exception, e:
                raise errors.BackupError("Connection to PostgreSQL Server "
                                         "for querying database list failed.",
                                         "Error Message: %s" % str(e))
        try:
            self._conf['db_list'].remove('template0')
        except ValueError:
            pass
        logger.info("Starting dump of %d PostgreSQL Databases.",
                    len(self._conf['db_list']))
        for db in self._conf['db_list']:
            self.pgDumpDatabase(db)
        logger.info("Finished dump of PostgreSQL Databases.")

    def pgDumpFull(self):
        self.pgDumpGlobals()
        self.pgDumpDatabases()
        

def pgDumpFull(job_conf):
    pg = PluginPostgreSQL(**job_conf)
    pg.pgDumpFull()
    
backupPluginRegistry.register('pg_dump_full', pgDumpFull)