"""pybackup - Backup Plugin for PostgreSQL Database

"""

import os
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger
from pybackup.plugins import BackupPluginBase
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
    
    _extOpts = {'filename_dump_globals': '', 
                'filename_dump_db': '',
                'db_host': 'PostgresSQL Database Server Name or IP.', 
                'db_port': 'Postgres Database Server Port.', 
                'db_user': 'Postgres Database Server User.', 
                'db_password': 'Postgres Database Server Password.',
                'db_database': 'Postgres Database for initial connection.',
                'db_list': 'List of databases. (All databases by default.)',}
    _extReqOptList = ()
    _extDefaults = {'cmd_pg_dump': 'pg_dump','cmd_pg_dumpall': 'pg_dumpall',
                    'filename_dump_globals': 'pg_dump_globals',
                    'filename_dump_db': 'pg_dump_db',}
    
    def __init__(self, global_conf, job_conf):
        BackupPluginBase.__init__(self, global_conf, job_conf)
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
            
    def dumpGlobals(self):
        dump_path = os.path.join(self._conf['job_path'],
                                 "%s.%s" % (self._conf['filename_dump_globals'],
                                            self._conf['suffix_compress']))
        args = [self._conf['cmd_pg_dumpall'], '-w', '-g']
        args.extend(self._connArgs)
        logger.info("Starting PostgreSQL Global Objects dump."
                    "  Backup: %s", dump_path)
        returncode, out, err = self._execBackupCmd(args, #@UnusedVariable
                                                   self._env,
                                                   out_path=dump_path, 
                                                   out_compress=True) 
        if returncode == 0:
            logger.info("Finished PostgreSQL Global Objects dump."
                        "  Backup: %s", dump_path)
        else:
            raise errors.BackupError("Dump failed with error code: %s" 
                                     % returncode,
                                     *utils.splitMsg(err))
        
    def dumpDatabase(self, db):
        dump_filename = "%s_%s.dump" % (self._conf['filename_dump_db'], 
                                        db)
        dump_path = os.path.join(self._conf['job_path'], dump_filename)
        args = [self._conf['cmd_pg_dump'], '-w', '-Fc']
        args.extend(self._connArgs)
        args.extend(['-f', dump_path, db])
        logger.info("Starting dump of PostgreSQL Database: %s"
                    "  Backup: %s", db, dump_path)
        returncode, out, err = self._execBackupCmd(args, self._env) #@UnusedVariable
        if returncode == 0:
            logger.info("Finished dump of PostgreSQL Database: %s"
                        "  Backup: %s", db, dump_path)
        else:
            raise errors.BackupError("Dump of PostgreSQL database %s failed "
                                     "with error code %s." % (db, returncode),
                                     *utils.splitMsg(err))
    
    def dumpDatabases(self):
        if not self._conf.has_key('db_list'):
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
            self.dumpDatabase(db)
        logger.info("Finished dump of PostgreSQL Databases.")

    def dumpFull(self):
        self.dumpGlobals()
        self.dumpDatabases()
        
description = "Plugin for backups of PostgreSQL Database." 
methodList = (('pg_dump_full', PluginPostgreSQL, 'dumpFull'),
              ('pg_dump_globals', PluginPostgreSQL, 'dumpGlobals'),
              ('pg_dump_databases', PluginPostgreSQL, 'dumpDatabases'),)