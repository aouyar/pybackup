"""pybackup - Backup Plugin for PostgreSQL Database

"""

import os
from pybackup import errors
from pybackup import utils
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
    
    _optList = ('cmd_pg_dump', 'cmd_pg_dumpall', 
                'filename_dump_globals', 'filename_dump_db_prefix',
                'db_host', 'db_port', 'db_database', 'db_user', 'db_password',
                'db_list',)
    _reqOptList = ()
    _defaults = { 'job_name': 'PostgreSQL Backup',
               'cmd_pg_dump': 'pg_dump',
               'cmd_pg_dumpall': 'pg_dumpall',
               'filename_dump_globals': 'pg_dump_globals',
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
            
    def dumpGlobals(self):
        dump_path = os.path.join(self._conf['backup_path'],
                                 "%s.%s" % (self._conf['filename_dump_globals'],
                                            self._conf['suffix_compress']))
        args = [self._conf['cmd_pg_dumpall'], '-w', '-g']
        args.extend(self._connArgs)
        logger.info("Starting PostgreSQL Global Objects dump."
                    "  Backup: %s", dump_path)
        returncode, out, err = self._execBackupCmd(args, dump_path, True) #@UnusedVariable
        if returncode == 0:
            logger.info("Finished PostgreSQL Global Objects dump."
                        "  Backup: %s", dump_path)
        else:
            raise errors.BackupError("Dump failed with error code %s." 
                                     % returncode,
                                     *utils.split_msg(err))
        
    def dumpDatabase(self, db):
        dump_filename = "%s_%s.dump" % (self._conf['filename_dump_db_prefix'], 
                                        db)
        dump_path = os.path.join(self._conf['backup_path'], dump_filename)
        args = [self._conf['cmd_pg_dump'], '-w', '-Fc']
        args.extend(self._connArgs)
        args.extend(['-f', dump_path, db])
        logger.info("Starting dump of PostgreSQL Database: %s"
                    "  Backup: %s", db, dump_path)
        returncode, out, err = self._execBackupCmd(args) #@UnusedVariable
        if returncode == 0:
            logger.info("Finished dump of PostgreSQL Database: %s"
                        "  Backup: %s", db, dump_path)
        else:
            raise errors.BackupError("Dump of PostgreSQL database %s failed "
                                     "with error code %s." % (db, returncode),
                                     *utils.split_msg(err))
    
    def dumpDatabases(self):
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
            self.dumpDatabase(db)
        logger.info("Finished dump of PostgreSQL Databases.")

    def dumpFull(self):
        self.dumpGlobals()
        self.dumpDatabases()
        
    
backupPluginRegistry.register('pg_dump_full', 'dumpFull', 
                              PluginPostgreSQL)
backupPluginRegistry.register('pg_dump_globals', 'dumpGlobals', 
                              PluginPostgreSQL)
backupPluginRegistry.register('pg_dump_databases', 'dumpDatabases', 
                              PluginPostgreSQL)
