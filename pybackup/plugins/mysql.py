"""pybackup - Backup Plugin for MySQL Database

"""

import os
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger
from pybackup.plugins import BackupPluginBase, backupPluginRegistry
from pysysinfo.mysql import MySQLinfo


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"



class PluginMySQL(BackupPluginBase):
    
    _optList = ('cmd_mysqldump',
                'filename_dump_db_prefix',
                'db_host', 'db_port', 'db_user', 'db_password',
                'db_list',)
    _reqOptList = ()
    _defaults = {'job_name': 'MySQL Backup',
                 'cmd_mysqldump': 'mysqldump',
                 'filename_dump_db_prefix': 'mysql_dump',}
    
    def __init__(self, **kwargs):
        BackupPluginBase.__init__(self, **kwargs)
        self._connArgs = []
        for (opt, key) in (('-h', 'db_host'),
                           ('-P', 'db_port'),
                           ('-u', 'db_user')):
            val = self._conf.get(key) 
            if val is not None:
                self._connArgs.extend([opt, val])
        self._env = os.environ.copy()
        db_password = self._conf.get('db_password')
        if db_password is not None:
            self._env['MYSQL_PWD'] = db_password
    
    def dumpDatabase(self, db, data=True):
        if data:
            dump_type = 'data'
            dump_desc = 'MySQL Database Contents'
        else:
            dump_type = 'db'
            dump_desc = 'MySQL Database Container'
        dump_filename = "%s_%s_%s.dump.%s" % (self._conf['filename_dump_db_prefix'], 
                                        db, dump_type,
                                        self._conf['suffix_compress'])
        dump_path = os.path.join(self._conf['backup_path'], dump_filename)
        args = [self._conf['cmd_mysqldump'],]
        args.extend(self._connArgs)
        if db == 'information_schema':
            args.append('--skip-lock-tables')
        if not data:
            args.extend(['--no-create-info', '--no-data' ,'--databases'])
        args.append(db)
        logger.info("Starting dump of %s: %s"
                    "  Backup: %s", dump_desc, db, dump_path)
        returncode, out, err = self._execBackupCmd(args, #@UnusedVariable
                                                   dump_path,
                                                   True)
        if returncode == 0:
            logger.info("Finished dump of %s: %s"
                        "  Backup: %s", dump_desc, db, dump_path)
        else:
            raise errors.BackupError("Dump of %s for %s failed "
                                     "with error code %s." 
                                     % (dump_desc, db, returncode),
                                     *utils.split_msg(err))    
    
    def dumpDatabases(self):
        if self._conf['db_list'] is None:
            try:
                my = MySQLinfo(host=self._conf.get('db_host'),
                               port=self._conf.get('db_port'),
                               user=self._conf.get('db_user'),
                               password=self._conf.get('db_password'))
                self._conf['db_list'] = my.getDatabases() 
                del my
            except Exception, e:
                raise errors.BackupError("Connection to MySQL Server "
                                         "for querying database list failed.",
                                         "Error Message: %s" % str(e))
        logger.info("Starting dump of %d MySQL Databases.",
                    len(self._conf['db_list']))
        for db in self._conf['db_list']:
            self.dumpDatabase(db, False)
            self.dumpDatabase(db, True)
        logger.info("Finished dump of MySQL Databases.")

    def dumpFull(self):
        self.dumpDatabases()
        
    
backupPluginRegistry.register('mysql_dump_full', 'dumpFull', PluginMySQL)
backupPluginRegistry.register('mysql_dump_databases', 'dumpDatabases', PluginMySQL)