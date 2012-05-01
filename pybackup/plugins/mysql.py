"""pybackup - Backup Plugin for MySQL Database

"""

import os
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger
from pybackup.plugins import BackupPluginBase
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
    
    _extOpts = {'filename_dump_db': 'Filename for MySQL dump files.',
                'db_host': 'MySQL Database Server Name or IP.', 
                'db_port': 'MySQL Database Server Port.', 
                'db_user': 'MySQL Database Server User.', 
                'db_password': 'MySQL Database Server Password.',
                'db_list': 'List of databases. (All databases by default.)',}
    _extReqOptList = ()
    _extDefaults = {'cmd_mysqldump': 'mysqldump',
                    'filename_dump_db': 'mysql_dump',}
    
    def __init__(self, global_conf, job_conf):
        BackupPluginBase.__init__(self, global_conf, job_conf)
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
        dump_filename = "%s_%s_%s.dump.%s" % (self._conf['filename_dump_db'], 
                                        db, dump_type,
                                        self._conf['suffix_compress'])
        dump_path = os.path.join(self._conf['job_path'], dump_filename)
        args = [self._conf['cmd_mysqldump'],]
        args.extend(self._connArgs)
        if db in ('information_schema', 'mysql'):
            args.append('--skip-lock-tables')
        if not data:
            args.extend(['--no-create-info', '--no-data' ,'--databases'])
        args.append(db)
        logger.info("Starting dump of %s: %s"
                    "  Backup: %s", dump_desc, db, dump_path)
        returncode, out, err = self._execBackupCmd(args, #@UnusedVariable
                                                   self._env,
                                                   out_path=dump_path,
                                                   out_compress=True)
        if returncode == 0:
            logger.info("Finished dump of %s: %s"
                        "  Backup: %s", dump_desc, db, dump_path)
        else:
            raise errors.BackupError("Dump of %s for %s failed "
                                     "with error code: %s" 
                                     % (dump_desc, db, returncode),
                                     *utils.splitMsg(err))    
    
    def dumpDatabases(self):
        if not self._conf.has_key('db_list'):
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
        

description = "Plugin for backups of MySQL Database."    
methodList = (('mysql_dump_full', PluginMySQL, 'dumpFull'),
              ('mysql_dump_databases', PluginMySQL, 'dumpDatabases'),)
