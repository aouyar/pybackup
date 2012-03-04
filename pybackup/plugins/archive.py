"""pybackup - Backup Plugin for Generation of Archive Files

"""

import os
import re
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger
from pybackup.plugins import BackupPluginBase, backupPluginRegistry
from pysysinfo.util import parse_value


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"



class PluginArchive(BackupPluginBase):
    
    _optList = ('cmd_tar', 'filename_archive', 'path_list', 'base_dir', 
                'backup_index', 'suffix_list',
                'exclude_patterns', 'exclude_patterns_file')
    _reqOptList = ('filename_archive', 'path_list')
    _defaults = {'job_name': 'Archive Backup',
                 'backup_index': 'yes', 'suffix_list': 'list'}
    
    def __init__(self, **kwargs):
        BackupPluginBase.__init__(self, **kwargs)
        
    def _checkSrcPaths(self, path_list):
        for path in path_list:
            base_dir = self._conf.get('base_dir') or '/'
            if not os.path.exists(os.path.join(base_dir, path)):
                raise errors.BackupConfigError("Invalid source path: %s" % path)
        
    def backupDirs(self):
        archive_filename = "%s.%s" % (self._conf['filename_archive'], 
                                      self._conf['suffix_tgz'])
        index_filename = "%s.%s" % (self._conf['filename_archive'], 
                                      self._conf['suffix_list'])
        archive_path = os.path.join(self._conf['job_path'], archive_filename)
        index_path = os.path.join(self._conf['job_path'], index_filename)
        path_list = re.split('\s*,\s*|\s+', self._conf['path_list'])
        if self._conf['exclude_patterns'] is not None:
            exclude_patterns = re.split('\s*,\s*|\s+', 
                                        self._conf['exclude_patterns'])
        else:
            exclude_patterns = None
        logger.info("Starting backup of paths: %s", ', '.join(path_list))
        args = [self._conf['cmd_tar'],]
        base_dir = self._conf['base_dir']
        if base_dir is not None:
            if os.path.isdir(base_dir):
                args.extend(['-C', base_dir])
            else:
                raise errors.BackupConfigError("Invalid base directory "
                                               "(base_dir): %s"% base_dir)
        backup_index = parse_value(self._conf['backup_index'], True)
        if backup_index:
            args.append('-v')
        if exclude_patterns is not None:
            for pattern in exclude_patterns:
                args.append("--exclude=%s" % pattern)
        exclude_patterns_file = self._conf['exclude_patterns_file']
        if exclude_patterns_file is not None:
            if os.path.isfile(exclude_patterns_file):
                args.append("--exclude-from=%s" % exclude_patterns_file)
            else:
                raise errors.BackupConfigError("Invalid exclude patterns file: %s"
                                               % exclude_patterns_file)
        args.extend(['-zcf', archive_path])
        self._checkSrcPaths(path_list)
        args.extend(path_list)
        if backup_index:
            returncode, out, err = self._execBackupCmd(args, index_path) #@UnusedVariable
        else:
            returncode, out, err = self._execBackupCmd(args) #@UnusedVariable
        if returncode == 0:
            logger.info("Finished backup of paths: %s", ', '.join(path_list))
        else:
            raise errors.BackupError("Backup of paths failed with error code: %s." 
                                     % returncode,
                                     *utils.split_msg(err))
        
        
            
backupPluginRegistry.register('archive', 'backupDirs', PluginArchive)

