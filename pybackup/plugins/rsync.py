"""pybackup - Backup Plugin for generation of backups using rsync.

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



class PluginRsync(BackupPluginBase):
    
    _extOpts = {'remote_host': 'The remote host for backup.'
                               ' (Local host by default)',
                'remote_user': 'User for connecting to remote host.'
                               ' (Same as user executing backup by default.)', 
                'path_list': 'List of paths to be included in the backup.', 
                'base_dir': 'Base directory for list of paths to be included '
                            ' the backup. (Absolute paths are used by default.)', 
                'backup_index': 'Enable / disable index file. (Default: no)',
                'compress': 'Enable / disable compression of file data for'
                            'network transfer. (Enabled by default.)',
                'delete': 'Enable / disable deletion of files from destination'
                          ' path. (Disabled by default.)',
                'exclude_patterns': 'List of filename patterns to exclude from '
                                    'the backup.', 
                'exclude_patterns_file': 'Path for file that stores list of '
                                        'filename patterns to exclude from '
                                        'the backup.',
                'index_filename': 'Filename for index of synchronized files.',}
    _extReqOptList = ('path_list',)
    _extDefaults = {'cmd_rsync': 'rsync',
                    'filename_index': 'rsync', 
                    'suffix_index': 'list',}
    
    def __init__(self, global_conf, job_conf):
        BackupPluginBase.__init__(self, global_conf, job_conf)
        
    def syncDirs(self):
        index_filename = "%s.%s" % (self._conf['filename_index'], 
                                    self._conf['suffix_index'])
        remote_host = self._conf.get('remote_host')
        remote_user = self._conf.get('remote_host')
        if remote_host is not None:
            archive_path = os.path.join(self._conf['job_path'], remote_host)
            if remote_user is not None:
                remote = '%s@%s' % (remote_host, remote_user)
            else:
                remote = remote_host
        else:
            archive_path = os.path.join(self._conf['job_path'], 'localhost')
            remote = None
        index_path = os.path.join(self._conf['job_path'], index_filename)
        base_dir = self._conf.get('base_dir')
        path_list = [os.path.normpath(path) 
                     for path in re.split('\s*,\s*|\s+', self._conf['path_list'])]
        compress = parse_value(self._conf.get('compress'), True)
        delete = parse_value(self._conf.get('delete'), False)
        backup_index = parse_value(self._conf.get('backup_index'), True)
        if self._conf.has_key('exclude_patterns'):
            exclude_patterns = re.split('\s*,\s*|\s+', 
                                        self._conf['exclude_patterns'])
        else:
            exclude_patterns = None
        exclude_patterns_file = self._conf.get('exclude_patterns_file')
        logger.info("Starting backup of paths: %s", ', '.join(path_list))
        args = [self._conf['cmd_rsync'],]
        if self._dryRun:
            args.append('-n')
        args.append('-aR')
        if compress:
            args.append('-z')
        if backup_index:
            args.append('-v')
            args.append('--stats')
        if delete:
            args.append('--delete')
        if exclude_patterns is not None:
            for pattern in exclude_patterns:
                args.append("--exclude=%s" % pattern)
        if exclude_patterns_file is not None:
            if os.path.isfile(exclude_patterns_file):
                args.append("--exclude-from=%s" % exclude_patterns_file)
            else:
                raise errors.BackupConfigError("Invalid exclude patterns file: %s"
                                               % exclude_patterns_file)
        first = True
        for path in path_list:
            if base_dir is not None:
                src_path = os.path.join(base_dir, '.' ,path)
            else:
                src_path = path
            if remote is not None:
                if first:
                    args.append('%s:%s' % (remote, src_path))
                else:
                    args.append(':%s' % (src_path,))
            else:
                args.append(src_path)
        args.append(archive_path)         
        if backup_index:
            returncode, out, err = self._execBackupCmd(args, index_path, force_exec=True) #@UnusedVariable
        else:
            returncode, out, err = self._execBackupCmd(args, force_exec=True) #@UnusedVariable
        if returncode == 0:
            logger.info("Finished backup of paths: %s", ', '.join(path_list))
        else:
            raise errors.BackupError("Backup of paths failed with error code: %s." 
                                     % returncode,
                                     *utils.split_msg(err))
        
        
            
backupPluginRegistry.register('rsync', PluginRsync, 'syncDirs')

