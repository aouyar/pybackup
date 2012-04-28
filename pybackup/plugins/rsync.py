"""pybackup - Backup Plugin for generation of backups using rsync.

"""

import os
import re
from pybackup import errors
from pybackup import utils
from pybackup.logmgr import logger
from pybackup.plugins import BackupPluginBase
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
                'dst_dir': 'Destination directory for files. '
                            ' (The job directory is used by default.)', 
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
        self._index_filename = "%s.%s" % (self._conf['filename_index'], 
                                          self._conf['suffix_index'])
        self._index_path = os.path.join(self._conf['job_path'], self._index_filename)
        self._remote_host = self._conf.get('remote_host')
        self._remote_user = self._conf.get('remote_user')
        if self._remote_host is not None:
            if self._remote_user is not None:
                self._remote = '%s@%s' % (self._remote_user, self._remote_host)
            else:
                self._remote = self._remote_host
        else:
            self._remote = None

    def _initSrc(self):
        base_dir = self._conf.get('base_dir')
        self._path_list = [os.path.normpath(path) 
                           for path in re.split('\s*,\s*|\s+', 
                                                self._conf['path_list'])]
        self._src_list = []
        first = True
        for path in self._path_list:
            if base_dir is not None:
                src_path = os.path.join(base_dir, '.' ,path)
            else:
                src_path = path
            if self._remote is not None:
                if first:
                    self._src_list.append('%s:%s' % (self._remote, src_path))
                    first = False
                else:
                    self._src_list.append(':%s' % (src_path,))
            else:
                self._src_list.append(src_path)

    def _initDest(self):
        dst_dir = self._conf.get('dst_dir') 
        if dst_dir is not None:
            if os.path.isdir(dst_dir):
                self._archive_path = os.path.normpath(dst_dir)
            else:
                raise errors.BackupConfigError("Destination directory (dst_dir: %s)"
                                               " does not exist." % dst_dir)
        else:
            if self._remote_host is not None:
                self._archive_path = os.path.join(self._conf['job_path'], 
                                                  self._remote_host)
            else:
                self._archive_path = os.path.join(self._conf['job_path'], 
                                                  'localhost')
                
    def syncDirs(self):
        self._initSrc()
        self._initDest()
        compress = parse_value(self._conf.get('compress'), True)
        delete = parse_value(self._conf.get('delete'), False)
        backup_index = parse_value(self._conf.get('backup_index'), True)
        if self._conf.has_key('exclude_patterns'):
            exclude_patterns = re.split('\s*,\s*|\s+', 
                                        self._conf['exclude_patterns'])
        else:
            exclude_patterns = None
        exclude_patterns_file = self._conf.get('exclude_patterns_file')
        logger.info("Starting backup of paths: %s", ', '.join(self._path_list))
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
        if len(self._src_list) > 0:
            args.extend(self._src_list)
        else:
            raise errors.BackupConfigError("No valid source paths defined for backup.")
        args.append(self._archive_path)         
        if backup_index:
            returncode, out, err = self._execBackupCmd(args, #@UnusedVariable
                                                       out_path=self._index_path, 
                                                       force_exec=True) 
        else:
            returncode, out, err = self._execBackupCmd(args, force_exec=True) #@UnusedVariable
        if returncode == 0:
            logger.info("Finished backup of paths: %s", ', '.join(self._path_list))
        else:
            raise errors.BackupError("Backup of paths failed with error code: %s." 
                                     % returncode,
                                     *utils.split_msg(err))

class PluginBackupSync(PluginRsync):
    
    _extOpts = {'remote_host': 'The remote host for backup.'
                               ' (Local host by default)',
                'remote_user': 'User for connecting to remote host.'
                               ' (Same as user executing backup by default.)',
                'remote_backup_root': 'Backup root directory of remote host.'
                                      ' (Same as local backup_root by default.)',
                'dst_dir': 'Destination directory for files. '
                            ' (Backup root directory is used by default.)', 
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
    _extReqOptList = ('remote_host',)
    _extDefaults = {'cmd_rsync': 'rsync',
                    'filename_index': 'rsync', 
                    'suffix_index': 'list',}
    
    def __init__(self, global_conf, job_conf):
        PluginRsync.__init__(self, global_conf, job_conf)
        if not self._conf.has_key('dst_dir'):
            self._conf['dst_dir'] = self._conf['backup_root']
        if not self._conf.has_key('remote_backup_root'):
            self._conf['remote_backup_root'] = self._conf['backup_root']
    
    def _initSrc(self):
        self._path_list = [self._conf['remote_backup_root'],]
        self._src_list = []
        src_path = os.path.join(self._conf['remote_backup_root'], '.' , '*')
        if self._remote is not None:
            self._src_list.append('%s:%s' % (self._remote, src_path))
        else:
            self._src_list.append(src_path)
    

description = "Plugin for backups using rsync."        
methodList = (('rsync_dirs', PluginRsync, 'syncDirs'),
              ('rsync_backupdir', PluginBackupSync, 'syncDirs'),)
