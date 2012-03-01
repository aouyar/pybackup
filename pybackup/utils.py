"""pybackup - Global Defaults

"""


__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"


def split_msg(msg):
    return [line for line in msg.splitlines() if len(line.strip()) > 0]