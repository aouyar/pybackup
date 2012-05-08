"""pybackup - Generic utility functions.

"""

import os
import pwd

__author__ = "Ali Onur Uyar"
__copyright__ = "Copyright 2011, Ali Onur Uyar"
__credits__ = []
__license__ = "GPL"
__version__ = "0.5"
__maintainer__ = "Ali Onur Uyar"
__email__ = "aouyar at gmail.com"
__status__ = "Development"



def splitMsg(msg):
    """Returns the list of lines in msg breaking at line boundaries and skipping
    empty lines.  
    
    @param msg: Multi-line text.
    @return:    List of lines.
    
    """
    return [line for line in msg.splitlines() if len(line.strip()) > 0]

def checkUser(user):
    """Check of current user matches login user passed to function.
    
    @param user: User login.
    
    """
    return pwd.getpwnam(user).pw_uid == os.getuid()
