import os
from configparser import ConfigParser

def read_config():
    config_dir = os.path.abspath ( os.path.dirname ( __file__ ) ) + '\\'
    filename = config_dir + 'Server.cfg'
    config = ConfigParser ()
    config.read ( filename )
    return config
