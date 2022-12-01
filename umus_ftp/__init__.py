import arxcfg

CFG = arxcfg.get_cfg('ftp')
GLOBALS = arxcfg.get_cfg('globals')

from . import core

ftp = core
