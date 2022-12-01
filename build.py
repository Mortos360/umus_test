#!/usr/bin/env python

import os
import shutil
import subprocess
import sys


source = os.environ['REZ_BUILD_SOURCE_PATH']
install = os.environ['REZ_BUILD_INSTALL_PATH']
exclude = ['build.py', 'package.py', '.gitignore', 'test']


if 'install' in (sys.argv[1:] or []):

    files = subprocess.check_output(('git', '-C', source, 'ls-files')).decode('utf-8').strip().split('\n')

    if os.path.exists(install):
        shutil.rmtree(install)

    for file in (x for x in files if not any(x.startswith(y) for y in exclude)):
        sourceFile = source + '/' + file
        destFile = install + '/' + file

        folder = os.path.dirname(destFile)
        if not os.path.exists(folder):
            os.makedirs(folder)

        print(destFile)
        shutil.copy(sourceFile, destFile)


