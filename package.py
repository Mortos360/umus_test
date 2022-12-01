name = 'umus_ftp'

version = '0.1.2'

requires = [
]

build_command = 'python {root}/build.py {install}'

def commands():
    env.PYTHONPATH.append('{root}')
