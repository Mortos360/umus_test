import os, logging, json, ftplib, re, inspect, time
from multiprocessing import Queue, Pool
from ftplib import FTP_TLS, FTP
from arxutils import crypt
from . import CFG
from pathlib import Path

LOG=logging.getLogger(__name__)

def get_ftp_from_config(server='main'):
    """create a ftp object from config

    Args:
        server (str, optional): name of server in config. Defaults to 'main'.

    Returns:
        FTP / FTP_TLS: ftp object
    """
    login = json.loads(crypt.decrypt(CFG['server'][server]))
    tls = login.get('tls')
    if tls == None:
        tls = True
    else:
        login.pop('tls')
    return get_ftp(tls=tls, **login)


def get_ftp(host, user, passwd, tls=True, **kwargs):
    """create a ftp object

    Args:
        host (str): host address for ftp
        user (str): username 
        passwd (str): password
        tls (bool, optional): True if TLS should be used. Defaults to True.

    Returns:
        FTP / FTP_TLS: ftp object
    """
    ftp = (FTP_TLS if tls else FTP)(host, user, passwd, **kwargs)
    if isinstance(ftp, FTP_TLS):
        ftp.prot_p()
    LOG.debug(ftp.getwelcome())
    return ftp  


def ftp_object(func):
    """decorator method to ensure ftp object is passed on

    Args:
        func (funciton): method to be decorated
    """
    def decorate(*args, **kwargs):
        argKeys = inspect.getfullargspec(func).args
        args = dict(zip(argKeys, args))
        args.update(kwargs)
        if not args.get('ftp'):
            if args.get('server'):
                args['ftp'] = get_ftp_from_config(args.get('server'))
            elif args.get('login'):
                args['ftp'] = get_ftp(**args.get('login'))
            else:
                args['ftp'] = get_ftp_from_config()
        return func(**args)
    return decorate


class Ftp():
    """Conetxt manager for opening up ftp connection and closing it after execution
    """
    def __init__(self, ftp=None, server=None, login=None) -> None:

        if ftp:
            self.ftp = ftp
        elif server:
            self.ftp = get_ftp_from_config(server)
        elif login:
            self.ftp = get_ftp(**login)
        else:
            self.ftp = get_ftp_from_config()


    def __enter__(self):
        return self.ftp


    def __exit__(self, type, value, traceback):
        self.ftp.quit()


def _ftp_worker_upload(queue, ftp=None, server=None, login=None, result=None):
    """worker for file up / download queue

    Args:
        queue (Queue): queue where worker gets tasks from
        func (function): method worker will execute for task (download, upload)
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    with Ftp(ftp, server, login) as ftp:
        while not queue.empty():
            data = queue.get(True)
            for i in range(CFG.get('retries', 5)):
                try:
                    upload(ftp=ftp, server=server, login=login, close=False, **data)
                    break
                except Exception as e:
                    src = data.get('src')
                    LOG.info(f'Retry {i+1} on data transfer {src}')
                    time.sleep(i**i+0.2)
            else:
                result.append(data.get('src'))
                LOG.exception(e)


def _ftp_worker_download(queue, ftp=None, server=None, login=None, result=None):
    """worker for file up / download queue

    Args:
        queue (Queue): queue where worker gets tasks from
        func (function): method worker will execute for task (download, upload)
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    with Ftp(ftp, server, login) as ftp:
        while not queue.empty():
            data = queue.get(True)
            for i in range(CFG.get('retries', 5)):
                try:
                    download(ftp=ftp, server=server, login=login, close=False, **data)
                    break
                except Exception as e:
                    src = data.get('src')
                    LOG.info(f'Retry {i+1} on data transfer {src}')
                    time.sleep(i**i+0.2)
            else:
                result.append(data.get('src'))
                LOG.exception(e)


def _ftp_multiple(files, typ, force=False, makedirs=False, ftp=None, server=None, login=None):
    """Queue for file up / download

    Args:
        files (dict): {string sourceFile : string destination file }
        typ (string): defines which worker should be used (upload, download)
        force (bool, optional): when True files will be overwritten. Defaults to False.
        makedirs (bool, optional): when True directories will be created. Defaults to True.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    queue = Queue()
    result = []
    for src, dst in files.items():
        queue.put({
            'src':src,
            'dst':dst,
            'force':force,
            'makedirs':makedirs
        })
    if typ == 'upload':
        pool = Pool(CFG.get('connections', 5), _ftp_worker_upload,(queue, ftp, server, login, result))
    elif typ == 'download':
        pool = Pool(CFG.get('connections', 5), _ftp_worker_download,(queue, ftp, server, login, result))

    queue.close()
    queue.join_thread()
    
    pool.close()
    pool.join()
    return result


@ftp_object
def ls(path, ftp=None, server=None, login=None, close=True):
    """list files and directories of given path

    Args:
        path (string): the ftp path to be checked;
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
        close (bool, optional): when True ftp connection will be closed after execution. Defaults to True.

    Returns:
        list: list of paths
    """
    data = ftp.nlst(path)
    if close:
        ftp.quit()
    return data


@ftp_object
def is_dir(path, ftp=None, server=None, login=None, close=True):
    """
    check if a path is a dir.

    Args:
        path (string): the ftp path to be checked;
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
        close (bool, optional): when True ftp connection will be closed after execution. Defaults to True.

    Returns:
        boolean: True if the path provided as arg represent a directory,
                    False otherwise.
    """
    current = ftp.pwd()
    try:
        ftp.cwd(path)
    except ftplib.error_perm:
        if close:
            ftp.quit()
        return False
    ftp.cwd(current)
    if close:
        ftp.quit()
    return True


@ftp_object
def mkdirs(path, ftp=None, server=None, login=None, close=True):
    """_summary_

    Args:
        path (str): path on the ftp starting from ftp root
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
        close (bool, optional): when True ftp connection will be closed after execution. Defaults to True.
    """

    dirs = path.split('/')
    for i in range(len(dirs)):
        dir = '/'.join(dirs[:i+1])
        if dir and not is_dir(dir, ftp, close=False):
            LOG.info(f'Create directory on FTP {dir}')
            ftp.mkd(dir)
        

@ftp_object
def download(src, dst, force=False, makedirs=True, ftp=None, server=None, login=None, close=True):
    """
    download one file

    Args:
        src (string): a full path of the file to be dowloaded from the ftp location;
        dst (string): a full path (file name included) of a location on the server where the file will be downloaded;
        force (boolean): when set to True the function will overwrite the
                            dst file if it already exists;
        makedirs (boolean): when set to True the function will try to create
                            all the dirs in the dst path if they don't exist
                            yet;
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
        close (bool, optional): when True ftp connection will be closed after execution. Defaults to True.
    """
    ftp.voidcmd('TYPE I')
    if os.path.isfile(dst) and not force:
        if close:
            ftp.quit()
        raise FileExistsError('destination path already exists')

    if not os.path.exists(os.path.dirname(dst)) and makedirs:
        LOG.debug(f'Create directories {dst}')
        os.makedirs(os.path.dirname(dst))

    if os.path.isfile(dst) and force:
        LOG.info(f'Downloading {src} and overwrite {dst}')
    else:
        LOG.info(f'Downloading {src} to {dst}')

    with open(dst, 'wb') as f:
        ftp.retrbinary('RETR {}'.format(src), f.write)
    if close:
        ftp.quit()


def download_multiple(files, force=False, makedirs=True, ftp=None, server=None, login=None):
    """download multiple files from the ftp 

    Args:
        files (dict): {string sourceFile on ftp : string destination file on server}
        force (bool, optional): when True files will be overwritten. Defaults to False.
        makedirs (bool, optional): when True directories will be created. Defaults to True.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    return _ftp_multiple(files, 'download', force, makedirs, ftp, server, login)


@ftp_object
def tree(src, pattern=None, ftp=None, server=None, login=None, _depth=0):
    """list the content given directory includin all subdirectories

    Args:
        src (str): path to directory on FTP
        force (bool, optional): when True files will be overwritten. Defaults to False.
        pattern (str, optional): regular expression describing files to be downloaded. Defaults to None.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
        _depth (int) : counter for recursion to now when to close ftp connection
    """

    for path in ls(src, ftp, close=False) or []:
        if is_dir(path, ftp, close=False):
            yield from tree(path, pattern, ftp, _depth=_depth+1)
        else:
            if pattern and re.search(pattern, path) or not pattern:
                yield path
    if _depth==0:
        ftp.quit()


def download_tree(src, dst, force=False, pattern=None, makedirs=True, ftp=None, server=None, login=None):
    """download content of a directory including all subdirectories

    Args:
        src (str): path to directory on FTP
        dst (str): path to directory on server
        force (bool, optional): when True files will be overwritten. Defaults to False.
        pattern (str, optional): regular expression describing files to be downloaded. Defaults to None.
        makedirs (bool, optional): when True directories will be created. Defaults to True.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    files =  {path:path.replace(src,dst) for path in tree(src, pattern, ftp, server, login) }
    return download_multiple(files, force, makedirs, ftp, server, login)


@ftp_object
def upload(src, dst, force=False, makedirs=True, ftp=None, server=None, login=None, close=True):
    """upload a file to the ftp

    Args:
        src (str): a full path (file name included) of a location on the server for a file to be uploaded
        dst (str): a full path of the file to be to the FTP
        force (bool, optional): when True files will be overwritten. Defaults to False.
        makedirs (bool, optional): when True directories will be created. Defaults to True.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
        close (bool, optional): when True ftp connection will be closed after execution. Defaults to True.

    Raises:
        FileExistsError: When file on ftp already exists and force is False
    """
    
    if not force and ls(dst, ftp, close=False):
        if close:
            ftp.quit()
        raise FileExistsError(f"File already exists on FTP {dst}")

    dir = dst.rsplit('/',1)[0]
    if makedirs and not is_dir(dir):
        mkdirs(dir, ftp, close=False)
  
    if ls(dst, ftp, close=False) and force:
        LOG.info(f'Uploading {src} and overwrite {dst}')
    else:
        LOG.info(f'Uploading {src} to {dst}')

    ftp.voidcmd('TYPE I')
    with open(src, 'rb') as f:
        ftp.storbinary(f"STOR {dst}", f)
    
    if close:
        ftp.quit()


def upload_multiple(files, force=False, makedirs=False, ftp=None, server=None, login=None):
    """upload multiple files to the FTP

    Args:
        files (dict): {string sourceFile on server : string destination file on FTP}
        force (bool, optional): when True files will be overwritten. Defaults to False.
        makedirs (bool, optional): when True directories will be created. Defaults to True.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    return _ftp_multiple(files, 'upload', force, makedirs, ftp, server, login)


def upload_tree(src, dst, force=False, pattern=None, makedirs=True, ftp=None, server=None, login=None):
    """_summary_

    Args:
        src (str): path to directory on server
        dst (str): path to directory on FTP
        force (bool, optional): when True files will be overwritten. Defaults to False.
        pattern (str, optional): regular expression describing files to be downloaded. Defaults to None.
        makedirs (bool, optional): when True directories will be created. Defaults to True.
        ftp (FTP / FTP_TLS, optional): ftp to connect to, if None default will be taken. Defaults to None.
        server (str, optional): name of server in config to get ftp object for, if ftp is passed this will be ignored. Defaults to None.
        login (dict, optional): arguments for get_ftp method, if ftp or server are passed on this will be ignored. Defaults to None.
    """
    files = {}

    for path in Path(src).glob('**/*'):
        if not path.is_file():
            continue
        if pattern and re.search(pattern, str(path)) or not pattern:
            files[str(path)] = str(path).replace(src, dst)

    return upload_multiple(files, force, makedirs, ftp, server, login)
