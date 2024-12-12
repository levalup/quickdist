# -*- coding: utf-8 -*-
import json
import os
import os.path as osp
import pathlib
import hashlib
import shutil
from collections import deque
from typing import Any, Dict, Union, List, Generator

__all__ = [
    'File',
    'WorkFile',
    'LocalFile',
    'TemplFile',
    'each_file',
]


def quickdist_config_json() -> str:
    home = str(pathlib.Path.home())
    return osp.join(home, '.quickdist', 'config.json')


def parse_json(obj: Dict, key: Union[str, List[str]]) -> Any:
    if obj is None:
        return None
    if isinstance(key, list):
        if not key:
            return obj
        return parse_json(obj.get(key[0], None), key[1:])
    if not isinstance(obj, dict):
        raise ValueError(f'Missing key {key}')
    for k, v in obj.items():
        if k.lower() == key.lower():
            return v
    return None


def load_json_value(path: str, key: str) -> Any:
    try:
        with open(path, 'rb') as f:
            obj = json.load(f)
        keys = [k for k in key.split('.') if k]
        return parse_json(obj, keys)
    except Exception as _:
        return None


def get_workdir(source: str = None) -> str:
    if source:
        env = f'WORKDIR_{source.upper()}'
        key = f'workdirs.{source.lower()}'
    else:
        env = 'WORKDIR'
        key = 'workdir'

    env_value = os.environ.get(env, '')
    if env_value:
        return env_value

    config_json = quickdist_config_json()
    json_value = load_json_value(config_json, key)
    if json_value:
        return json_value

    raise ValueError(f'Missing config in environment {env} or json({config_json}, "{key}")')


def get_tempdir(source: str = None) -> str:
    env = 'TEMPDIR'
    key = 'tempdir'
    if source:
        sep = source.lower()
    else:
        sep = '__root__'

    env_value = os.environ.get(env, '')
    if env_value:
        return osp.join(env_value, sep)

    config_json = quickdist_config_json()
    json_value = load_json_value(config_json, key)
    if json_value:
        return osp.join(json_value, sep)

    raise ValueError(f'Missing config in environment {env} or json({config_json}, "{key}")')


def get_localdir(source: str = None) -> str:
    env = 'LOCALDIR'
    key = 'localdir'
    if source:
        sep = source.lower()
    else:
        sep = '__root__'

    env_value = os.environ.get(env, '')
    if env_value:
        return osp.join(env_value, sep)

    config_json = quickdist_config_json()
    json_value = load_json_value(config_json, key)
    if json_value:
        return osp.join(json_value, sep)

    home = str(pathlib.Path.home())
    return osp.join(home, '.quickdist', 'cache')


def calculate_md5(realpath: str, chunk_size: int = 8192):
    md5 = hashlib.md5()

    try:
        with open(realpath, 'rb') as f:
            while chunk := f.read(chunk_size):
                md5.update(chunk)
    except FileNotFoundError:
        print(f"File not found: {realpath}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    return md5.hexdigest()


def copy_file(src: str, dst: str):
    dst_root = os.path.dirname(dst)
    if dst_root:
        os.makedirs(dst_root, exist_ok=True)
    shutil.copy(src, dst)


def same_path(path1: str, path2: str):
    try:
        if os.path.samefile(path1, path2):
            return True

        real_path1 = os.path.realpath(path1)
        real_path2 = os.path.realpath(path2)

        norm_path1 = os.path.normcase(real_path1)
        norm_path2 = os.path.normcase(real_path2)

        return norm_path1 == norm_path2
    except Exception as _:
        return False


class File(object):
    def __init__(self, path: str, source: str = None, nocopy: bool = False):
        """
        :param path: path is related path in local, temp and workdir
        :param source:
        :param nocopy: not copy file from origin to local
        """
        self.__source = source
        self.__path = path
        self._md5 = None
        self.__nocopy = nocopy

    @property
    def path(self):
        return self.__path

    @property
    def nocopy(self):
        return self.__nocopy

    @property
    def md5(self):
        return self._md5

    def to_local(self, cache: bool = True):
        """
        Copy file from workdir in local
        :return:
        """
        src = self.origin
        dst = self.local
        if same_path(src, dst):
            return
        if cache and osp.isfile(dst) and calculate_md5(dst) == self.md5:
            return
        copy_file(src, dst)

    def to_temp(self, cache: bool = True):
        """
        Copy file from local to temp
        The Copy may cross network
        :return:
        """
        src = self.local
        dst = self.temp
        if same_path(src, dst):
            return
        if cache and osp.isfile(dst) and calculate_md5(dst) == self.md5:
            return
        copy_file(src, dst)

    def to_origin(self, cache: bool = True):
        """
        Copy file from temp to workdir
        :return:
        """
        src = self.temp
        dst = self.origin
        if same_path(src, dst):
            return
        if cache and osp.isfile(dst) and calculate_md5(dst) == self.md5:
            return
        copy_file(src, dst)

    @property
    def origin(self) -> str:
        return osp.join(get_workdir(self.__source), self.__path)

    @property
    def local(self) -> str:
        return osp.join(get_localdir(self.__source), self.__path)

    @property
    def temp(self) -> str:
        return osp.join(get_tempdir(self.__source), self.__path)


def reduce_absolute(path: str, root: str):
    if root:
        root = os.path.abspath(root)
    if osp.isabs(path):
        return osp.relpath(path, root)
    if not osp.exists(osp.join(root, path)) and osp.exists(path):
        path = osp.abspath(path)
        return osp.relpath(path, root)
    return path


class WorkFile(File):
    def __init__(self, path: str, source: str = None, nocopy: bool = False):
        super().__init__(reduce_absolute(path, get_workdir(source)), source, nocopy)
        self._md5 = calculate_md5(self.origin)


class LocalFile(File):
    def __init__(self, path: str, source: str = None):
        super().__init__(reduce_absolute(path, get_localdir(source)), source)
        self._md5 = calculate_md5(self.local)


class TemplFile(File):
    def __init__(self, path: str, source: str = None):
        super().__init__(reduce_absolute(path, get_tempdir(source)), source)
        self._md5 = calculate_md5(self.temp)


def each_file(a: Any) -> Generator[File, None, None]:
    if isinstance(a, File):
        yield a
        return

    iters = deque()
    if isinstance(a, (list, tuple, dict)):
        iters.append(a)

    while iters:
        values = iters.popleft()
        if isinstance(values, dict):
            for v in values.values():
                if isinstance(v, File):
                    yield v
                elif isinstance(v, (list, tuple, dict)):
                    iters.append(v)
        else:
            for v in values:
                if isinstance(v, File):
                    yield v
                elif isinstance(v, (list, tuple, dict)):
                    iters.append(v)


def main():
    pass


if __name__ == '__main__':
    main()
