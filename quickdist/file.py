# -*- coding: utf-8 -*-
import json
import os
import os.path as osp
import pathlib
import hashlib
import shutil
from enum import Enum
from collections import deque
from typing import Any, Dict, Union, List, Generator, Optional

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


def get_workdir(origin: str = None) -> str:
    if origin:
        env = f'WORKDIR_{origin.upper()}'
        key = f'workdirs.{origin.lower()}'
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


def get_tempdir(origin: str = None) -> str:
    env = 'TEMPDIR'
    key = 'tempdir'
    if origin:
        sep = origin.lower()
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


def get_localdir(origin: str = None) -> str:
    env = 'LOCALDIR'
    key = 'localdir'
    if origin:
        sep = origin.lower()
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


class Location(Enum):
    workdir = 1
    local = 2
    temp = 3


class File(object):
    def __init__(self, location: Location, path: str,
                 origin: str = None,
                 copy_from: Location = None,
                 md5: str = None):
        """
        :param path: path is related path in local, temp and workdir
        :param origin:
        :param copy_from: copy from location
        :param md5: copy check md5
        """
        self.__location: Location = location
        self.__path = path
        self.__origin = origin

        # Copied file from location
        self.__from: Optional[Location] = copy_from

        # File MD5
        self.__md5 = md5

    def _to(self, location: Location):
        if self.__location == location:
            return self
        if self.__from is not None:
            raise RuntimeError('Unable to copy a file that is waiting to be copied')
        current_path = self.path(self.__location)
        if not osp.exists(current_path):
            raise FileNotFoundError(current_path)
        md5 = self.__md5 or calculate_md5(current_path)
        copy_from = self.__from or self.__location
        return File(
            location, self.__path, self.__origin,
            copy_from=copy_from, md5=md5,
        )

    def to_local(self):
        """
        Copy file to local.
        :return: File
        """
        return self._to(Location.local)

    def to_origin(self):
        """
        Copy file to origin directly.
        :return: File
        """
        return self._to(Location.workdir)

    def to_temp(self):
        """
        Copy file to temp, then the host system will copy it into workdir.
        :return: File
        """
        return self._to(Location.temp)

    @property
    def copied(self) -> bool:
        return self.__from is not None

    def copy(self):
        """
        Copy file from `self.__from`.
        Execute copy.
        :return: File
        """
        if self.__from is None or self.__from == self.__location:
            return self
        src = self.path(self.__from)
        dst = self.path(self.__location)
        if same_path(src, dst):
            self.__from = None
            return self
        if osp.isfile(dst) and calculate_md5(dst) == self.__md5:
            self.__from = None
            return self
        copy_file(src, dst)
        self.__from = None
        return self

    def path(self, location: Location = None) -> str:
        if location is None:
            location = self.__location
        if location is Location.temp:
            return osp.join(get_tempdir(self.__origin), self.__path)
        if location is Location.local:
            return osp.join(get_localdir(self.__origin), self.__path)
        if location is Location.workdir:
            return osp.join(get_workdir(self.__origin), self.__path)
        return self.__path

    @property
    def parent(self):
        return File(self.__location, os.path.dirname(self.__path))

    def __truediv__(self, tail: str):
        return File(self.__location, os.path.join(self.__path, tail))


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
    def __init__(self, path: str, origin: str = None):
        super().__init__(Location.workdir, reduce_absolute(path, get_workdir(origin)), origin)


class LocalFile(File):
    def __init__(self, path: str, origin: str = None):
        super().__init__(Location.local, reduce_absolute(path, get_localdir(origin)), origin)


class TemplFile(File):
    def __init__(self, path: str, origin: str = None):
        super().__init__(Location.temp, reduce_absolute(path, get_tempdir(origin)), origin)


def each_file(a: Any) -> Generator[File, None, None]:
    if isinstance(a, File):
        yield a
        return

    cache = set()
    iters = deque()
    if isinstance(a, (list, tuple, dict)):
        cache.add(a)
        iters.append(a)

    while iters:
        values = iters.popleft()
        if isinstance(values, dict):
            for v in values.values():
                if isinstance(v, File):
                    yield v
                elif isinstance(v, (list, tuple, dict)) and v not in cache:
                    cache.add(v)
                    iters.append(v)
        else:
            for v in values:
                if isinstance(v, File):
                    yield v
                elif isinstance(v, (list, tuple, dict)) and v not in cache:
                    cache.add(v)
                    iters.append(v)


def main():
    pass


if __name__ == '__main__':
    main()
