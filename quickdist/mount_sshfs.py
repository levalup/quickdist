# -*- coding: utf-8 -*-

import os
import tempfile
import pathlib
from typing import Dict, Optional, List, Tuple

from .mount import Mount


def format_path(path: str):
    return path.replace('\\', '_').replace('/', '_')


class MountSshfs(Mount):
    def __init__(self):
        super().__init__()

        self.__username: str = ''
        self.__password: Optional[str] = ''
        self.__host: str = ''
        self.__port: Optional[int] = 22

    def address(self, host: str, port: int = 22):
        self.__host = host
        self.__port = port

    def passwd(self, username: str, password: str = None):
        self.__username = username
        self.__password = password


    def __mount_root(self):
        return os.path.join(tempfile.gettempdir(), 'quickdist', 'mount', self.__host, self.__username)

    def script(self, platform=None) -> str:
        mount_root = self.__mount_root()

        script_functions_path = os.path.join(os.path.dirname(__file__), 'scripts', 'linux_mount_sshfs.sh')
        with open(script_functions_path, 'r', encoding='utf-8') as f:
            script_functions = f.read()

        lines = []
        def append(remote, local):
            lines.append('')
            lines.append(f'manage_mount_path {local}')
            if self.__password:
                lines.append(f'mount_sshfs "{local}" "{remote}" "{self.__host}" {self.__port} "{self.__username}"'
                             f' "{self.__password}"')
            else:
                lines.append(f'mount_sshfs "{local}" "{remote}" "{self.__host}" {self.__port} "{self.__username}"')

        if self._tempdir:
            local = os.path.join(mount_root, format_path(self._tempdir))
            append(self._tempdir, local)
        if self._workdir:
            local = os.path.join(mount_root, format_path(self._workdir))
            append(self._workdir, local)
        for sep, path in self._workdirs.items():
            local = os.path.join(mount_root, format_path(path))
            append(path, local)

        lines.append('')
        for k, v in self.env():
            lines.append(f'export {k}="{v}"')

        lines.append('')

        return '\n'.join([script_functions, *lines])

    def env(self) -> List[Tuple[str, str]]:
        mount_root = self.__mount_root()
        kv: List[Tuple[str, str]] = []
        if self._tempdir:
            k = 'TEMPDIR'
            v = os.path.join(mount_root, format_path(self._tempdir))
            kv.append((k, v))
        if self._workdir:
            k = 'WORKDIR'
            v = os.path.join(mount_root, format_path(self._workdir))
            kv.append((k, v))
        for sep, path in self._workdirs.items():
            k = f'WORKDIR_{sep.upper()}'
            v = os.path.join(mount_root, format_path(path))
            kv.append((k, v))
        return kv


def main():
    pass


if __name__ == '__main__':
    main()
