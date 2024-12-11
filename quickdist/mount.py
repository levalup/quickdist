# -*- coding: utf-8 -*-

import os.path
import tempfile
import platform as pf
from abc import abstractmethod
from typing import List, Tuple, Dict, Optional
import subprocess
import pathlib
import uuid


def get_nodeid():
    home = str(pathlib.Path.home())
    nodeid_file = os.path.join(home, '.quickdist', 'nodeid')
    nodeid_root = os.path.dirname(nodeid_file)

    if nodeid_root:
        os.makedirs(nodeid_root, exist_ok=True)

    if not os.path.exists(nodeid_file):
        new_uuid = str(uuid.uuid4())
        with open(nodeid_file, 'w', encoding='utf-8') as f:
            f.write(new_uuid)

    with open(nodeid_file, 'r', encoding='utf-8') as f:
        content = f.read().strip()
    return content


def quick_link(src: str, dst: str):
    if os.path.exists(dst):
        os.unlink(dst)

    root = os.path.dirname(dst)
    if root:
        os.makedirs(root, exist_ok=True)

    os.symlink(src, dst, target_is_directory=True)


class Mount(object):
    def __init__(self):
        """
        This function will default write cache information.
        Use to verify if Mount running in self information.
        """
        self.__nodeid = get_nodeid()
        self._workdir: Optional[str] = ''
        self._workdirs: Dict[str, str] = {}
        self._tempdir: str = ''

    @property
    def nodeid(self):
        return self.__nodeid

    def workdir(self, path: str, source: str = None) -> pathlib.Path:
        if source:
            source = source.lower()
            self._workdirs[source] = path
        else:
            self._workdir = path
        return pathlib.Path(path)

    def tempdir(self, path: str) -> pathlib.Path:
        self._tempdir = path
        return pathlib.Path(path)

    @abstractmethod
    def script(self, platform = None) -> str:
        """
        Get mount script.
        :param platform:
        :return:
        """
        return ''

    @abstractmethod
    def env(self) -> List[Tuple[str, str]]:
        """
        :return: environ to run scripts
        """
        return []

    def __mount_host(self):
        mount_root = os.path.join(tempfile.gettempdir(), 'quickdist', 'hosting')
        env: List[Tuple[str, str]] = []

        os.makedirs(mount_root, exist_ok=True)

        # host ignore temp dir setting, use same setting in local dir
        # if self._tempdir:
        #     k = 'TEMPDIR'
        #     v = self._tempdir
        #     env.append((k, v))

        if self._workdir or self._workdirs:
            k = 'LOCALDIR'
            v = mount_root
            env.append((k, v))
            env.append(('TEMPDIR', v))  # use same temp setting in host

        if self._workdir:
            k = 'WORKDIR'
            v = os.path.join(mount_root, '__root__')
            path = self._workdir
            quick_link(path, v)
            env.append((k, path))
        if self._workdirs:
            for sep, path in self._workdirs.items():
                k = f'WORKDIR_{sep.upper()}'
                v = os.path.join(mount_root, sep.lower())
                quick_link(path, v)
                env.append((k, path))

        for k, v in env:
            os.environ[k] = v

    def __mount_remote(self):
        script_content = self.script()
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as temp_script:
            temp_script.write(script_content)
            script_path = temp_script.name
        try:
            subprocess.run(
                ['bash', script_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f'Mount failed: {e.stderr or e.stdout}')
        finally:
            if script_path and os.path.exists(script_path):
                os.unlink(script_path)
        for k, v in self.env():
            os.environ[k] = v

    def mount(self):
        """
        Run mount script， setup env
        :return:
        """
        assert pf.system() == 'Linux'
        if get_nodeid() == self.nodeid:
            self.__mount_host()
        else:
            self.__mount_remote()

def main():
    pass


if __name__ == '__main__':
    main()
