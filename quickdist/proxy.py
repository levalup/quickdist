# -*- coding: utf-8 -*-

"""
This file only run in subprocess
"""

from typing import Dict

from .pyzmq.binding import Dealer
from .tunnel import Message
from .logger import logger
from .mount import Mount


class Proxy(object):
    def __init__(self, host: str, port: int):
        self.__client = Dealer(host, port)

    @property
    def host(self):
        return self.__client.host

    @property
    def port(self):
        return self.__client.port

    def close(self):
        self.__client.close()

    def __send(self, cmd: str, *args, **kwargs) -> Message:
        self.__client.socket.send(Message(cmd, *args, **kwargs).bytes())
        body = self.__client.socket.recv()
        ret = Message.load(body)
        return ret

    def setup(self, script_file: str):
        with open(script_file, 'r', encoding='utf-8') as f:
            script_content = f.read()
        ret = self.__send('SETUP', script_content)
        if ret.cmd != 'OK':
            logger.error(ret)
            raise RuntimeError(ret)

    def info(self) -> Dict:
        ret = self.__send('INFO')
        if ret.cmd != 'OK':
            logger.error(ret)
            raise RuntimeError(ret)
        return ret.kwargs

    def call(self, *args, **kwargs) -> Message:
        return self.__send('CALL', *args, **kwargs)

    def mount(self, mount: Mount):
        ret = self.__send('MOUNT', mount)
        if ret.cmd != 'OK':
            logger.error(ret)
            raise RuntimeError(ret)


def main():
    pass


if __name__ == '__main__':
    main()
