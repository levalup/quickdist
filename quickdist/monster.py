# -*- coding: utf-8 -*-

import multiprocessing
from typing import List, Optional, Tuple, Any, Iterable

from .logger import logger
from .proxy import Proxy
from .monster_proxy import ProxyPool


class Monster(object):
    def __init__(self):
        self.__nodes: List[Proxy] = []
        self.__pool: Optional[ProxyPool] = None

    def close(self):
        for node in self.__nodes:
            node.close()
        if self.__pool is not None:
            self.__pool.shutdown()

    def connect(self, host: str, port: int = 8421):
        self.__nodes.append(Proxy(host, port))

    def setup(self, script_file: str):
        for node in self.__nodes:
            node.setup(script_file)

        # build pipeline
        links: List[Tuple[str, int]] = []

        for node in self.__nodes:
            info = node.info()
            processes = info.get('processes', 1)
            links.extend([(node.host, node.port)] * processes)

        serial = multiprocessing.Value('i', 0)
        ctx = multiprocessing.get_context('spawn')

        if self.__pool is not None:
            self.__pool.shutdown()

        self.__pool = ProxyPool(links)

    def _test(self, *args, **kwargs):
        # use pipeline
        if not self.__nodes:
            raise ValueError('Has no node registered.')
        node = self.__nodes[0]
        ret = node.call(*args, **kwargs)
        if ret.cmd != 'OK':
            logger.error(ret)
            raise ValueError(ret)
        if len(ret.args) == 1:
            return ret.args[0]
        return ret.args

    def join(self):
        self.__pool.join()

    def call_async(self, *args, **kwargs):
        assert self.__pool is not None
        return self.__pool.call_async(args, kwargs)

    def call(self, *args, **kwargs) -> Any:
        assert self.__pool is not None
        return self.__pool.call(args, kwargs)

    def map(self, iterable, chunk_size=None) -> List[Any]:
        assert self.__pool is not None
        return self.__pool.map(iterable, chunk_size=chunk_size)

    def imap(self, iterable, chunk_size=None) -> Iterable[Any]:
        assert self.__pool is not None
        return self.__pool.imap(iterable, chunk_size=chunk_size)

    def __call__(self, *args, **kwargs) -> Any:
        assert self.__pool is not None
        return self.__call__(args, kwargs)


def main():
    pass


if __name__ == '__main__':
    main()
