# -*- coding: utf-8 -*-

"""
This file only run in subprocess
"""

from datetime import datetime
import multiprocessing
from multiprocessing.pool import Pool
from typing import Optional, List, Tuple, Any, Iterable

from .proxy import Proxy
from .file import File

proxy: Optional[Proxy] = None


def init(host: str, port: int):
    global proxy
    proxy = Proxy(host, port)


def main(*args, **kwargs):
    global proxy
    ret = proxy.call(*args, **kwargs)

    if ret.cmd == 'OK':
        # copy temp files to work dir
        for arg in ret.args:
            if isinstance(arg, File):
                if not arg.nocopy:
                    # print(f'[{datetime.now()}] [DEBUG] COPY(TEMP->WORK): {arg.path}')
                    arg.to_origin()
    else:
        raise RuntimeError(f'{ret} on {proxy.host}:{proxy.port}')

    if len(ret.args) == 1:
        return ret.args[0]
    return ret.args


def init_ex(links: List[Tuple], serial: multiprocessing.Value):
    with serial.get_lock():
        index = serial.value
        serial.value += 1
    init(*links[index])


class ProxyPool(object):
    def __init__(self, links: List[Tuple[str, int]]):
        serial = multiprocessing.Value('i', 0)
        self.__ctx = multiprocessing.get_context('spawn')
        self.__pool: Pool = self.__ctx.Pool(
            processes=len(links),
            initializer=init_ex,
            initargs=(links, serial)
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__pool.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        self.__pool.close()

    def join(self):
        self.__pool.join()

    def shutdown(self):
        self.__pool.close()
        self.__pool.join()

    def call_async(self, *args, **kwargs):
        return self.__pool.apply_async(main, args, kwargs)

    def call(self, *args, **kwargs) -> Any:
        return self.__pool.apply(main, args, kwargs)

    def map(self, iterable, chunk_size=None) -> List[Any]:
        return self.__pool.map(main, iterable, chunksize=chunk_size)

    def imap(self, iterable, chunk_size=None) -> Iterable[Any]:
        return self.__pool.imap(main, iterable, chunksize=chunk_size)

    def __call__(self, *args, **kwargs) -> Any:
        return self.__pool.apply(main, args, kwargs)
