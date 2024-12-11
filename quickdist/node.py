# -*- coding: utf-8 -*-

import os.path
import random
import string
import tempfile
import multiprocessing
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Future
from typing import Dict, Callable, List

from .mount import Mount
from .pyzmq.binding import *
from .tunnel import Message
from .process import ProcessDistribute
from .logger import logger
from .file import File


def script_cache_dir():
    return os.path.join(tempfile.gettempdir(), 'quickdist', 'jobs')


def pong(msg: Message) -> Message:
    return Message('PONG', *msg.args, **msg.kwargs)


def copy_origin_to_local(file: File):
    if not file.nocopy:
        file.to_local()


def copy_local_to_temp(file: File):
    file.to_temp()


class Node(object):
    def __init__(self, port: int = 8421, processes: int = None):
        if processes is None:
            processes = multiprocessing.cpu_count()

        self.__port = port
        self.__processes = processes
        self.__ctx = multiprocessing.get_context('spawn')

        # using subprocess to copy file
        self.__executor = ProcessPoolExecutor(mp_context=self.__ctx, max_workers=processes)
        # using thread pool to response request
        self.__handle_executor = ThreadPoolExecutor(max_workers=processes)

        self.__script_path: Optional[str] = None
        self.__pool: Optional[ProcessDistribute] = None

        self.__timeout_ms = 1000

        self.__functions: Dict[str, Callable[[Message], Message]] = {
            'PING': pong,
            'INFO': self.info,
            'SETUP': self.setup,
            'CALL': self.call,
            'MOUNT': self.mount,
        }

    def run(self):
        with Router(port=self.__port) as server:
            server.setsockopt(zmq.RCVTIMEO, self.__timeout_ms)

            logger.info(f"Serve node :{self.__port}")
            while True:
                try:
                    try:
                        identity, body = server.recv_multipart()
                    except zmq.Again as _:
                        continue
                    msg = Message.load(body)

                    if msg.cmd.upper() == 'CLOSE':
                        break

                    handler = self.__functions.get(msg.cmd.upper(), None)
                    if handler is None:
                        error = f'Received unknown cmd {msg.cmd}'
                        logger.error(error)
                        server.send_multipart([identity, Message('ERROR', error).bytes()])
                        continue

                    logger.debug(f'Received {msg}')

                    def response(i, m):
                        try:
                            ret = handler(m)
                            logger.debug(f'Response {ret}')
                            server.send_multipart([i, ret.bytes()])
                        except Exception as a:
                            logger.error(a)
                            server.send_multipart([i, Message('ERROR', str(a)).bytes()])
                    self.__handle_executor.submit(response, identity, msg)

                except Exception as e:
                    logger.error(e)

    def info(self, msg: Message) -> Message:
        return Message('OK', processes=self.__processes)

    def setup(self, msg: Message) -> Message:
        script_content = msg.args[0]

        script_dir = script_cache_dir()
        os.makedirs(script_dir, exist_ok=True)

        now = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

        filename = f'job-{now}-{suffix}.py'

        script_path = os.path.join(script_dir, filename)
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(script_content)

        logger.debug(f'Setup {script_path}')

        if self.__pool is not None:
            self.__pool.shutdown()

        self.__script_path = script_path
        self.__pool = ProcessDistribute(script_path, self.__processes)

        return Message('OK')

    def call(self, msg: Message) -> Message:
        # copy work files to local
        results: List[Future] = []
        for arg in msg.args:
            if isinstance(arg, File):
                if not arg.nocopy:
                    logger.debug(f'COPY(WORK->LOCAL): {arg.path}')
                    results.append(self.__executor.submit(copy_origin_to_local, arg))
        for result in results:
            result.result()

        ret = self.__pool.call(*msg.args, **msg.kwargs)
        if isinstance(ret, tuple):
            args = ret
        else:
            args = (ret, )

        # copy local files to temp
        results: List[Future] = []
        for arg in args:
            if isinstance(arg, File):
                if not arg.nocopy:
                    logger.debug(f'COPY(LOCAL->TEMP): {arg.path}')
                    results.append(self.__executor.submit(copy_local_to_temp, arg))
        for result in results:
            result.result()

        return Message('OK', *args)

    def mount(self, msg: Message) -> Message:
        m = msg.args[0]
        if isinstance(m, Mount):
            m.mount()
            return Message('OK')

        return Message('ERROR', f'Unsupported mount object {type(m)}')


def main():
    node = Node()
    node.run()


if __name__ == '__main__':
    main()
