# -*- coding: utf-8 -*-

import os.path
import tempfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Future
from typing import Dict, List

from .mount import Mount
from .pyzmq.binding import *
from .tunnel import Message
from .process import ProcessDistribute
from .logger import logger
from .file import File, each_file


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

    def run(self) -> NoReturn:
        def target(req: bytes) -> bytes:
            msg = Message.load(req)

            if msg.cmd.upper() == 'CLOSE':
                return Message('ERROR', 'Can not close server at current version').bytes()

            handler = self.__functions.get(msg.cmd.upper(), None)
            if handler is None:
                error = f'Received unknown cmd {msg.cmd}'
                logger.error(error)
                return Message('ERROR', error).bytes()

            try:
                ret = handler(msg)
                logger.debug(f'Response {ret}')
                return ret.bytes()
            except Exception as e:
                logger.error(e)
                return Message('ERROR', str(e)).bytes()

        rep = MultiThreadRep(port=self.__port, target=target, threads=self.__processes)
        rep.run()

    def info(self, msg: Message) -> Message:
        return Message('OK', processes=self.__processes)

    def setup(self, msg: Message) -> Message:
        script_content = msg.args[0]

        # script_dir = script_cache_dir()
        # os.makedirs(script_dir, exist_ok=True)
        #
        # now = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        # suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        #
        # filename = f'job-{now}-{suffix}.py'
        #
        # script_path = os.path.join(script_dir, filename)
        # with open(script_path, 'w', encoding='utf-8') as f:
        #     f.write(script_content)
        #
        # self.__script_path = script_path
        #
        # logger.debug(f'Setup {script_path}')

        if self.__pool is not None:
            self.__pool.shutdown()

        # self.__pool = ProcessDistribute(pathlib.Path(script_path), self.__processes)
        self.__pool = ProcessDistribute(script_content, self.__processes)

        return Message('OK')

    def call(self, msg: Message) -> Message:
        # copy work files to local
        results: List[Future] = []
        for arg in each_file(msg.args):
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
        for arg in each_file(args):
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
