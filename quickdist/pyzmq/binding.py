# -*- coding: utf-8 -*-

import sys
import uuid
import threading
import multiprocessing
from typing import Optional, Union, NoReturn, Callable

import zmq


class Context(object):
    def __init__(self):
        self.__ctx = zmq.Context()

    def destroy(self):
        self.__ctx.destroy()

    @property
    def ctx(self) -> zmq.Context:
        return self.__ctx

    def __enter__(self) -> zmq.Context:
        return self.__ctx

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__ctx.destroy()


class Socket(object):
    def __init__(self, socket_type: int, ctx: Union[Context, zmq.Context] = None):
        self.__ctx: Optional[Context] = None
        if ctx is None:
            self.__ctx = Context()
            ctx = self.__ctx

        if isinstance(ctx, Context):
            ctx = ctx.ctx
        ctx: zmq.Context
        self.__raw_context = ctx
        self.__socket: zmq.Socket = ctx.socket(socket_type)

    @property
    def context(self) -> zmq.Context:
        return self.__raw_context

    @property
    def socket(self) -> zmq.Socket:
        return self.__socket

    def close(self):
        self.__socket.close()
        if self.__ctx:
            self.__ctx.destroy()

    def __enter__(self) -> zmq.Socket:
        return self.__socket

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__socket.close()
        if self.__ctx is not None:
            self.__ctx.__exit__(exc_type, exc_val, exc_tb)


class Router(Socket):
    def __init__(self, port: int, ctx: Union[Context, zmq.Context] = None):
        super().__init__(zmq.ROUTER, ctx)
        self.__port = port
        try:
            self.socket.bind(f'tcp://*:{port}')
        except Exception as _:
            self.__exit__(*sys.exc_info())
            raise

    @property
    def port(self):
        return self.__port


class Dealer(Socket):
    def __init__(self, host: str, port: int, ctx: Union[Context, zmq.Context] = None):
        super().__init__(zmq.DEALER, ctx)
        self.__port = port
        self.__host = host
        try:
            self.__identity = str(uuid.uuid4())
            identity = self.__identity.encode('utf-8')
            self.socket.setsockopt(zmq.IDENTITY, identity)
            self.socket.connect(f'tcp://{host}:{port}')
        except Exception as _:
            self.__exit__(*sys.exc_info())
            raise

    @property
    def identity(self) -> str:
        return self.__identity

    @property
    def port(self):
        return self.__port

    @property
    def host(self):
        return self.__host


class MultiThreadRep(Socket):
    def __init__(self, port: int,
                 target: Callable[[bytes], Optional[bytes]],
                 threads=None,
                 ctx: Union[Context, zmq.Context] = None):
        super().__init__(zmq.ROUTER, ctx)
        self.__port = port
        try:
            self.socket.bind(f'tcp://*:{port}')
        except Exception as _:
            self.__exit__(*sys.exc_info())
            raise

        self.__frontend = self
        self.__backend = Socket(zmq.DEALER, self.context)
        self.__backend_addr = f'inproc://workers/{id(self)}'
        self.__backend.socket.bind(self.__backend_addr)

        if threads is None:
            threads = multiprocessing.cpu_count()

        self.__threads = threads
        self.__target = target
        self.__threads = [
            threading.Thread(target=self._work, args=(i, ))
            for i in range(threads)
        ]
        for t in self.__threads:
            t.start()

    @property
    def port(self):
        return self.__port

    def _work(self, thread_id: int = 0):
        with Socket(zmq.REP, self.__backend.context) as socket:
            socket.connect(self.__backend_addr)
            while True:
                req = socket.recv()
                try:
                    rep = self.__target(req)
                    socket.send(rep)
                except Exception as _:
                    continue

    def run(self) -> NoReturn:
        zmq.proxy(self.__frontend.socket, self.__backend.socket)

    def close(self):
        self.__backend.close()
        super().close()

        for t in self.__threads:
            t.join()


class Req(Socket):
    def __init__(self, host: str, port: int, ctx: Union[Context, zmq.Context] = None):
        super().__init__(zmq.REQ, ctx)
        self.__port = port
        self.__host = host
        try:
            self.socket.connect(f'tcp://{host}:{port}')
        except Exception as _:
            self.__exit__(*sys.exc_info())
            raise

    @property
    def port(self):
        return self.__port

    @property
    def host(self):
        return self.__host
