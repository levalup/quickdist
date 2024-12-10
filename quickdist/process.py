# -*- coding: utf-8 -*-

import os
import importlib.util
import multiprocessing
from multiprocessing.pool import Pool
from typing import Callable, Union, Optional, Any, Tuple, List, Iterable


__all__ = [
    'ProcessDistribute',
]


def load_script_main(script_path: str, module_name: str = None) -> Tuple[Callable, Callable]:
    if module_name is None:
        name = os.path.splitext(os.path.basename(script_path))[0]
        module_name = f'quickdist.dynamic.{name}'
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    main_entry = getattr(module, 'main', None)
    init_entry = getattr(module, 'init', None)
    if not callable(main_entry):
        raise ValueError("The specified script does not have a callable 'main' function.")
    if init_entry is not None and not callable(init_entry):
        raise ValueError("The specified script does not have a callable 'init' function.")
    return main_entry, init_entry


__subprocess_init: Optional[Callable] = None
__subprocess_main: Optional[Callable] = None
__subprocess_id: Optional[int] = None


def init_subprocess(script: Union[str, Callable], serial: multiprocessing.Value):
    global __subprocess_init
    global __subprocess_main
    global __subprocess_id

    with serial.get_lock():
        __subprocess_id = serial.value
        serial.value += 1

    os.environ['PROCESS_ID'] = f'{__subprocess_id}'
    os.environ['PID'] = f'{__subprocess_id}'

    if callable(script):
        __subprocess_main = script
    elif isinstance(script, str):
        __subprocess_main, __subprocess_init = load_script_main(script)
    else:
        raise ValueError("The specified script does not have a callable 'main' function.")
    if __subprocess_init is not None:
        __subprocess_init()


def run_subprocess(*args, **kwargs):
    global __subprocess_main
    if __subprocess_main is None:
        raise RuntimeError("Main function has not been initialized.")
    return __subprocess_main(*args, **kwargs)


class ProcessDistribute(object):
    def __init__(self, script: Union[str, Callable], size: int = None):
        serial = multiprocessing.Value('i', 0)
        self.__size = size
        self.__ctx = multiprocessing.get_context('spawn')
        self.__pool: Pool = self.__ctx.Pool(
            processes=size,
            initializer=init_subprocess,
            initargs=(script, serial)
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__pool.__exit__(exc_type, exc_val, exc_tb)

    def shutdown(self):
        self.__pool.close()
        self.__pool.join()

    def call_async(self, *args, **kwargs):
        return self.__pool.apply_async(run_subprocess, args, kwargs)

    def call(self, *args, **kwargs) -> Any:
        return self.__pool.apply(run_subprocess, args, kwargs)

    def map(self, iterable, chunk_size=None) -> List[Any]:
        return self.__pool.map(run_subprocess, iterable, chunksize=chunk_size)

    def imap(self, iterable, chunk_size=None) -> Iterable[Any]:
        return self.__pool.imap(run_subprocess, iterable, chunksize=chunk_size)

    def __call__(self, *args, **kwargs) -> Any:
        return self.__pool.apply(run_subprocess, args, kwargs)


def main():
    pass


if __name__ == '__main__':
    main()
