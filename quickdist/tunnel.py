# -*- coding: utf-8 -*-

import pickle
from typing import Tuple, Dict, Any


class Message(object):
    def __init__(self, cmd: str, *args, **kwargs):
        self.__cmd: str = cmd
        self.__args: Tuple[Any, ...] = args
        self.__kwargs: Dict[str, Any] = kwargs

    @property
    def cmd(self) -> str:
        return self.__cmd

    @property
    def args(self) -> Tuple[Any, ...]:
        return self.__args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.__kwargs

    def bytes(self) -> bytes:
        return pickle.dumps(self)

    @staticmethod
    def load(pkl: bytes):
        obj: Message = pickle.loads(pkl)
        return obj

    def __str__(self):
        prefix = [f'{self.cmd}(']
        params = []
        for v in self.args:
            params.append(repr(v))
        for k, v in self.kwargs.items():
            params.append(f'{str(k)}={repr(v)}')
        suffix = [')']
        return ''.join([*prefix, ', '.join(params), *suffix])


def main():
    msg = Message('A', 1, "2", 3, a=4, b=[1, '3', 5], c=6)
    print(msg)
    print(Message.load(msg.bytes()))


if __name__ == '__main__':
    main()
