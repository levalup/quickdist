# -*- coding: utf-8 -*-
import os
import time

from quickdist.process import ProcessDistribute


def init():
    print(f'initialized {__file__} in pid={os.environ["PID"]}')


def main(*args, **kwargs):
    time.sleep(0.1)
    pid = int(os.environ.get('PID', 0))
    prefix = [f'main[{pid}](']
    params = []
    for v in args:
        params.append(repr(v))
    for k, v in kwargs.items():
        params.append(f'{str(k)}={repr(v)}')
    suffix = [')']
    msg = ''.join([*prefix, ', '.join(params), *suffix])
    print(msg)
    return msg


if __name__ == '__main__':
    with ProcessDistribute(__file__, 4) as pd:
        results = []
        for i in range(10):
            results.append(pd.call_async(1, '2, 3'))
        for result in results:
            print(result.get())
