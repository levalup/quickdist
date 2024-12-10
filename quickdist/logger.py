# -*- coding: utf-8 -*-

import os
import sys
import logging
import random
import string
from datetime import datetime


__all__ = [
    'logger'
]


def create_logger(log_root: str, name: str = None, debug: bool = False) -> logging.Logger:
    now = datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    if name is not None:
        log = logging.getLogger(name)
        log.setLevel(level=logging.DEBUG)
    else:
        log = logging.Logger(suffix, level=logging.DEBUG)

    logging_format = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    ch = logging.StreamHandler(stream=sys.stdout)

    if not debug:
        ch.setLevel(logging.INFO)

    ch.setFormatter(logging_format)
    log.addHandler(ch)

    if log_root:
        try:
            os.makedirs(log_root, exist_ok=True)
            if os.path.exists(log_root):
                log_file = os.path.join(log_root, f'engine_{now}_{suffix}.log')

                fh = logging.FileHandler(log_file)
                fh.setLevel(logging.DEBUG)
                fh.setFormatter(logging_format)

                log.addHandler(fh)
        except Exception as _:
            pass

    return log


logger = create_logger('/var/log/quickdist', 'quickdist', debug=True)


def main():
    pass


if __name__ == '__main__':
    main()
