# -*- coding: utf-8 -*-
import os

from quickdist.file import WorkFile


def main():
    os.environ['WORKDIR_VIDEO'] = os.path.dirname(__file__)
    os.environ['LOCALDIR'] = '/tmp'
    os.environ['TEMPDIR'] = '/mount/nfs'

    f = WorkFile('file.py', 'video')
    print(f.origin)
    print(f.local)
    print(f.temp)

    print(f.md5)


if __name__ == '__main__':
    main()
