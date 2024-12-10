# -*- coding: utf-8 -*-

import argparse
import json
import os.path
from typing import Dict

from quickdist.node import Node
from quickdist.file import quickdist_config_json


def serve(args: argparse.Namespace):
    port = args.port
    processes = args.processes
    node = Node(port=port, processes=processes)
    node.run()


def read_config_json() -> Dict:
    path = quickdist_config_json()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as _:
        return {}


def write_config_json(cfg: Dict):
    path = quickdist_config_json()
    root = os.path.dirname(path)
    if root:
        os.makedirs(root, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, indent=4)


def config(args: argparse.Namespace):
    key = str(args.key)
    value = args.value
    if not value:
        return
    value = str(value)

    if not os.path.exists(value):
        print(f'[ERROR] {value} not exists')
        exit(1)

    value = os.path.abspath(value)
    cfg = read_config_json()

    if key.lower() == 'temp':
        cfg['tempdir'] = value
        write_config_json(cfg)
        print(f'[INFO] updated {quickdist_config_json()}')
        return

    if key.lower() == 'local':
        cfg['localdir'] = value
        write_config_json(cfg)
        print(f'[INFO] updated {quickdist_config_json()}')
        return

    if key.lower() == 'origin':
        cfg['workdir'] = value
        write_config_json(cfg)
        print(f'[INFO] updated {quickdist_config_json()}')
        return

    if key.lower()[:7] == 'origin.':
        sep = key[8:].lower()
        if 'workdirs' in cfg:
            cfg['workdirs'][sep] = value
        else:
            cfg['workdirs'] = {sep: value}
        write_config_json(cfg)
        print(f'[INFO] updated {quickdist_config_json()}')
        return

    print(f'[ERROR] the config key should be be origin|local|temp|origin.*')
    exit(1)


def build_parser():
    parser = argparse.ArgumentParser(description="Config mount point setting and start serve in node")

    subparsers = parser.add_subparsers(dest='command', help='Subcommands')

    serve_parser = subparsers.add_parser('serve', help='Start the node service')
    serve_parser.add_argument('--port', type=int, default=8421, help='serve port')
    serve_parser.add_argument('-n', '--processes', type=int, help='serve port')
    serve_parser.set_defaults(func=serve)

    config_parser = subparsers.add_parser('config', help='Config mount point')
    config_parser.add_argument('key', type=str, help='Should be origin|local|temp|origin.*')
    config_parser.add_argument('value', type=str, help='Absolute path')
    config_parser.set_defaults(func=config)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_help()
        return

    args.func(args)


if __name__ == '__main__':
    main()
