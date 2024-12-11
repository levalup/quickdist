# -*- coding: utf-8 -*-

from quickdist.node import Node


def main():
    node = Node(port=8421, processes=4)
    node.run()


if __name__ == '__main__':
    main()
