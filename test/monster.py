# -*- coding: utf-8 -*-

from quickdist.monster import Monster


def main():
    monster = Monster()
    monster.connect('localhost', port=8421)

    monster.setup('entry.py')

    results = []
    for i in range(10):
        results.append(monster.call_async(1, '2', 3))

    for r in results:
        r.wait()
        if r.successful():
            print(r.get())
        else:
            try:
                r.get()
            except Exception as e:
                print(e)


if __name__ == '__main__':
    monster = Monster()
    monster.connect('localhost', port=8421)

    monster.setup('entry.py')

    print(monster.call(1, '2', 3))
