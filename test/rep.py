import threading
import time

from quickdist.pyzmq.binding import MultiThreadRep

def copy(req: bytes) -> bytes:
    return req

def close(rep: MultiThreadRep, seconds: float):
    time.sleep(seconds)
    rep.close()


def main():
    rep = MultiThreadRep(port=6606, target=copy, threads=4)

    # threading.Thread(target=close, args=(rep, 3), daemon=True).start()

    rep.run()


if __name__ == '__main__':
    main()
