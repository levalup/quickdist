import threading

from quickdist.pyzmq.binding import Req

def hello():
    req = Req('localhost', 6606)
    req.socket.send_string('Hello')
    print(req.socket.recv_string())

def main():
    threads = [
        threading.Thread(target=hello)
        for _ in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
