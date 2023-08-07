from threading import Thread
import time, sys
import socket
#import socketserver
MSGLEN = 4096

class MySocket:
    """demonstration class only
      - coded for clarity, not efficiency
    """

    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock

    def connect(self, host, port):
        self.sock.connect((host, port))

    def mysend(self, msg):
        totalsent = 0
        while totalsent < MSGLEN:
            sent = self.sock.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

    def myreceive(self):
        chunks = []
        bytes_recd = 0
        while bytes_recd < MSGLEN:
            chunk = self.sock.recv(min(MSGLEN - bytes_recd, 2048))
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        return b''.join(chunks)

if __name__== "__main__":
    s=MySocket()
    #s.connect('127.0.0.1', 5555)
    s.connect('', 5555)
    if '-svr' in sys.argv:
        print(s.myreceive())
    elif '-clt' in sys.argv:
        s.mysend('this is a test')
    else:

        thr=[Thread(target=s.mysend, args=(b'hello',)), Thread(target=s.myreceive)]
        for t in thr:
            t.start()
        for t in thr:
            t.join()
