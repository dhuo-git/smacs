"""
srcsnk.py 
    provides a test source and a sink in two independent threads to test SMACS3
    sends payload to Rprod.py via /temp/zmqtestp
    receives payload from Rcons.py via /tmp/zmqtestc

Run with Rprod.py 2/3 + Rcons.py 2/3, or rrRprod.py 2/3 + rrRcons.py 2/3

created on 8/16/2023/dhuo, updated on 8/17/2023
"""

import zmq
import time, sys,json, os, random, pprint, copy
from collections import deque
from threading import Thread
from rabbitRpc import close

class SrcSnk:
    def __init__(self, conf):
        self.conf = copy.deepcopy(conf)
        if not self.conf['conf']['p']['esrc'] or  not self.conf['conf']['c']['esnk']:
            print('wrong esrc', self.conf)
            exit()
        print('SrcSnk:', self.conf)
        self.open()
        self.seq = 0

    def open(self):
        self.context = zmq.Context()
        #if self.conf['maxlen']: self.subsdu = deque(maxlen=self.conf['maxlen'])
        #else: self.subsdu = deque([])
        #sink server receives delivery from Rcons.py
        self.snksvr_socket = self.context.socket(zmq.PAIR)
        self.snksvr_socket.bind("ipc://tmp/zmqtestc")

        #source cient sends payload to Rprod.py
        self.srcclt_socket = self.context.socket(zmq.PAIR)
        self.srcclt_socket.connect("ipc://tmp/zmqtestp")


    def close(self):
        self.snksvr_socket.close()
        self.srcclt_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    def source(self):#corresponds to source in Rprod.srcsvr
        #print('Prod.source buffer ---- :', self.pubsdu) #a = deque(list('this-is-a-test'))
        lyric=[" La donna è mobile", "Qual piuma al vento", "Muta d'accento", "E di pensiero.", "Sempre un a mabile", "Leggiadro viso", "In pianto o in riso", "è mensognero."]
        a = deque(lyric)
        while True: 
            sdu = {'seq': self.seq, 'pld': a[0]}    
            self.seq += 1
            a.rotate(-1)
            self.srcclt_socket.send_json(sdu)
            print('\n [x] srcsnk.source generated:', sdu)
            time.sleep(self.conf['dly'])

    def sink(self): #correspond to Rcons.py snkclt
        #print('Rcons.sink buffer ---- :', self.subsdu)
        while True:
            sdu = self.snksvr_socket.recv_json()
            print('[*] srcsnk.sink received :', sdu)

if __name__ == "__main__":
    from rrRcontr import CONF
    import sys

    try:
        s = SrcSnk(CONF)
        if '-srssnk' in sys.argv: 
            thr = [Thread(target=s.source), Thread(target=s.sink)]
            for t in thr: t.start()
            for t in thr: t.join()
        elif '-snk' in sys.argv:
            s.sink()
        elif '-src' in sys.argv:
            s.source()
        else: 
            print('usage: python3 srcsnk.py -src/snk/srcsnk')
    except KeyboardInterrupt:
        print('interrupted')
        close()
