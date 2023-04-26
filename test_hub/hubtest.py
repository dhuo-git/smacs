'''
hubtest.py derived from hub.py for the purpose of testing the hub (python hub.py -fwd) using pub,py and sub.py
3 cases are to be tested:

    case 0: pub/hub both loops (4*(sub-delay+pub-delay) step)
    case 1: sub side loop (sub-delay step), while pub side reconnects and stops with sub receives one. Return true
    case 2: pub side loop (pub-delay step), while sub side reconnects and stops when receives one. Return True
Principle:
    case 0: pub-buffer and sub-buffer are compared until their intersection is non-empty
    case 1: sub-buffer and pub-message are compared until their intersection is non-empty
    case 2: pub-buffer and sub-message are compared until their intersection is non-empty
Configuration:
    pub.CONF, sub.CONF
This serves the purpose of unitest.  7/8/2022/
'''
import sys, time
from  multiprocessing import Process
from threading import Thread
import pub, sub
#==========================================================================
class PubSub:
    def __init__(self, pconf, sconf, case=0):
        pconf['buffer'] = True
        sconf['buffer'] = True
        """ """
        self.pconf = pconf
        self.sconf = sconf

        self.case = case

    def pubsub(self):
        if self.case == 1: #sub running, pub connects and reconnects every 'dly' seconds
            self.sins = sub.Sub(self.sconf) #subscriber instance
            Thread(target=self.sins.subscriber, daemon=True).start()
            while True:
                self.pins = pub.Pub(self.pconf) #publisher instance
                self.pins.pubtest()
                self.pins.close() 
                self.pins.conf['dly']
                if len(self.sins.buffer) and isinstance(self.sins.buffer[0], dict): break
            self.dispose()
        elif self.case == 2: # pub running, sub connects and reconnects every 'dly' seconds
            self.pins = pub.Pub(self.pconf) #publisher instance
            Thread(target=self.pins.publisher, daemon=True).start()
            while True:
                self.sins = sub.Sub(self.sconf) #subscriber instance
                msg = self.sins.subtest()
                self.sins.close() 
                self.sins.conf['dly']
                if isinstance(self.pins.buffer, list) and msg in self.pins.buffer: break
            self.dispose()
        else:
            self.pins = pub.Pub(self.pconf) #publisher instance
            self.sins = sub.Sub(self.sconf) #subscriber instance
            thr =[Thread(target=self.sins.subscriber), Thread(target=self.pins.publisher)]
            for t in thr: t.start()
            time.sleep(4*(self.pins.conf['dly']+self.sins.conf['dly']))
            self.sins.sub_active=False
            self.pins.pub_active=False
            for t in thr: t.join()
            self.dispose()
        print('Server sent:\n', self.pins.buffer)
        print('Client received:\n', self.sins.buffer)
        return True
    def dispose(self):
        self.pins.close()
        self.sins.close()
#=============for docker ====================
AUTOGET = False
if AUTOGET:
    import socket
    ipv4 = socket.gethostbyname(socket.gethostname())
else:
    ipv4 = '192.168.1.11'
    ipv4 = '172.17.0.1' #host ip seen from inside the docker container
    ipv4 = '0.0.0.0'    #local test

pub.CONF['ipv4'] = ipv4
sub.CONF['ipv4'] = ipv4
#-------------------------------------------------------------------------
if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) == 2:
        test = PubSub(pub.CONF, sub.CONF,  int(sys.argv[1])) 
        test.pubsub()
        test.dispose()
    else:
        print('usage: python hubtest.py case[0,1,2]')
        print('case 0: pub/hub loops, case 1: pub-reconnect, case 2: sub-reconnect') #sys.stdout = open('sdo.txt', 'w')

    #sys.stdout.close() #with open('sdo.txt','r') as f: print(f.read())
