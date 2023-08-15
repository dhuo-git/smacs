'''
smacs3/Rcons.py <-smacs2/rrcons.py <-  smacs1/cons.py <-consumer.py 
    to be used by rrRcons.py, receives on N4 and transmit on N6
    release 3: based on rabbitRpc.py 

major method:

    3.) receive SDU+CDU from producer (N104/4)
    4.) transmit CDU to producer (N6/106)
    5.) deliver received payload SDU

TX-message: {'cdu':, 'sdu':} on N6, 
RX-message: {'cdu':, 'sdu':} on N4 

dependence: rabbitMQ

5/2/2023/, laste update 8/15/2023
'''
import zmq 
import sys, json, os, time, pprint, copy
from collections import deque
from threading import Thread #, Lock
from rabbitRpc import RpcServer
#==========================================================================
class Cons(RpcServer):
    def __init__(self, conf):
        super().__init__(conf['hub_ip'], conf['rtkey'])
        self.conf = copy.deepcopy(conf)
        print('Consumer:', self.conf)
        self.id = conf['key'][1]
        self.open()

    def open(self):
        self.context = zmq.Context()
        """ """
        if self.conf['maxlen']:
           self.subsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.subsdu = deque([])

        self.cst = self.cst_template()
        #pprint.pprint( self.cst)
        self.c2p = deque(maxlen=self.conf['maxlen'])
        self.ctr = deque(maxlen=self.conf['maxlen'])

        self.snksvr_socket = self.context.socket(zmq.PAIR)
        self.snksvr_socket.setsockopt(zmq.RCVHWM,1)
        self.snksvr_socket.setsockopt(zmq.LINGER,1)
        self.snksvr_socket.bind("ipc://tmp/zmqtestc")


        self.snkclt_socket = self.context.socket(zmq.PAIR)
        self.snkclt_socket.setsockopt(zmq.SNDHWM,1)
        self.snkclt_socket.setsockopt(zmq.LINGER,1)
        self.snkclt_socket.connect("ipc://tmp/zmqtestc")


    def close(self):
        self.cst.clear()
        """ """
        self.snksvr_socket.close()
        self.snkclt_socket.close()

        self.context.term()
        print('sockets closed and context terminated')

    #producer state template: 'update' only needed for u-plane in combination with 'urst'
    def cst_template(self):
        ctr= {'loop': True }#'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'pt':[],  'loop': True} #not used here, rather in acons.py
        c2p= {'rtkey': self.conf['rtkey'], 'seq':0, 'mseq':0,  'ct':[], 'urst': False, 'update': False, 'proto':1, 'ready': True}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}


    #cdu to N6 (mode 1,3)
    def c2p_cdu13(self, seq, mseq, ct, proto):
        return {'id': self.id, 'rtkey':self.conf['rtkey'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'ct':ct, 'proto': proto}
    #cdu to N6 (mode 2)
    def c2p_cdu2(self, seq):
        return {'id': self.id, 'rtkey':self.conf['rtkey'], 'key': self.conf['key'], 'seq':seq, 'proto': 0}
    #-------------------------------------
    """ """
    def svr_handler(self, rx_bytes:bytes)->bytes:
        match self.conf['mode']:
            case 2:
                rx = json.loads(rx_bytes.decode())
                print('cons receives:', rx)
                if 'cdu' in rx and 'sdu' in rx:
                    cdu, sdu = rx['cdu'], rx['sdu']
                    if cdu['seq'] > self.cst['c2p']['seq']:
                        self.cst['c2p']['seq'] = cdu['seq'] 
                        self.deliver_sdu(sdu)
                rcdu = self.c2p_cdu2(self.cst['c2p']['seq']) 
                tx = json.dumps({'cdu':rcdu, 'sdu': {}})
                print('cons sends:', tx)
                return tx.encode()

            case 3|1: #N4 rx
                rx = json.loads(rx_bytes.decode())
                print('cons receives:', rx)
                if 'cdu' in rx and 'sdu' in rx:
                    cdu, sdu = rx['cdu'],  rx['sdu']
                    if 'pt' in cdu:
                        cdu['pt'].append(time.time_ns())
                    if cdu['seq'] > self.cst['c2p']['seq']:
                        self.cst['c2p']['seq'] = cdu['seq'] 
                        self.deliver_sdu(sdu)
                    if 'pt' in cdu and len(cdu['pt']) == 4:  
                        self.ctr.append([cdu['mseq'], cdu['pt'].copy(), cdu['proto']])

                if self.c2p:                             #N6 tx
                    self.cst['c2p']['mseq'], self.cst['c2p']['ct'], self.cst['c2p']['proto'] = self.c2p.popleft() 
                    rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'].copy(), self.cst['c2p']['proto'])
                    rcdu['ct'].append(time.time_ns())
                else:
                    rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], [], self.cst['c2p']['proto'])
                tx = json.dumps({'cdu':rcdu, 'sdu': {}})
                print('cons sends:', tx)
                return tx.encode()

            #case 4: return super().svr_handler(rx_bytes)
            case _:
                print('only mode 2 is possible')
        """this code is for local tests """
    def c_server(self):
        print('mode :', self.conf['mode'])
        match self.conf['mode']:
            case 2:
                thr=[Thread(target = self.sink), Thread(target=self.call)]
                for t in thr: t.start()
                for t in thr: t.join()
            case 3:
                thr=[Thread(target = self.sink), Thread(target=self.call)]
                for t in thr: t.start()
                for t in thr: t.join() 
            #case 4: self.call()
            case _:
                print('only mode 2 and 3 possible', self.conf['mode'])

    #----------------Cons-TX-RX ------------------
    def deliver_sdu(self, sdu):
        if self.conf['mode'] in [2,3]:
            if self.conf['esnk']:
                self.snkclt_socket.send_json(sdu)
                print('cons delivered:', sdu)
            elif len(self.subsdu) < self.subsdu.maxlen:
                self.subsdu.append(sdu)
                print('cons delivered:', sdu)
            else:
                print('sdu receive buffer full or disabled')
    #------------------ User application  interface -------
    #deliver user payload 
    def sink(self):
        if self.conf['esnk']:
            while True:
                rx = self.snksvr_socket.recv_json()
                print('sink received:', rx)
        else:
            print('---- :', self.subsdu)
            while True:
                try: #if self.subsdu:
                    data = self.subsdu.popleft()
                except IndexError:
                    #print('sink buffer empty')
                    data = dict()
                finally:
                    #print('sink received:', data)
                    pass


# --------------TEST Consumer ---------------------------------------------
from rrRcontr import C_CONF as CONF 
if __name__ == "__main__":
    print(sys.argv)
    if '-local' in sys.argv and len(sys.argv) > 2:
        f =open('c.conf', 'r')
        file = f.read()
        conf = json.loads(file)
        print(conf)
        conf['mode'] = int(sys.argv[2])
        inst=Cons(conf) 
        inst.c_run()
        inst.close()
    elif len(sys.argv) > 1:
        CONF['mode'] = int(sys.argv[1])
        inst=Cons(CONF) 
        inst.c_server()
        inst.close()
    else: 
        print('usage: python3 cons.py mode (2 and 3 only)')
        print('usage: python3 cons.py -local mode (use local c.conf)')
        exit()
