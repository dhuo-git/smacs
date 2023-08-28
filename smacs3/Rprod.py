'''
smacs3/Rprod.py <- smacs2/rprod.py <- smacs1/prod.py <- network/producer.py 
    to be used by rrRprod.py,  receives on N6,  transmit on N4
    release 3: based on rabbitRpc.py

major methods:
    
    3.) receive CDU from consumer (N106/6)
    4.) transmit CDU and/or SDU to consumer (N4)
    5.) generate test payload SDU

TX-message: {'cdu':, 'sdu':} on N4
RX-message: {'cdu':, 'sdu':} on N6
                
dpendence: rabbitMQ

5/3/2023/nj, laste update 8/15/2023
'''
import zmq 
import time, sys,json, os, random, pprint, copy
from collections import deque
from threading import Thread
from rabbitRpc import RpcClient
#==========================================================================
class Prod(RpcClient):
    def __init__(self, conf):
        super().__init__(conf['hub_ip'], conf['rtkey'])

        self.conf = copy.deepcopy(conf)
        print('Producer:', self.conf)
        self.id = self.conf['key'][0]
        self.open()

    def open(self):
        self.context = zmq.Context()
        if self.conf['maxlen']:
           self.pubsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.pubsdu = deque([])

        self.seq = 0 #payload sequence number
        self.pst= self.pst_template()
        print('state:',self.pst)
        #pprint.pprint(self.pst)
        self.p2c = deque(maxlen=self.conf['maxlen'])
        self.ctr = deque(maxlen=self.conf['maxlen'])
        
        #generator receiver from source
        self.srcsvr_socket = self.context.socket(zmq.PAIR)
        #self.srcsvr_socket.setsockopt(zmq.RCVHWM,1)
        #self.srcsvr_socket.setsockopt(zmq.LINGER,1)
        self.srcsvr_socket.bind("ipc://tmp/zmqtestp")


    def close(self):
        self.pst.clear()

        self.srcsvr_socket.close()
        self.srcclt_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    #--------------------------------------
    #producer state template
    def pst_template(self):
        ctr= {'loop': True}#'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'ct':[], 'new': True, 'crst': False, 'loop': True}#not used here, rather in aprod.py
        p2c= {'rtkey': self.conf['rtkey'], 'seq':0, 'mseq':0,  'pt':[], 'urst': False, 'update': False, 'proto': 1}#, 'ready': True}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}


    #cdu to N4 (mode 1,3)
    def p2c_cdu13(self, seq, mseq, pt, proto): 
        return {'id': self.id, 'rtkey': self.conf['rtkey'], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'pt':pt, 'proto': proto}
    #cdu to N4 (mode 2)
    def p2c_cdu2(self, seq):
        return {'id': self.id, 'rtkey': self.conf['rtkey'], 'key': self.conf['key'], 'seq':seq, 'proto': 0}

    #------------------------------------
    #device TX
    """ """ 
    def pmode2(self):
        rcdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
        message = json.dumps(rcdu)
        while True:
            print('prod sends:', message)
            response = self.call(message.encode())
            print('prod receives:', response)
            rx = json.loads(response.decode())
            cdu = rx['cdu']
            if cdu:
                if cdu['seq'] > self.pst['p2c']['seq']: 
                    self.pst['p2c']['seq'] = cdu['seq']
            rcdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
            message = json.dumps({'cdu': rcdu, 'sdu':self.get_sdu()})

    def pmode3(self)->None: 
        while self.pst['ctr']['loop']:                                  #N4 tx
            if self.p2c:
                self.pst['p2c']['mseq'], self.pst['p2c']['pt'], self.pst['p2c']['proto'] = self.p2c.popleft() 
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'].copy(), self.pst['p2c']['proto'])
                rcdu['pt'].append(time.time_ns())                       #out-going time stamp
            else:
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], [], self.pst['p2c']['proto'])
            tx = json.dumps({'cdu':rcdu, 'sdu': self.get_sdu()})
            print('prod sends:', tx)
            message = tx.encode()
            response = self.call(message)

            rx = json.loads(response.decode())
            print('prod receives:', rx)
            cdu = rx['cdu'] 
            if cdu:
                cdu['ct'].append(time.time_ns())                        #in-coming time stamp
                if cdu['seq'] > self.pst['p2c']['seq']: 
                    self.pst['p2c']['seq'] = cdu['seq']
                if len(cdu['ct']) == 4: 
                    self.ctr.append([cdu['mseq'], cdu['ct'].copy(), cdu['proto']])
            time.sleep(self.conf['dly'])

    """p_client is only valid in this module, not in children  """
    def p_client(self):
        print('mode: ', self.conf['mode'])
        match self.conf['mode']:
            case 2:
                thr =[Thread(target=self.source), Thread(target=self.pmode2)]
                for t in thr: t.start()
                for t in thr: t.join()
            case 3:
                thr =[Thread(target=self.source), Thread(target=self.pmode3)]
                for t in thr: t.start()
                for t in thr: t.join()
            #3case 4: #test Rabbit message = self.clt_handler(None)#json.dumps(tx) while True: response = self.call(message) message = self.clt_handler(response)
            case _: 
                print('only mode 2 and 3 possible', self.conf['mode'])
                exit()

    #--------------------------------Prod-TX-RX------------------------
    def get_sdu(self):
        if self.conf['mode'] in [2,3] and  self.pubsdu:
            sdu = self.pubsdu.popleft() 
        else:
            sdu = dict()
        print('Prod.source send SDU:', sdu)
        return sdu
    #----------------------User application interface ---------------
    #obain user payload
    def source(self):
        print('Prod.source buffer ---- :', self.pubsdu) #a = deque(list('this-is-a-test'))
        lyric=[" La donna è mobile", "Qual piuma al vento", "Muta d'accento", "E di pensiero.", "Sempre un a mabile", "Leggiadro viso", "In pianto o in riso", "è mensognero."]
        a = deque(lyric)
        while True: 
            if len(self.pubsdu) < self.pubsdu.maxlen: 
                if self.conf['esrc']:
                    sdu  = self.srcsvr_socket.recv_json()
                else:
                    sdu = {'seq': self.seq, 'pld': a[0]}    
                    self.seq += 1
                    a.rotate(-1)
                self.pubsdu.append(sdu)
                print('Prod.source generated:', sdu)
            else:
                print('Prod.source send buffer full')
                time.sleep(self.conf['dly'])
    
#------------------ TEST Producer  -------------------------------------------
from rrRcontr import P_CONF as CONF 
if __name__ == "__main__":
    print(sys.argv)
    if '-local' in sys.argv and len(sys.argv) > 2:
        f =open('p.conf', 'r')
        file = f.read()
        conf = json.loads(file)
        print(conf)
        conf['mode'] = int(sys.argv[2])
        inst=Prod(conf)
        inst.p_run()
        inst.close()
    elif len(sys.argv) > 1:
        CONF['mode'] = int(sys.argv[1])
        inst=Prod(CONF)
        inst.p_client()
        inst.close()
    else:              
        print('usage: python3 Rprod.py mode (2 or 3 only)')
        print('usage: python3 Rprod.py -local mode (use local p.conf)')
        exit()

