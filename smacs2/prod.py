'''
smacs1/prod.py <- smacs1/producer.py 
    to be used by aprod.py
    receives on N6,  transmit on N5

major methods:
    
    3.) receive CDU from consumer (N106/6)
    4.) transmit CDU and/or SDU to consumer (N4)
    5.) generate test payload SDU

TX-message: {'cdu':, 'sdu':} on N4
RX-message: {'cdu':, 'sdu':} on N6
                
dpendence: hub.py -fwd

5/3/2023/nj, laste update 8/8/2023
'''
import zmq 
import time, sys,json, os, random, pprint, copy
from collections import deque
from threading import Thread
#==========================================================================
class Prod:
    def __init__(self, conf):
        self.conf = copy.deepcopy(conf)
        print('Producer:', self.conf)
        self.id = self.conf['key'][0]
        self.open()

    def open(self):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['hub_ip'], self.conf['pub'][0]))
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['hub_ip'], self.conf['sub'][0]))

        self.subtopics = [self.conf['sub'][1]]         #receive paths
        for sub_topic in self.subtopics:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(sub_topic))

        self.pubtopics =[self.conf['pub'][1]]      #controll channel first
        print('pub:', self.pubtopics, 'sub:', self.subtopics)


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

        self.srcsvr_socket = self.context.socket(zmq.PAIR)
        self.srcsvr_socket.setsockopt(zmq.RCVHWM,1)
        self.srcsvr_socket.setsockopt(zmq.LINGER,1)
        self.srcsvr_socket.bind("ipc://tmp/zmqtestp")

        #self.srcsvr_socket.bind("ipc:///tmp/zmqtestp")
        #self.srcsvr_socket.bind("tcp://localhost:{}"/format(self.conf['psrc_port']))

        self.srcclt_socket = self.context.socket(zmq.PAIR)
        self.srcclt_socket.setsockopt(zmq.SNDHWM,1)
        self.srcclt_socket.setsockopt(zmq.LINGER,1)
        self.srcclt_socket.connect("ipc://tmp/zmqtestp")

        #self.srcclt_socket.connect("ipc:///tmp/zmqtestp")
        #self.srcclt_socket.connect("tcp://localhost:{}"/format(self.conf['psrc_port']))


    def close(self):
        self.pst.clear()

        self.sub_socket.close()
        self.pub_socket.close()

        self.srcsvr_socket.close()
        self.srcclt_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    #--------------------------------------
    #producer state template
    def pst_template(self):
        ctr= {'loop': True}#'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'ct':[], 'new': True, 'crst': False, 'loop': True}#not used here, rather in aprod.py
        p2c= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'pt':[], 'urst': False, 'update': False, 'proto': 1, 'ready': True}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}


    #cdu to N4 (mode 1,3)
    def p2c_cdu13(self, seq, mseq, pt, proto): 
        return {'id': self.id, 'chan': self.conf['pub'][1], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'pt':pt, 'proto': proto}
    #cdu to N4 (mode 2)
    def p2c_cdu2(self, seq):
        return {'id': self.id, 'chan': self.conf['pub'][1], 'key': self.conf['key'], 'seq':seq, 'proto': 0}
    #------------------------------------
    #device TX
    def transmit(self, rcdu, note, sdu = dict()): 
        if rcdu['proto']:
            rcdu['pt'].append(time.time_ns())

        message = {'cdu': copy.deepcopy(rcdu), 'sdu': copy.deepcopy(sdu)}
        bstring = json.dumps(message)
        self.pub_socket.send_string("%d %s"% (rcdu['chan'], bstring)) 
        print(note, rcdu) 

    def receive(self, note):#device RX
        bstring = self.sub_socket.recv()
        slst= bstring.split()
        sub_topic=json.loads(slst[0])
        messagedata =b''.join(slst[1:])
        message = json.loads(messagedata) 
        cdu = message['cdu']
        if cdu['proto']:
            cdu['ct'].append(time.time_ns())
        print(note, message)
        return sub_topic, cdu
    """ """
    def pmode2(self):
        rcdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
        while True: 
            self.transmit(rcdu, 'tx p2c-N4:', self.get_sdu())   #N4 tx

            time.sleep(self.conf['dly'])

            sub_topic, cdu = self.receive('rx c2p N6:')
            if sub_topic == self.conf['sub'][1] and cdu:        #N6 rx
                if cdu['seq'] > self.pst['p2c']['seq']: 
                    self.pst['p2c']['seq'] = cdu['seq']
                rcdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
    """ """
    def pmode3(self)->None: 
        while self.pst['ctr']['loop']:                                  #N4 tx
            if self.p2c:
                self.pst['p2c']['mseq'], self.pst['p2c']['pt'], self.pst['p2c']['proto'] = self.p2c.popleft() 
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'].copy(), self.pst['p2c']['proto'])
            else:
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], [], self.pst['p2c']['proto'])

            self.transmit(rcdu, 'tx p2c on N4:', self.get_sdu())        #N4 tx

            #time.sleep(self.conf['dly'])

            sub_topic, cdu = self.receive('rx c2p on N6:')              #N6 rx

            if sub_topic == self.conf['sub'][1] and cdu: 
                if cdu['seq'] > self.pst['p2c']['seq']: 
                    self.pst['p2c']['seq'] = cdu['seq']
                if len(cdu['ct']) == 4: 
                    self.ctr.append([cdu['mseq'], cdu['ct'].copy(), cdu['proto']])
    """ """
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
            case 5:
                self.pmode3() #source external, requires 'esrc'=True, with port+2
            case _: 
                print('unknown mode in prod.py', self.conf['mode'])
                exit()

    #--------------------------------Prod-TX-RX------------------------
    def get_sdu(self):
        if self.conf['esrc']:
            tx = self.srcsvr_socket.recv_json()
            print('sent:', tx)
            return tx
        else:
            if self.pubsdu and self.conf['mode'] in [2,3]: 
                return self.pubsdu.popleft() #{'seq':self.seq, 'pld': self.pubsdu.popleft()}
            else:
                return dict()
    #----------------------User application interface ---------------
    #obain user payload
    def source(self):
        
        if self.conf['esrc']:
            while True:
                self.srcclt_socket.send_json({'seq': self.seq})
                self.seq+=1

        else:
            print('----:', self.pubsdu)
            a = deque(list('this-is-a-test'))
            while True: 
                if len(self.pubsdu) < self.pubsdu.maxlen: 
                    self.seq += 1
                    sdu = {'seq': self.seq, 'pld': a[0]} 
                    print('prepared sdu:', sdu)
                    self.pubsdu.append(sdu)
                    a.rotate(-1)
                else:
                    time.sleep(self.conf['dly'])
                    print('source buffer full')
    
#------------------ TEST Producer  -------------------------------------------
from rrcontr import P_CONF as CONF 
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
        print('usage: python3 prod.py mode (2 or 3 only)')
        print('usage: python3 prod.py -local mode (use local p.conf)')
        exit()

