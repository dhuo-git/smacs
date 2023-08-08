'''
smacs1/cons.py <-consumer.py 
    to be used by rrcons.py
    release 2: introduced buffers

major method:

    3.) receive SDU+CDU from producer (N104/4)
    4.) transmit CDU to producer (N6/106)
    5.) deliver received payloa SDU

TX-message: {'cdu':, 'sdu':} on N6, 
RX-message: {'cdu':, 'sdu':} on N4 

dependence: hub.py -fwd

5/2/2023/, laste update 8/8/2023
'''
import zmq 
import sys, json, os, time, pprint, copy
from collections import deque
from threading import Thread #, Lock
#==========================================================================
class Cons:
    def __init__(self, conf):
        self.conf = copy.deepcopy(conf)
        print('Consumer:', self.conf)
        self.id = conf['key'][1]
        self.open()

    def open(self):
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['hub_ip'], self.conf['sub'][0]))
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['hub_ip'], self.conf['pub'][0]))

        self.subtopics = [self.conf['sub'][1]]
        for topic in self.subtopics: 
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
        self.pubtopics = [self.conf['pub'][1]]
        print('pub:', self.pubtopics, 'sub:', self.subtopics)


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

        #self.snksvr_socket.bind("ipc:///tmp/zmqtestc")
        #self.snksvr_socket.bind("tcp://localhost:{}".format(self.conf['csrc_port']))

        self.snkclt_socket = self.context.socket(zmq.PAIR)
        self.snkclt_socket.setsockopt(zmq.SNDHWM,1)
        self.snkclt_socket.setsockopt(zmq.LINGER,1)
        self.snkclt_socket.connect("ipc://tmp/zmqtestc")

        #self.snkclt_socket.connect("ipc:///tmp/zmqtestc")
        #self.snkclt_socket.connect("tcp://localhost:{}".format(self.conf['csrc_port']))

    def close(self):
        self.cst.clear()

        self.sub_socket.close()
        self.pub_socket.close()

        self.snksvr_socket.close()
        self.snkclt_socket.close()

        self.context.term()
        print('sockets closed and context terminated')

    #producer state template: 'update' only needed for u-plane in combination with 'urst'
    def cst_template(self):
        ctr= {'loop': True }#'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'pt':[],  'loop': True} #not used here, rather in acons.py
        c2p= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'ct':[], 'urst': False, 'update': False, 'proto':1, 'ready': True}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}


    #cdu to N6 (mode 1,3)
    def c2p_cdu13(self, seq, mseq, ct, proto):
        return {'id': self.id, 'chan':self.conf['pub'][1], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'ct':ct, 'proto': proto}
    #cdu to N6 (mode 2)
    def c2p_cdu2(self, seq):
        return {'id': self.id, 'chan':self.conf['pub'][1], 'key': self.conf['key'], 'seq':seq, 'proto': 0}
    #-------------------------------------
    #device TX
    def transmit(self, rcdu, note, sdu = dict()):
        if rcdu['proto']:
            rcdu['ct'].append(time.time_ns())

        message = {'cdu': rcdu.copy(), 'sdu': sdu.copy()}
        bstring = json.dumps(message)
        self.pub_socket.send_string("%d %s"% (rcdu['chan'], bstring)) 
        print(note, rcdu)

    def receive(self, note, contr = True):
        bstring = self.sub_socket.recv()
        slst= bstring.split()
        sub_topic=json.loads(slst[0])
        mstring=b''.join(slst[1:])
        message = json.loads(mstring) 

        if message['cdu']['proto']:
            message['cdu']['pt'].append(time.time_ns())

        if contr:   #c-plane
            print(note, message['cdu'])
            return sub_topic, message['cdu']
        else:       #c+u-plane
            print(note,  message)
            return sub_topic, message
    """ """
    def cmode2(self):
        while True: 
            sub_topic, message = self.receive('rx p2c-N4:', False) 
            cdu = message['cdu']
            if sub_topic == self.conf['sub'][1] and cdu:            #from N4
                if cdu['seq'] > self.cst['c2p']['seq']:
                    self.cst['c2p']['seq'] = cdu['seq'] 
                    self.deliver_sdu(message['sdu'])

                rcdu = self.c2p_cdu2(self.cst['c2p']['seq']) 
                self.transmit(rcdu, 'tx c2p N6:')
            time.sleep(self.conf['dly'])

    def cmode3(self)->None: 
        while self.cst['ctr']['loop']:
            sub_topic, message = self.receive('Cons rx c2p N4:', False) #N4 rx, step 2

            cdu = message['cdu']

            if sub_topic == self.conf['sub'][1] and cdu: #N4 rx
                if cdu['seq'] > self.cst['c2p']['seq']:
                    self.cst['c2p']['seq'] = cdu['seq'] 
                    self.deliver_sdu(message['sdu'])
                if len(cdu['pt']) == 4:  
                    self.ctr.append([cdu['mseq'], cdu['pt'].copy(), cdu['proto']])

                if self.c2p:                             #N6 tx
                    self.cst['c2p']['mseq'], self.cst['c2p']['ct'], self.cst['c2p']['proto'] = self.c2p.popleft() #if len(self.cst['c2p']['ct']) == 4:
                    rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'].copy(), self.cst['c2p']['proto'])
                else:
                    rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], [], self.cst['c2p']['proto'])

                self.transmit(rcdu, 'tx c2p N6:')

                #time.sleep(self.conf['dly'])
            """ """
    def c_server(self):
        print('mode :', self.conf['mode'])
        match self.conf['mode']:
            case 2:
                thr=[Thread(target = self.sink), Thread(target=self.cmode2)]
                for t in thr: t.start()
                for t in thr: t.join()
            case 3:
                thr=[Thread(target = self.sink), Thread(target=self.cmode3)]
                for t in thr: t.start()
                for t in thr: t.join() 
            case 5: 
                self.cmode3()	
            case _:
                print('only mode 2 is possible')

    #----------------Cons-TX-RX ------------------
    def deliver_sdu(self, sdu):
        if self.conf['esnk']:
            self.snkclt_socket.send_json(sdu)
            print('received:', sdu)
        else:
            if self.conf['mode'] in [2,3] and len(self.subsdu) < self.subsdu.maxlen:
               self.subsdu.append(sdu)
            else:
                print('sdu receive buffer full or disabled')
    #------------------ User application  interface -------
    #deliver user payload 
    def sink(self):
        if self.conf['esnk']:
            while True:
                rx = self.snksvr_socket.recv_json()
                print('received:', rx)
        else:
            print('---- :', self.subsdu)
            while True:
                if self.subsdu:
                    data = self.subsdu.popleft()
                    print('delivered sdu', data)

# --------------TEST Consumer ---------------------------------------------
#from contrtest import C_CONF as CONF 
from rrcontr import C_CONF as CONF 
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
