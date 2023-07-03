'''
consumer.py 
    contains two nested loops: receive loop over a transmit loop
    configured by CONF
    assume runing medium.py, including hub.py -fwd, in backgraound
major method:
    1.) receive request CDU from controller via multicast: N0
    2.) respond multi-cast (N7)
    3.) receive SDU+CDU from producer (N104/4)
    4.) transmit CDU to producer (N6/106)
    5.) deliver received payloa SDU

TX-message: {'id':,'chan':, 'cdu':} for mode 0,1,3 on N7, {'id':, 'chan':,'cdu':, 'sdu':} for mode 2,3 on N6/106
RX-message: {'id':,'chan':, 'cdu':} for mode 0,1,3 on N104/4 or N0

5/2/2023/, laste update 6/5/2023
'''
import zmq 
import time
import sys, json, os
from collections import deque
from threading import Thread #, Lock
#==========================================================================
class Consumer:
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Consumer:', self.conf)
        self.id = conf['key'][1]
        self.open()

    def open(self):
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))

        self.subtopics = [self.conf['ctr_sub'], self.conf['u_sub']]
        for topic in self.subtopics: 
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))

        self.pubtopics = [self.conf['ctr_pub'], self.conf['u_pub']]

        if self.conf['maxlen']:
           self.subsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.subsdu = deque([])

        self.cst = self.cst_template()

        print('pub:', self.pubtopics)
        print('sub:', self.subtopics)
        print('ctr-state:', self.cst)


    def close(self):
        self.cst.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')

    #producer state template
    def cst_template(self):
        ctr= {'chan': self.conf['ctr_pub'],'seq':0,'mseq': 0, 'pt':[], 'val':False, 'ack': False}
        c2p= {'chan': self.conf['u_pub'], 'seq':0, 'mseq':0,  'ct':[],'val': False, 'ack': False}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}

    def ctr_cdu0(self, seq):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'key': self.conf['key'], 'seq':seq}
    #cdu send to N0
    def ctr_cdu13(self, seq, mseq, ct):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'pt':ct}
    #cdu send to N4
    def c2p_cdu13(self, seq, mseq, pt):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'ct':pt}

    def run(self):
        while True:
            for pub_topic in self.pubtopics: #tx message
                message = self.tx_handler(pub_topic)
                if message:
                    bstring = json.dumps(message)
                    self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 

            bstring = self.sub_socket.recv()
            slst= bstring.split()
            sub_topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata) 
            if message and message['cdu']:  #message contains cdu or cdu and sdu
                self.rx_handler(message['cdu'], sub_topic)
                print('Consumer received', message)
                self.process_handler(message['cdu'])

            time.sleep(self.conf['dly'])
    #----------------Cons-TX-RX ------------------
    #to controller: {'id':, 'chan':, 'cdu':{}}
    #to producer: {'id':, 'chan':, 'cdu':{}}


    #RX
    def rx_handler(self, cdu, sub_topic ):
        if self.conf['mode'] in [0]:
            if sub_topic == self.conf['ctr_sub'] and  not self.cst['ctr']['ack'] and cdu['seq'] > self.cst['ctr']['seq']: #from N0
                self.cst['ctr']['ack'] = True
        elif self.conf['mode'] in [1,3]:
            if sub_topic == self.conf['ctr_sub'] and  not self.cst['ctr']['ack'] and (cdu['mseq'] > self.cst['ctr']['mseq'] or cdu['seq'] > self.cst['ctr']['seq']): #from N0
                self.cst['ctr']['ack'] = True
            if sub_topic == self.conf['u_sub'] and not self.pst['c2p']['ack'] and (cdu['seq'] > self.cst['c2p']['seq'] or cdu['mseq'] == self.cst['c2p']['mseq']): #from N4
                self.cst['c2p']['ack'] = True
        elif self.conf['mode'] in [2]:
            if sub_topic == self.conf['u_sub'] and (not self.pst['c2p']['ack']) and (cdu['seq'] > self.cst['c2p']['seq']): #from N4
                self.cst['c2p']['ack'] = True
            else:
                print('no reception', self.cst)
        else:
            print('unknown operation mode in RX', self.conf['mode'])
    #Handler
    def process_handler(self, cdu, sdu):
        if self.conf['mode'] in [0]:# and sub_topic == self.conf['ctr_sub']:
            if self.pst['ctr']['ack'] and cdu['seq'] > self.pst['ctr']['seq']:  #from N0
                self.pst['ctr']['seq']= cdu['seq']
                self.pst['ctr']['ack'] = True
                self.conf = cdu['conf']['c']
        elif self.conf['mode'] in [1,3]:
            #if sub_topic == self.conf['ctr_sub'] and  
            if self.pst['ctr']['ack']:   #from N0
                if cdu['mseq'] > self.pst['ctr']['mseq']:
                    cdu['ct'].append(time.time_ns())
                    if len(cdu['ct']) == 2:
                        self.cst['c2p']['ct'] = cdu['ct']
                        self.cst['c2p']['mseq'] = cdu['mseq']

                        self.cst['ctr']['val'] = True
                        self.cst['ctr']['ack'] = False
                    else:
                        self.cst['ctr']['val'] = False
                if cdu['seq'] > self.pst['ctr']['seq']:
                    if cdu['met']:
                        self.adopt_met(cdu['met'])
                    if cdu['mode'] != self.conf['mode']:
                        self.conf['mode'] = cdu['mode']
                    self.cst['ctr']['seq'] = cdu['seq']
                    self.cst['ctr']['ack'] = False

            #if sub_topic == self.conf['u_sub'] and  
            if self.pst['c2p']['ack']:     #from N4
                if cdu['mseq'] == self.cst['c2p']['mseq']:
                    cdu['pt'].append(time.time_ns())
                    if len(cdu['pt']) == 4:
                        self.cst['ctr']['pt'] = cdu['pt']
                        self.pst['ctr']['mseq'] = cdu['mseq']

                        self.cst['c2p']['val'] = True
                        self.cst['c2p']['ack'] = False
                    else:
                        self.cst['c2p']['val'] = False
        elif self.conf['mode'] in [2]:
            self.cst['c2p']['seq'] = cdu['seq']
            self.deliver_sdu(sdu)
            self.cst['c2p']['ack'] = False
        else:
            print('unknown operation mode in Process', self.conf['mode'])
    #TX
    def tx_handler(self, pub_topic):
        cdu = {}
        sdu = []
        if self.conf['mode'] in [0] and not self.cst['ctr']['ack']:
            if pub_topic == self.conf['ctr_pub']:
                cdu = self.ctr_cdu0(self.cst['ctr']['seq'])

        elif self.conf['mode'] in [1,3]:
            if pub_topic == self.conf['ctr_pub'] and  not self.cst['ctr']['ack']: #to N7
                if self.cst['ctr']['val']:
                    self.cst['ctr']['ct'].append(time.time_ns())
                    cdu = self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], self.cst['ctr']['pt'])
            if pub_topic == self.conf['u_pub'] and not self.cst['c2p']['ack']: #to N6
                if self.cst['c2p']['val']:
                    self.cst['c2p']['pt'].append(time.time_ns())
                    cdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'])
        elif self.conf['mode'] in [2]:
            if pub_topic == self.conf['u_pub'] and not self.cst['c2p']['ack']: #to N6
                    cdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'])
                    cdu['seq'] = self.cst['c2p']['seq']
            else:
                print('no reception in cst', self.cst)
        else:
            print('unknown operation mode in TX', self.conf['mode'])
        return {'id': self.id,'chan': pub_topic, 'cdu': cdu, 'sdu': sdu}

    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation should work here, currently skipped:', met)
            met.clear()
            return True
        else:
            print('unknown MET',met)
            return False
    #-------------------------------U-RX-SDU ------------------------
    def deliver_sdu(self, sdu):
        if len(self.subsdu) < self.subsdu.maxlen:
           self.subsdu.append(sdu)
        else:
            print('buffer full, received no SDU')
    #------------------ User application  interface -------
    #deliver user payload 
    def sink(self):
        print('---- :', self.subsdu)
        data = {}
        active = self.conf['mode'] in [2,3]
        while active: 
            try:
                if self.subsdu:
                    data = self.subsdu.popleft()
                else:
                    print('sink buffer empty')
            except: print('failed to read received content', data)
            finally:
                time.sleep(self.conf['dly']) 

#-------------------------------------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'pub_port': "5568", 'key':(1,2), 'dly':1., 'maxlen': 4,  'print': True, 'mode': 2}
CONF.update({'ctr_sub': 0, 'ctr_pub': 7, 'u_sub':104, 'u_pub':6})
#4 operation modes: ('u','ctr') =FF, FT,TF, TT =  00, 01, 10, 11 =0,1,2,3
#'key': (pid,cid) where cid=id for consumer
if __name__ == "__main__":
    print(sys.argv)
    inst=Consumer(CONF) 
    #inst.run()
    thread = [Thread(target=inst.run), Thread(target=inst.sink)]
    for t in thread: t.start()
    for t in thread: t.join()
    inst.close()
