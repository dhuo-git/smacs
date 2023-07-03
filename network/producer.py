'''
producer.py
    works with controller.py and consumer.py
    configured by CONF, or receive CONF from controller
    assumes medium.py (n106 instead of n6) or hub.py -fwd 
    receives on n0, n6
    transmit on channel n4,n5
major methods:
    
    1.) receive request by controller multicast (N0)
    2.) respond multi-cast (N5) 
    3.) receive CDU from consumer (N106/6)
    4.) transmit CDU and/or SDU to consumer (N4)
    5.) generate test payload SDU

Mode 2: Data only 
Mode 4: Test hub 
Mode 1: Measurement only
Mode 3: Measurement and Data
Mode 0: Configuration 

TX-message: {'cdu':, 'sdu':} for mode 0,1,3 on N5, N4/104
RX-message: {'cdu':, 'sdu':} for mode 0,1,3 on N106/6 or N0

5/3/2023/nj, laste update 7/3/2023
'''
import zmq 
import time, sys,json, os, random, pprint, copy
from collections import deque
from threading import Thread
#==========================================================================
class Producer:
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Produce:', self.conf)
        self.id = self.conf['key'][0]
        self.open()

    def open(self):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        self.subtopics = [self.conf['ctr_sub'], self.conf['u_sub']]         #receive paths
        for sub_topic in self.subtopics:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(sub_topic))
        self.pubtopics =[self.conf['ctr_pub'], self.conf['u_pub']]      #controll channel first
        print('pub:', self.pubtopics, 'sub:', self.subtopics)


        if self.conf['maxlen']:
           self.pubsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.pubsdu = deque([])

        self.seq = 0 #payload sequence number
        self.pst= self.pst_template()

        print('state:',self.pst)
        #pprint.pprint(self.pst)

    def close(self):
        self.pst.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    #--------------------------------------
    #producer state template
    def pst_template(self):
        ctr= {'chan': self.conf['ctr_pub'],'seq':0,'mseq': 0, 'ct':[], 'new': True, 'crst': False}
        p2c= {'chan': self.conf['u_pub'], 'seq':0, 'mseq':0,  'pt':[], 'new': True}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}
        
    #cdu to N5
    def ctr_cdu0(self, seq):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'key': self.conf['key'], 'seq':seq}
    #cdu to N5
    def ctr_cdu13(self, seq, mseq, ct):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'ct':ct}
    #cdu to N4
    def p2c_cdu13(self, seq, mseq, pt):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'pt':pt}
    #cdu to N4
    def p2c_cdu2(self, seq):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'seq':seq}
    #------------------------------------
    def run(self):
        if self.conf['mode'] == 0: 
            self.Mode0()
            thread = []#Thread(target=self.Mode0)]
        elif self.conf['mode'] == 2:
            thread = [Thread(target=self.Mode2Tx), Thread(target=self.Mode2Rx), Thread(target=self.source)]
        elif self.conf['mode'] == 1:
            thread = [Thread(target=self.Mode1Rx), Thread(target=self.Mode1Tx)]
        elif self.conf['mode'] == 3:
            thread = [Thread(target=self.Mode3Rx), Thread(target=self.Mode3Tx),  Thread(target=self.source)]
        elif self.conf['mode'] == 4:        #test
            self.Test()
            thread = []
        else:
            print('unknown mode in run', self.conf)
            return
        for t in thread: t.start()
        for t in thread: t.join()
    #device TX
    def transmit(self, rcdu, note, sdu = dict()):
        message = {'cdu': rcdu, 'sdu': sdu}
        bstring = json.dumps(message)
        self.pub_socket.send_string("%d %s"% (rcdu['chan'], bstring)) 
        print(note, rcdu)
    #device RX
    def receive(self, note):
        bstring = self.sub_socket.recv()
        slst= bstring.split()
        sub_topic=json.loads(slst[0])
        messagedata =b''.join(slst[1:])
        message = json.loads(messagedata) 
        cdu = message['cdu']
        print(note,sub_topic,  message)
        return sub_topic, cdu
    #operation modes 4,0,1,2,3
    def Test(self):
        print('mode Test')
        while True: 
            sub_topic, cdu = self.receive('rx:')
            self.pst['ctr']['seq'] =cdu['seq']
            rcdu = self.ctr_cdu0(self.pst['ctr']['seq'])
            self.transmit(rcdu, 'tx:')
            time.sleep(self.conf['dly'])

    #receive from permissible interfaces [N0]
    def Mode0(self):
        print('mode 0')
        while True: 
            sub_topic, cdu = self.receive('rx:')
            if sub_topic == self.conf['ctr_sub']:                       #N0
                if cdu['seq'] > self.pst['ctr']['seq']:  #from N0
                    self.pst['ctr']['seq'] = cdu['seq']
                    if cdu['conf']:
                        conf=cdu['conf'].copy()
                        self.conf = copy.deepcopy(conf['p'])
                        print('p', conf['p'])
                    else:   #if empty
                        print('no valid conf received', cdu['conf'])
                    #acknowledge any way
                    rcdu = self.ctr_cdu0(self.pst['ctr']['seq'])
                    self.transmit(rcdu, 'tx:')
                    if cdu['crst']:
                        self.pst['ctr']['seq'] = 0
                        print('producer reset and wait...') #print('new state', self.pst) 
            time.sleep(self.conf['dly'])

    #receive from permissible interfaces [N6]
    def Mode2Tx(self):
        print('mode 2')
        while True: 
            cdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
            self.transmit(cdu, 'tx:', self.get_sdu())
            time.sleep(self.conf['dly'])
    def Mode2Rx(self): 
        while True: 
            sub_topic,cdu = self.receive('rx:') 
            if sub_topic == self.conf['u_sub']:
                if cdu['seq'] > self.pst['p2c']['seq']:                                 #to N6
                    self.pst['p2c']['seq'] = cdu['seq']
            time.sleep(self.conf['dly'])
    '''
    #receive from permissible interfaces [N0, N6]
    #pst['ctr'] is tx-cdu-buffer for N5
    #pst['p2c'] is tx-cdu-buffer for N4
    #slot 1: rx N0, send to N4 (with just received pt), send to N5 (local update , receved from last slot 2)
    #slot 2: rx N6, send to N5 (with ct from slot 1), send to N4 (ack with local CDU)
    #2 slots, each with a SDU on N4, where slot 1 together with pt, slot 2 with local ack
    '''

    def Mode1Rx(self):
        print('mode 1 in producer', self.conf['mode'])
        while True: #slot 1
            sub_topic, cdu = self.receive('rx:')
            if sub_topic == self.conf['ctr_sub']:                           #prepare for N4
                #if cdu['mseq'] > self.pst['p2c']['mseq']:                   #prepare for N4 #if 1:
                if cdu['pt']:
                    cdu['pt'].append(time.time_ns())
                    if len(cdu['pt']) == 2:
                        self.pst['p2c']['pt'] =copy.deepcopy(cdu['pt'])
                        self.pst['p2c']['mseq'] = cdu['mseq']               #update p2c buffer with the received from N0
                        self.pst['p2c']['new'] = True
                if cdu['seq'] > self.pst['ctr']['seq']:                     #prepare for N5
                    if cdu['met']: 
                        self.adopt_met(cdu['met'])
                        self.pst['ctr']['seq'] = cdu['seq']                     #ack the receivd from N0
                        self.pst['ctr']['new'] = True                           
                        self.pst['ctr']['crst'] = cdu['crst']
                    print('rx ctr N0')#,cdu)

            if sub_topic == self.conf['u_sub']:                             #prepare for N5
                #if cdu['mseq'] > self.pst['ctr']['mseq']:  #
                if cdu['ct']:
                    cdu['ct'].append(time.time_ns())
                    if len(cdu['ct']) == 4:
                        self.pst['ctr']['ct'] = copy.deepcopy(cdu['ct'])
                        self.pst['ctr']['mseq'] = cdu['mseq']               #update ctr buffer with the received from N6
                        self.pst['ctr']['new'] = True                           #ack the received from N0
                    else:
                        self.pst['ctr']['ct'].clear()
                if cdu['seq'] > self.pst['p2c']['seq']:                     #prepare for N5
                    self.pst['p2c']['seq'] = cdu['seq']                      #ack the received from N6
                    self.pst['p2c']['new'] = True 
                print('rx p2c N6')#,cdu)


    def Mode1Tx(self): #slot 2
        print('mode 1 in producer ', self.conf['mode'])
        while True:

            if self.pst['p2c']['new']: #response on N4
                self.pst['p2c']['new'] = False 
                if len(self.pst['p2c']['pt']) == 2:
                    rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'])
                    rcdu['pt'].append(time.time_ns())

                    self.transmit(rcdu, 'tx p2c N4:')
                else:
                    self.pst['p2c']['pt'].clear()

            if self.pst['ctr']['new']: #response on N5, no SDU
                self.pst['ctr']['new'] = False 
                if len(self.pst['ctr']['ct']) == 4:
                    rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'])
                    rcdu['ct'].append(time.time_ns())

                    self.transmit(rcdu, 'tx ctr N5:')
                    if self.pst['ctr']['crst']:
                        self.pst['ctr']['seq'] = 0
                        print('producer reset and wait...', self.pst['ctr'])
                else:
                    self.pst['ctr']['ct'].clear()

    #---
    def Mode3Rx(self):
        print('mode 1 or 3 for ctr', self.conf['mode'])
        while True: #slot 1
            sub_topic, cdu =self.receive('rx ctr No:')
            if sub_topic == self.conf['ctr_sub']:                           #prepare for N4
                if cdu['pt']:
                    cdu['pt'].append(time.time_ns())
                    if len(cdu['pt']) == 2:
                        self.pst['p2c']['pt'] = cdu['pt'].copy() #copy.deepcopy(cdu['pt'])
                        self.pst['p2c']['mseq'] = cdu['mseq']               #update p2c buffer with the received from N0
                        self.pst['p2c']['new'] = True
                #local
                if cdu['seq'] > self.pst['ctr']['seq']:                     #prepare for N5
                    if cdu['met']: 
                        self.adopt_met(cdu['met'])
                        self.pst['ctr']['seq'] = cdu['seq']                     #ack the receivd from N0
                        self.pst['ctr']['new'] = True                           
                    if cdu['mode'] != self.conf['mode']: 
                        self.conf['mode'] = cdu['mode']
                        self.pst['ctr']['seq'] = cdu['seq']                     #ack the receivd from N0
                        self.pst['ctr']['new'] = True                           
                print('rx ctr N0:',cdu)

            if sub_topic == self.conf['u_sub']:                             #prepare for N5
                if cdu['ct']:
                    cdu['ct'].append(time.time_ns())
                    print(cdu)
                    if len(cdu['ct']) == 4:
                        self.pst['ctr']['ct'] = cdu['ct'].copy() #copy.deepcopy(cdu['ct'])
                        self.pst['ctr']['mseq'] = cdu['mseq']               #update ctr buffer with the received from N6
                        self.pst['ctr']['new'] = True                           #ack the received from N0
                    else:
                        self.pst['ctr']['ct'].clear()
                #local
                if cdu['seq'] > self.pst['p2c']['seq']:                     #prepare for N5
                    self.pst['p2c']['seq'] = cdu['seq']                      #ack the received from N6
                    self.pst['p2c']['new'] = True 
                print('rx p2c N6:',cdu)

            time.sleep(self.conf['dly'])

    #def Mode13TxN5(self): #slot 2
    def Mode3Tx(self): #slot 2
        print('mode 1 or 3 for ctr', self.conf['mode'])
        while True:
            if self.pst['ctr']['new']: #response on N5, no SDU
                self.pst['ctr']['new'] = False 
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'])
                if len(rcdu['ct']) == 4:
                    rcdu['ct'].append(time.time_ns())
                    self.transmit(rcdu, 'tx ctr N5:')
                else:
                    rcdu['ct'].clear()

            if self.pst['p2c']['new']: #response on N4
                self.pst['p2c']['new'] = False 
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'])
                if len(rcdu['pt']) == 2:
                    rcdu['pt'].append(time.time_ns())
                else:
                    rcdu['pt'].clear()
                self.transmit(rcdu, 'tx p2c N4:', self.get_sdu()) 

            time.sleep(self.conf['dly'])
    #--------------------------------Prod-TX-RX------------------------
    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation, skipped:', met)#self.ctr_state['met'])
            met.clear()
            return True
        else:
            print('unknown MET',met)
            return False

    def get_sdu(self): 
        if self.conf['mode'] not in [2,3]: 
            return dict()
        if self.pubsdu:
            return self.pubsdu.popleft() #{'seq':self.seq, 'pld': self.pubsdu.popleft()}
        else:
            return dict()

    #----------------------User application interface ---------------
    #obain user payload
    def source(self):
        print('----:', self.pubsdu)
        a = deque(list('this is a test'))
        while self.conf['mode'] in [2,3]:
            if len(self.pubsdu) < self.pubsdu.maxlen: 
                self.seq += 1
                sdu = {'seq': self.seq, 'pld': a[0]} #sdu = {'seq': self.seq, 'pld': random.choice(['p','r','o','d','u','c','e','r'])}
                print('prepared sdu:', sdu)
                self.pubsdu.append(sdu)
                a.rotate(-1)
            else:
                time.sleep(self.conf['dly'])
                print('source buffer full')
#------------------------------ TEST Producer  -------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'pub_port': "5568", 'key':[1, 2], 'dly':1., 'maxlen': 4, 'print': True, 'mode':0}
#CONF.update({'ctr_sub':0, 'ctr_pub':5, 'u_sub': 106, 'u_pub':4})
CONF.update({'ctr_sub':0, 'ctr_pub':5, 'u_sub': 6, 'u_pub':4})
#4 operation modes: ('u','ctr') =FF, FT,TF, TT =  00, 01, 10, 11 =0,1,2,3
#'key': (pid, cid)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        CONF['mode'] = int(sys.argv[1])
        #print('usage: python3 producer.py')
        #exit()
    print(sys.argv)
    inst=Producer(CONF)
    inst.run()
    inst.close()

