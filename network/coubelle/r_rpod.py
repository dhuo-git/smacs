'''
producer.py
    works with controller.py and consumer.py
    configured by CONF, or receive CONF from controller
    assumes medium.py, including hub.py -fwd, in background
    receives on channel n106, n105
    transmit on channel n4,n5
major methods:
    1.) receive request by controller multicast (N0)
    2.) respond multi-cast (N5) 
    3.) receive CDU from consumer (N106/6)
    4.) transmit CDU and/or SDU to consumer (N4/104)
    5.) generate test payload SDU

TX-message: {'id':,'chan':, 'cdu':} for mode 0,1,3 on N5, {'id':, 'chan':,'cdu':, 'sdu':} for mode 2,3 on N4/104
RX-message: {'id':,'chan':, 'cdu':} for mode 0,1,3 on N106/6 or N0

5/3/2023/nj, laste update 6/5/2023
'''
import zmq 
import time, sys,json, os, random
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
        self.pubtopics =[self.conf['ctr_pub']]      #controll channel first


        if self.conf['maxlen']:
           self.pubsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.pubsdu = deque([])

        self.seq = 0 #payload sequence number
        self.pst= self.pst_template()

        print('pub:', self.pubtopics)
        print('sub:', self.subtopics)
        print('producer -state:', self.pst)

    def close(self):
        self.pst.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    #--------------------------------------
    #producer state template
    def pst_template(self):
        ctr= {'chan': self.conf['ctr_pub'],'seq':0,'mseq': 0, 'ct':[], 'val':False, 'ack': False}
        p2c= {'chan': self.conf['u_pub'], 'seq':0, 'mseq':0,  'pt':[],'val': False, 'ack': False}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}
        

    def ctr_cdu0(self, seq):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'key': self.conf['key'], 'seq':seq}
    #cdu send to N0
    def ctr_cdu13(self, seq, mseq, ct):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'ct':ct}
    #cdu send to N4
    def p2c_cdu13(self, seq, mseq, pt):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'pt':pt}
    def p2c_cdu2(self, seq):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'key': self.conf['key'], 'seq':seq}
    #------------------------------------
    def run(self):
        message = {'cdu':self.p2c_cdu2(0), 'sdu':[]}
        while True: 
            '''
            for pub_topic in self.pubtopics: #tx message
                message = self.tx_handler(pub_topic)
                print('tx', pub_topic, message)
                if message and  message['cdu']['chan'] == pub_topic: #can send both cdu and sdu
                    bstring = json.dumps(message)
                    self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 
            '''
            if message and message['cdu']:        
                print('Producer received:', message)
                self.process_handler(message['cdu'])
                pub_topic = message['cdu']['chan']
                bstring = json.dumps(message)
                self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 

            time.sleep(self.conf['dly'])

            bstring = self.sub_socket.recv()
            slst= bstring.split()
            sub_topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata) 
            self.rx_handler(message['cdu'], sub_topic)
    #--------------------------------Prod-TX-RX------------------------
    #to controller: {'id':, 'chan':, 'cdu': {}}
    #to consumer: {'id', 'chan':, 'cdu':{}, 'sdu':{}}


    def rx_handler(self, cdu, sub_topic):
        #RX
        if self.conf['mode'] in [0]:
            if sub_topic == self.conf['ctr_sub'] and  not self.pst['ctr']['ack'] and cdu['seq'] > self.pst['ctr']['seq']: #from N0
                self.pst['ctr']['ack'] = True

        elif self.conf['mode'] in [1,3]:
            if sub_topic == self.conf['ctr_sub'] and  not self.pst['ctr']['ack'] and (cdu['mseq'] > self.pst['ctr']['mseq'] or cdu['seq'] > self.pst['ctr']['seq']): #from N0
                self.pst['ctr']['ack'] = True
            if sub_topic == self.conf['u_sub'] and not self.pst['p2c']['ack'] and (cdu['seq'] > self.pst['p2c']['seq'] or cdu['mseq'] == self.pst['p2c']['mseq']): #from N6
                self.pst['p2c']['ack'] = True
        elif self.conf['mode'] in [2]:
            print('rx mode 0')
            if sub_topic == self.conf['u_sub'] and (not self.pst['p2c']['ack']) and (cdu['seq'] > self.pst['p2c']['seq']): #from N6
                self.pst['p2c']['ack'] = True
            else:
                print('no reception ', self.pst)
        else:
            print('unknown operation mode in RX', self.conf['mode'])
    #Process
    def process_handler(self, cdu):
        if self.conf['mode'] in [0]:# and sub_topic == self.conf['ctr_sub']:
            if self.pst['ctr']['ack'] and cdu['seq'] > self.pst['ctr']['seq']:  #from N0
                self.pst['ctr']['seq'] = cdu['seq']
                self.pst['ctr']['ack'] = True
                self.conf = cdu['conf']['p']
        elif self.conf['mode'] in [1,3]:
            if self.pst['ctr']['ack']:   #from N0
                if cdu['mseq'] > self.pst['ctr']['mseq']:
                    cdu['pt'].append(time.time_ns())
                    if len(cdu['pt']) == 2:
                        self.pst['p2c']['pt'] = cdu['pt']
                        self.pst['p2c']['mseq'] = cdu['mseq']

                        self.pst['ctr']['val'] = True
                        self.pst['ctr']['ack'] = False
                    else:
                        self.pst['ctr']['val'] = False
                if cdu['seq'] > self.pst['ctr']['seq']:
                    if cdu['met']:
                        self.adopt_met(cdu['met'])
                    if cdu['mode'] != self.conf['mode']:
                        self.conf['mode'] = cdu['mode']
                    self.pst['ctr']['seq'] = cdu['seq']
                    self.pst['ctr']['ack'] = False
                
            #if sub_topic == self.conf['p_sub'] and  
            if self.pst['p2c']['ack']:     #from N6
                if cdu['mseq'] == self.pst['p2c']['mseq']:
                    cdu['ct'].append(time.time_ns())
                    if len(cdu['ct']) == 4:
                        self.pst['ctr']['ct'] = cdu['ct']
                        self.pst['ctr']['mseq'] = cdu['mseq']

                        self.pst['p2c']['val'] = True
                        self.pst['p2c']['ack'] = False
                    else:
                        self.pst['p2c']['val'] = False
        elif self.conf['mode'] in [2]:
            self.pst['p2c']['seq'] = cdu['seq']
            self.pst['p2c']['ack'] = False

        else:
            print('unknown operation mode in Process', self.conf['mode'])
    #TX
    def tx_handler(self,  pub_topic):
        #cdu = {}
        #sdu = []
        if self.conf['mode'] in [0]:
            if not self.pst['ctr']['ack']:
                if pub_topic == self.conf['ctr_pub']: 
                    cdu = self.ctr_cdu0(self.pst['ctr']['seq'])
                    return {'cdu': cdu, 'sdu':[] }

        elif self.conf['mode'] in [1,3]:
            if pub_topic == self.conf['ctr_pub']:
                if not self.pst['ctr']['ack']: #to N5
                    if self.pst['ctr']['val']:
                        self.pst['ctr']['ct'].append(time.time_ns())
                        cdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'])
                        return {'cdu': cdu, 'sdu': []}

            if pub_topic == self.conf['u_pub']:
                if not self.pst['p2c']['ack']: #to N4
                    if self.pst['p2c']['val']:
                        self.pst['p2c']['pt'].append(time.time_ns())
                        cdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'])
                        return {'cdu': cdu, 'sdu': self.get_sdu()}
        elif self.conf['mode'] in [2]:
            if pub_topic == self.conf['u_pub']:
                print('tx mode 2',pub_topic, self.pst)
                if not self.pst['p2c']['ack']: #to N4
                    cdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
                    return {'cdu': cdu, 'sdu': self.get_sdu()}
        else:
            print('unknown operation mode in TX', self.conf['mode'])
        #return {'id': self.id,'chan': pub_topic, 'cdu': cdu, 'sdu': sdu}
        return {'cdu': {'chan': pub_topic}, 'sdu':[]}

    #auxilliary

    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation should work here, currently skipped:', met)#self.ctr_state['met'])
            met.clear()
            return True
        else:
            print('unknown MET',met)
            return False

    def get_sdu(self):
        return {'seq': self.seq, 'pld': random.choice(['p','r','o','d','u','c','e','r'])} #test call back
        
        if self.pubsdu:
            self.seq += 1
            return [self.seq, self.pubsdu.popleft()]
        else:
            return []
    #----------------------User application interface ---------------
    #obain user payload
    def source(self):
        print('----:', self.pubsdu)
        if self.conf['mode'] in [1,2,3]: active = False
        else: active = True

        while active: 
            #try:
            data = {'seq': self.seq, 'pld': random.choice(['p','r','o','d','u','c','e','r'])}
            if len(self.pubsdu) < self.pubsdu.maxlen: self.pubsdu.append(data)
            #except: print('failed in generating content', data)
            #finally:
            time.sleep(self.conf['dly']) 
#------------------------------ TEST Producer  -------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'pub_port': "5568", 'key':(1, 2), 'dly':1., 'maxlen': 4, 'print': True, 'mode':2}
CONF.update({'ctr_sub':0, 'ctr_pub':5, 'u_sub': 106, 'u_pub':4})
#4 operation modes: ('u','ctr') =FF, FT,TF, TT =  00, 01, 10, 11 =0,1,2,3
#'key': (pid, cid)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) > 1:
        print('usage: python3 producer.py')
        exit()
    inst=Producer(CONF)
    #inst.run()
    thread = [Thread(target=inst.run), Thread(target=inst.source)]
    for t in thread: t.start()
    for t in thread: t.join()
    inst.close()

