'''
producer.py
    works with controller.py and consumer.py
    configured by CONF, or receive CONF from controller
    assumes medium.py, including hub.py -fwd, in background
    receives on channel n106, n105
    transmit on channel n4,n5
major methods:
    1.) transmit SDU+CDU to consumer (N4)
    2.) receive CDU from consumer (N6/N106)
    3.) transmit CDU to controller (N5)
    4.) receive CDU from controller (N105)
    5.) generate user traffic for test purpose

TX-message: {'id':,'chan':, 'cdu':} , {'id':, 'chan':,'cdu':, 'sdu':}
RX-message: {'id':,'chan':, 'cdu':} 

5/3/2023/nj, laste update 5/26/2023
'''
import zmq 
import time, sys,json, os, random
from collections import deque
from threading import Thread
#==========================================================================
class Producer:
    def __init__(self, conf):
        self.conf = conf.copy()
        self.id = self.conf['id']
        self.open()



        #self.started = False
        print('Produce:', self.conf)

    def open(self):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        self.subtopics = [self.conf['ctr_sub'], self.conf['u_sub']]         #receive paths
        for sub_topic in self.subtopics:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(sub_topic))
        self.pubtopics = [self.conf['rt'][str(key)] for key in self.subtopics]   #transmit paths

        if self.conf['maxlen']:
           self.pubsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.pubsdu = deque([])

        self.u = False 
        self.seq = 0 #payload sequence number
        self.ctr_state = self.ctr_template()
        self.u_cdu_state = self.u_cdu_template()
    #CDU to ctr: {'pid':, 'chan', 'peer':, 'pseq':, 'mseq':, 'ct123':[t1,t2,t3]}
    def ctr_template(self):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'peer': self.conf['peer'], 'pseq':0,'mseq':0,  'pt123':[], 'ct123':[]}
    #U-CDU to cons:{'id':, 'chan':, 'peer': , 'seq':, 'mseq':, 'pt123':[t1]}
    def u_cdu_template(self):
        return {'id': self.id, 'chan': self.conf['u_pub'],'peer': self.conf['peer'], 'seq':0, 'mseq':0, 'pt123':[]}
    #U-SDU, template not needed for the time being
    #def u_sdu_template(self): return {'id': self.id, 'chan': self.conf['u_pub'],'sdu': dict()}

    def close(self):
        self.pubsdu.clear()
        self.ctr_state.clear()
        self.u_cdu_state.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')

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
            if message:
                self.rx_handler(message, sub_topic)

            time.sleep(self.conf['dly'])

            print('received', message)

    #--------------------------------Prod-TX-RX------------------------
    #to controller: {'id':, 'chan':, 'cdu': {}}
    #to consumer: {'id', 'chan':, 'cdu':{}, 'sdu':{}}

    #preppare transmit message 
    def tx_handler(self,  pub_topic):
        tx = {'id': self.id,'chan': pub_topic}
        if pub_topic == self.conf['ctr_pub']: #n5
            tx['cdu'] = self.tx_ctrcdu()

        elif pub_topic == self.conf['u_pub'] and self.u: #n4
            tx['cdu'] = self.tx_ucdu()          #n4 CDU
            tx['sdu'] = self.tx_usdu()          #n4 SDU
        else:
            tx = {}
        if self.conf['print']: 
            print("Producer id={} sent {}".format(self.id,  tx))
        return tx

    #from ctroller: {'id':, 'chan':, 'cdu': {}}
    #from consumer: {'id':, 'chan':, 'cdu': {}}
    def rx_handler(self, message, sub_topic):
        if sub_topic == self.conf['ctr_sub']:   
            self.rx_ctrcdu(message['cdu'])      #n105 CDU
        if sub_topic == self.conf['u_sub']:    
            self.rx_ucdu(message['cdu'])        #n106 CDU 
                                                #n106 prod receives no SDU
        else:
            if self.conf['print']: 
                print('Producer id={} received for chan{} and buffered {} '.format(self.id, sub_topic, message))
#------------------Prod- Ctr ------------------------
#rx Ctr-CDU-------controlls pseq, but not mseq
#CDU from ctr: {'pid':, 'p-chan':,'peer':,  'pseq':, 'mseq':, 'adapt':{}}}
    def rx_ctrcdu(self, message):           #n105
        if message['pid'] == self.id and message['pseq'] > self.ctr_state['pseq']: 
            self.ctr_state['pseq'] =  message['pseq']

            self.u = message['u'] 
            #self.u = True

            if message['mseq'] > self.ctr_state['mseq']: #new measurement
                self.ctr_state['mseq'] = message['mseq']
                if isinstance(message['adapt'], dict): 
                    self.ctr_state['adapt'] = message['adapt']
                    self.update()                       #update prod
#tx Ctr-CDU
#CDU to ctr: {'pid':, 'chan', 'peer':, 'pseq':, 'mseq':,'ct123':[[t1,t2,t3]}
    def tx_ctrcdu(self):                        #n5
        if self.ctr_state['pseq'] == 0:#first
            ctr_cdu = self.ctr_state
            ctr_cdu['pseq'] += 1
        else:
            if len(self.ctr_state['ct123'])==2:
                self.ctr_state['ct123'].append(time.time_ns())
                ctr_cdu = self.ctr_state
                ctr_cdu.pop('pt123')
            else:
                ctr_cdu = {}
            self.ctr_state['ct123'].clear()  #refresh, reset seq to 0
        return ctr_cdu

    def update(self):                       #for the time being, clear memory
        print('fron Ctr received:', self.ctr_state['adapt'])
        if self.ctr_state['adapt']: self.ctr_state['adapt'].clear()
    #----------------------------Prod-U-CDU: transparent for mseq ----------------
    #U-CDU from cons:{'id':, 'chan':, 'peer':, 'seq':, 'mseq':, 'ct123':[t1]}
    #U-CDU to cons:{'id':, 'chan':, 'peer': , 'seq':, 'mseq':, 'pt123':[t1]}
    #SDU to cons:{'seq':, 'pld,:}}
    def rx_ucdu(self, message): 
        #if message['seq']== 0: print('from Cons, no new packet received', message) return
        if message['peer'] != self.id or not self.u: return          #received nothing

        self.u_cdu_state['seq'] = message['seq']
        self.u_cdu_state['mseq'] = message['mseq']    #to check with ctr_state

        if len(message['ct123'])==1 and self.str_state['mseq'] == message['mseq']:
            self.ctr_state['ct123'] = message['ct123']
            self.ctr_state['ct123'].append(time.time_ns())    #added receive time
        #prod receives no sdu, but transmit sdu (see below)
    #---------------------------U-TX-CDU------------------------
    def tx_ucdu(self):
        ucdu = self.u_cdu_state
        ucdu['pt123']=[time.time_ns()]
        ucdu['mseq'] = self.ctr_state['mseq']
        ucdu['seq'] += 1            #increment transmitted sequence number
        return  ucdu
    #------------------------- U-TX-SDU--------------------------
    def tx_usdu(self):
        if list(self.pubsdu):             #n4-sdu: queue
            sdu = {'seq': self.seq, 'pld': self.pubsdu.popleft()}
            self.seq += 1
        else:
            sdu = dict()
        return sdu
    def source(self):
        print('----:', self.pubsdu)
        while True:
            try:
                data = {'pld': random.choice(['p','r','o','d','u','c','e','r'])}
                if len(self.pubsdu) < self.pubsdu.maxlen: 
                    self.pubsdu.append(data)
            except: print('failed in generating content', data)
            finally:
                    time.sleep(self.conf['dly']) 
#------------------------------ TEST Producer  -------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'pub_port': "5568", 'id':1, 'peer': 2, 'dly':1., 'maxlen': 4, 'print': True}
CONF.update({'rt':{'105': 5, '106':4}, 'ctr_sub':105, 'ctr_pub':5, 'u_sub': 106, 'u_pub':4  })
CONF['rounds'] =10

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

