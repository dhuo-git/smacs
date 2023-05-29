'''
consumer.py 
    contains two nested loops: receive loop over a transmit loop
    configured by CONF
    assume runing medium.py, including hub.py -fwd, in backgraound
major method:
    1.) receive SDU+CDU from producer: N4
    2.) transmit CDU to producer: N6
    3.) transmit CDU to controller: N7
    4.) receive CDU from controller: N107
    5.) deliver received SDU to sdio for test purpose
5/2/2023/, laste update 5/12/2023
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
        self.id = conf['id']
        self.open()

        self.subtopics = [self.conf['ctr_sub'], self.conf['u_sub']]
        self.pubtopics = [self.conf['rt'][str(key)] for key in self.subtopics]
        for topic in self.subtopics: 
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
        print('Consumer:', self.conf)
    def open(self):
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))

        if self.conf['maxlen']:
           self.subsdu = deque(maxlen=self.conf['maxlen'])
           self.ctrcdu = deque(maxlen=self.conf['maxlen'])
           #self.pld = deque(maxlen=self.conf['maxlen'])
        else:
           self.subsdu = deque([])
           self.ctrcdu = deque([])
           #self.pld = deque([])

        self.mseq = 0
        self.u = False
        self.ctr_state = self.ctr_template()
        self.u_cdu_state = self.u_cdu_template()

#CDU to ctr: {'cid', 'chan':, 'peer':, 'cseq':, 'mseq':, 'pt123':[[t1,t2,t3]}}
    def ctr_template(self):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'cseq':0,'mseq':0, 'pt123':[], 'ct123':[], 'peer': self.conf['peer']}

    #U-CDU to prod:{'id':, 'chan':, 'peer':, 'seq':, 'mseq':, 'ct123':[t1]}}
    def u_cdu_template(self):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'seq':0, 'mseq':0,  'ct123':[]}

    def close(self):
        self.pubsdu.clear()
        self.ctrcdu.clear()
        self.pld.clear()

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
 
    #----------------Cons-TX-RX ------------------
    #to producer/controller: {'id':, 'chan':, 'cdu':{}}
    def tx_handler(self, pub_topic):
        tx = {'id': self.id, 'chan': pub_topic}
        if pub_topic == self.conf['ctr_pub']:       #n7 if self.ctr_state['ctr']:
            tx['cdu'] = self.tx_ctrcdu()

        elif pub_topic == self.conf['u_pub'] and self.u:       #n6
            tx['cdu'] = self.tx_ucdu()              #n6 CDU
        else:
            tx={}
        if self.conf['print']: 
            print("Consumer id={} sent {}".format(self.id,  tx))
        return tx

    #from controller {'id':, 'chan':, 'cdu':{}}
    #from producer: {'id':, 'chan':, 'cdu':{}, 'sdu':{}}
    def rx_handler(self, message, sub_topic ):
        if sub_topic == self.conf['ctr_sub']:  
            self.rx_ctrcdu(message['cdu'])      #n107 CDU
        if sub_topic == self.conf['u_sub']:   
            self.rx_ucdu(message['cdu'])        #n104 CDU
            self.rx_usdu(message['sdu'])        #n104 SDU
        else:
            if self.conf['print']: 
                print('{} sid={} received for chan{} and buffered {} '.format(self.conf['name'], self.id, sub_topic, message))
#------------------- Consumer: Ctr ------------controlls cseq, but not mseq
#CDU from Ctr:  {'cid', 'c-chan':, 'peer':, 'cseq':, 'mseq':,'adapt':{}}}
    def rx_ctrcdu(self, message):           #107
        if message['cid'] == self.id and message['cseq'] > self.ctr_state['cseq']:
            self.ctr_state['cseq'] = message['cseq'] 

            self.u = message['u']
            #self.u = True

        if message['mseq'] > self.ctr_state['mseq']:
            self.ctr_state['mseq'] = message['mseq']
            if isinstance(message['adapt'], dict): 
                self.ctr_state['adapt'] = message['adapt']
                self.update()                       #update cons
#CDU to Ctr: {'cid', 'chan':, 'peer':, 'cseq':, 'mseq':, 'pt123':[[t1,t2,t3]}}
    def tx_ctrcdu(self):                    #n7
        if self.ctr_state['cseq'] == 0: #fist
            ctr_cdu = self.ctr_state
            ctr_cdu['cseq'] += 1
        else:
            if len(self.ctr_state['pt123'])==2:
                self.ctr_state['pt123'].append(time.time_ns())
                ctr_cdu = self.ctr_state
                ctr_cdu.pop('ct123')
            else:
                ctr_cdu = {}
            self.ctr_state['pt123'].clear()                #refresh, reset seq to 0 
        return ctr_cdu

    def update(self):
        print('fron Ctr received:', self.ctr_state['adapt'])
        if self.ctr_state['adapt']: self.ctr_state['adapt'].clear()
    #-------------------------Cons-U-CDU ----------------------------
    #U-CDU from prod:{'id':, 'chan':, 'peer':  'seq':, 'mseq':, 'ct123':[t1]}, 'sdu':{'seq':, 'pld:,}}}
    #U-CDU to prod:{'id':, 'chan':, 'peer':, 'seq':, 'mseq':, 'ct123':[t1]}}
    def rx_ucdu(self, message):
        if message['peer'] != self.id or not self.u: return 

        self.u_cdu_state['seq'] = message['seq']
        self.u_cdu_state['mseq'] = message['mseq']  #to check with ctr_state['mseq']

        #if message['seq'] == 0: print('from Pro:no new packet received', message)
        #    return
        if len(message['pt123'])==1 and self.ctr_state['mseq'] == message['mseq']:
            self.ctr_state['pt123'] = message['pt123']
            self.ctr_state['pt123'].append(time.time_ns())    #added receive time
    #-------------------------Cons-U-CDU ----------------------
    def tx_ucdu(self):
        ucdu = self.u_cdu_state
        ucdu['ct123'] = [time.time_ns()]
        ucdu['mseq'] = self.u_cdu_state['mseq']
        #ucdu['seq'] += 1 only producer can modify the sequence number
        return ucdu
    #-------------------------------U-RX-SDU ------------------------
    def rx_usdu(self, message):
        if len(self.subsdu) < self.subsdu.maxlen:
            self.subsdu.append(message)
    # no SDU to transmit on this channel to producer
    #SDU delivery
    def sink(self):
        print('---- :', self.subsdu)
        while True:
            try:
                print(f"Consumer output fifo for chan {key} of node {self.id} is empty", self.subsdu.popleft())
            except: print('failed to read received content')
            finally:
                time.sleep(self.conf['dly']) 

#-------------------------------------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'pub_port': "5568", 'id':2, 'peer': 1, 'dly':1., 'maxlen': 4,  'print': True}
CONF.update({'rt':{'104': 6, '107':7}, 'ctr_sub': 107, 'ctr_pub': 7, 'u_sub':104, 'u_pub':6 } )

if __name__ == "__main__":
    print(sys.argv)
    inst=Consumer(CONF) 
    #inst.run()
    thread = [Thread(target=inst.run), Thread(target=inst.sink)]
    for t in thread: t.start()
    for t in thread: t.join()
    inst.close()
