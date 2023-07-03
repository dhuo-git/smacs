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
5/2/2023/, laste update 5/30/2023
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
        self.id = conf['id']


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
        self.pubtopics = [self.conf['ctr_pub']]#[str(key)] for key in self.subtopics]

        if self.conf['maxlen']:
           self.subsdu = deque(maxlen=self.conf['maxlen'])
        else:
           self.subsdu = deque([])

        self.ctr_state = self.ctr_template()
        self.ucdu_state = self.ucdu_template()

        print('pub:', self.pubtopics)
        print('sub:', self.subtopics)
        print('ctr:', self.ctr_state)
        print('u:', self.ucdu_state)


    def close(self):
        self.subsdu.clear()
        self.ctr_state.clear()
        self.ucdu_state.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')

#CDU to ctr:
    def ctr_template(self):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'peer': self.conf['peer'],'cseq':0,'mseq':0, 'pt123':[], 'u': False}
    #U-CDU to prod:
    def ucdu_template(self):
        return {'id': self.id, 'chan': self.conf['u_pub'], 'seq':0, 'mseq':0,  'pt123':[]}



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
                self.rx_handler(message, sub_topic)
                print('Consumer received', message)

            time.sleep(self.conf['dly'])

 
    #----------------Cons-TX-RX ------------------
    #to controller: {'id':, 'chan':, 'cdu':{}}
    #to producer: {'id':, 'chan':, 'cdu':{}}

    def tx_handler(self, pub_topic):
        tx = {'id': self.id, 'chan': pub_topic}
        if pub_topic == self.conf['ctr_pub']:       #n7 if self.ctr_state['ctr']:
            tx['cdu'] = self.tx_ctrcdu()
        elif pub_topic == self.conf['u_pub'] and self.ctr_state['u']:       #n6
            tx['cdu'] = self.tx_ucdu()              #n6 CDU
            tx['cdu']['ct123'] =[tiime.time_ns()]
        else:
            tx = {}
        if self.conf['print']: 
            print("Consumer {} sent on channel {}: {}".format(self.id, pub_topic, tx))
        return tx

    #from controller {'id':, 'chan':, 'cdu':{}}
    #from producer: {'id':, 'chan':, 'cdu':{}, 'sdu':{}}
    def rx_handler(self, message, sub_topic ):
        if sub_topic == self.conf['ctr_sub']:  
            self.rx_ctrcdu(message['cdu'])      #n107 CDU
        elif sub_topic == self.conf['u_sub']:   
            self.rx_ucdu(message['cdu'])        #n104 CDU
            self.rx_usdu(message['sdu'])        #n104 SDU
        else:
            print('Consumer: received unknown sub_topic', sub_topic)
        if self.conf['print']: 
            print('{} sid={} received for chan{} and buffered {} '.format(self.conf['name'], self.id, sub_topic, message))
#------------------- Consumer: Ctr ------------controlls cseq, but not mseq
#CDU from Ctr:  {'cid', 'c-chan':, 'peer':, 'cseq':, 'mseq':,'adapt':{}}}
    def rx_ctrcdu(self, cdu):           #107
        print('ctr-rx: cdu', cdu)
        if cdu['cid'] == self.id and cdu['cseq'] > self.ctr_state['cseq']:
            self.ctr_state['cseq'] = cdu['cseq'] 

            if cdu['mseq'] > self.ctr_state['mseq']: 
                if cdu['adapt']:
                    self.ctr_state['adapt'] = cdu['adapt']
                    self.adaptation()                       #update cons
                    self.ctr_state['mseq'] = cdu['mseq']    #controller can choose not update adapt while still progress with measurment
            
            if cdu['u'] and not self.ctr_state['u']:
                self.ctr_state['u'] = cdu['u']
                self.pub_topics.append(self.conf['u_pub'])
                print('updated pub list:', self.pubtopics)
#CDU to Ctr: {'cid', 'chan':, 'peer':, 'cseq':, 'mseq':, 'pt123':[[t1,t2,t3]}}
    def tx_ctrcdu(self):                    #n7
        if len(self.ctr_state['pt123'])==2:
            self.ctr_state['pt123'].append(time.time_ns())
            ctr_cdu = self.ctr_state
            ctr_cdu['cseq'] += 1
        else:
            self.ctr_state['pt123'].clear()                #refresh, reset seq to 0 
            ctr_cdu =  self.ctr_state
        print('send ctr_cdu:', ctr_cdu)
        return ctr_cdu
#for the time being, clear cash
    def adaptation(self):
        print('adaptation should work here, currenlty skipped:', self.ctr_state['adapt'])
        self.ctr_state['adapt'].clear()

    #-------------------------Cons-U-CDU ----------------------------
    #U-CDU from prod:{'id':, 'chan':, 'peer':  'seq':, 'mseq':, 'ct123':[t1]}, 'sdu':{'seq':, 'pld:,}}}
    def rx_ucdu(self, ucdu):
        print('u-rx: cdu', ucdu)
        if ucdu['peer'] != self.id or not self.ctr_state['u']: return 

        if ucdu['seq'] > self.ucdu_state['seq']:

            if len(ucdu['pt123'])==1 and self.ctr_state['mseq'] == ucdu['mseq']:
                self.ctr_state['pt123'] = ucdu['pt123']
                self.ctr_state['pt123'].append(time.time_ns())    #added receive time
                self.ucdu_state['seq'] = ucdu['seq']
            else:
                print('u-rx no action for mseq', self.ctr_state['mseq'], ucdu['mseq'])
        else:
            print('u-rx no action for seq', self.ctr_state['seq'], ucdu['seq'])
    #-------------------------Cons-U-CDU ----------------------
    #U-CDU to prod:{'id':, 'chan':, 'peer':, 'seq':, 'mseq':, 'ct123':[t1]}}
    def tx_ucdu(self):
        ucdu = self.ucdu_state
        ucdu['ct123'] = [time.time_ns()]
        ucdu['mseq'] = self.ucdu_state['mseq']
        #ucdu['seq'] can only be modified by producer
        return ucdu
    #-------------------------------U-RX-SDU ------------------------
    #U-SDU from cons: {'seq':, 'pld':}
    def rx_usdu(self, message):
        if len(self.subsdu) < self.subsdu.maxlen:
            self.subsdu.append(message)
        else:
            print('consummer buffer full', self.subsdu)
    # no SDU to transmit to producer
    #------------------ User application  interface -------
    #deliver user payload 
    def sink(self):
        print('---- :', self.subsdu)
        data = {}
        while self.ctr_state['u']:
            try:
                if self.subsdu:
                    data = self.subsdu.popleft()
                else:
                    print('empty buffer')
            except: print('failed to read received content', data)
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
