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

5/3/2023/nj, laste update 5/30/2023
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

        self.id = self.conf['id']
        self.open()
        #self.started = False

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
        self.ctr_state = self.ctr_template()
        self.ucdu_state = self.ucdu_template()

        print('pub:', self.pubtopics)
        print('sub:', self.subtopics)
        print('ctr:', self.ctr_state)
        print('u:', self.ucdu_state)

    def close(self):
        self.pubsdu.clear()
        self.ctr_state.clear()
        self.ucdu_state.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    #--------------------------------------
    #CDU to ctr:
    def ctr_template(self):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'peer': self.conf['peer'], 'pseq':0,'mseq':0,  'ct123':[], 'u': False}
    #U-CDU to cons:
    def ucdu_template(self):
        return {'id': self.id, 'chan': self.conf['u_pub'],'peer': self.conf['peer'], 'seq':0, 'mseq':0, 'ct123':[]}
    #U-SDU to cons: 
    def u_sdu_template(self, pld = None): 
        return {'seq': self.seq, 'pld': pld}
    #------------------------------------
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
            if message and message['cdu']:
                self.rx_handler(message['cdu'], sub_topic)
                print('Producer received:', message)

            time.sleep(self.conf['dly'])


    #--------------------------------Prod-TX-RX------------------------
    #to controller: {'id':, 'chan':, 'cdu': {}}
    #to consumer: {'id', 'chan':, 'cdu':{}, 'sdu':{}}

    def tx_handler(self,  pub_topic):
        tx = {'id': self.id,'chan': pub_topic}
        if pub_topic == self.conf['ctr_pub']: #n5
            tx['cdu'] = self.tx_ctrcdu()

        elif pub_topic == self.conf['u_pub'] and self.ctr_state['u']: #n4
            tx['cdu'] = self.tx_ucdu()          #n4 CDU
            tx['cdu']['pt123'] = [time.time_ns()]

            tx['sdu'] = self.tx_usdu()          #n4 SDU
        else:
            tx = {}                             #transmit nothing
        if self.conf['print']: 
            print("Producer {} sent on channel {}:{}".format(self.id, pub_topic,  tx))
        return tx

    #from ctroller: {'id':, 'chan':, 'cdu': {}}
    #from consumer: {'id':, 'chan':, 'cdu': {}}
    def rx_handler(self, cdu, sub_topic):
        if sub_topic == self.conf['ctr_sub']:   
            self.rx_ctrcdu(cdu)      #n105 CDU
        elif sub_topic == self.conf['u_sub']:    
            self.rx_ucdu(cdu)        #n106 CDU 
                                                #n106 prod receives no SDU
        else:
            print('Producer: received unknown sub_topic', sub_topic)
        if self.conf['print']: 
            print(f'Producer id={self.id} received cdu on chan {sub_topic}', cdu)
#------------------Prod- Ctr ------------------------
#rx Ctr-CDU-------controlls pseq, but not mseq
#CDU from ctr: {'pid':, 'p-chan':,'peer':,  'pseq':, 'mseq':, 'adapt':{}}}
    def rx_ctrcdu(self, cdu):           #n105
        print('ctr_rx: cdu ', cdu)
        if cdu['pid'] == self.id and cdu['pseq'] > self.ctr_state['pseq']: 
            self.ctr_state['pseq'] =  cdu['pseq']

            if cdu['mseq'] > self.ctr_state['mseq']: 
                if cdu['adapt']:
                    self.ctr_state['adapt'] = cdu['adapt']
                    self.adaptation()                       #update prod
                    self.ctr_state['mseq'] = cdu['mseq']
                    print('updated adapt:', cdu['cdu'])

            if cdu['u'] and not self.ctr_state['u']:
                self.ctr_state['u'] = cdu['u'] 
                self.pubtopics.append(self.conf['u_pub'])
                print('updated pub list:', self.pubtopics)

#CDU to ctr: {'pid':, 'chan', 'peer':, 'pseq':, 'mseq':,'ct123':[t1,t2,t3]}
    def tx_ctrcdu(self):                        #n5
        if len(self.ctr_state['ct123'])==2:
            self.ctr_state['ct123'].append(time.time_ns())
            ctr_cdu = self.ctr_state
            ctr_cdu['pseq'] += 1
        else:
            self.ctr_state['ct123'].clear()  #refresh, do nothing
            ctr_cdu = self.ctr_state
        print('send:', ctr_cdu)
        return ctr_cdu
#for the time being, clear memory
    def adaptation(self):                       
        print('adaptation should work here, currently skipped:', self.ctr_state['adapt'])
        self.ctr_state['adapt'].clear()

    #----------------------------Prod-U-CDU ----------------
    #U-CDU from cons:{'id':, 'chan':, 'peer':, 'seq':, 'mseq':, 'ct123':[t1]}
    def rx_ucdu(self, ucdu): 
        print('u-rx: cdu', ucdu)
        if ucdu['peer'] != self.id or not self.ctr_state['u']: return          #received nothing

        if ucdu['seq'] > self.ucdu_state['seq']: #owner of seq updates seq value, not copy it #

            if len(ucdu['ct123'])==1 and self.str_state['mseq'] == ucdu['mseq']:
                self.ctr_state['ct123'] = ucdu['ct123']
                self.ctr_state['ct123'].append(time.time_ns())    #added receive time
                self.ucdu_state['seq'] = ucdu['seq']           #now Prod['seq'] = Con['seq']
            else:
                print('u-rx no action for mseq', self.ctr_state['mseq'], ucdu['mseq'])
        else:
            print('u-rx not action for seq', self.ctr_state['seq'], ucdu['seq'])
        #prod receives no sdu, but transmit sdu (see below)
    #---------------------------U-TX-CDU------------------------
    #U-CDU to cons:{'id':, 'chan':, 'peer': , 'seq':, 'mseq':, 'pt123':[t1]}
    def tx_ucdu(self):
        ucdu = self.ucdu_state
        ucdu['pt123']=[time.time_ns()]
        ucdu['mseq'] = self.ctr_state['mseq']
        ucdu['seq'] += 1            #increment transmitted sequence number
        return  ucdu
    #------------------------- U-TX-SDU--------------------------
    #U-SDU to cons:{'seq':, 'pld,:}}
    def tx_usdu(self):
        if self.pubsdu:             #n4-sdu: queue
            sdu = self.sdu_template(self.pubsdu.popleft())
            self.seq += 1
        else:
            sdu = dict()
        return sdu
    #----------------------User application interface ---------------
    #obain user payload
    def source(self):
        print('----:', self.pubsdu)
        while self.ctr_state['u']:
            try:
                data = {'seq': self.seq, 'pld': random.choice(['p','r','o','d','u','c','e','r'])}
                if len(self.pubsdu) < self.pubsdu.maxlen: 
                    self.pubsdu.append(data)
            except: print('failed in generating content', data)
            finally:
                time.sleep(self.conf['dly']) 
#------------------------------ TEST Producer  -------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'pub_port': "5568", 'id':1, 'peer': 2, 'dly':1., 'maxlen': 4, 'print': True}
CONF.update({'rt':{'105': 5, '106':4}, 'ctr_sub':105, 'ctr_pub':5, 'u_sub': 106, 'u_pub':4  })
#CONF['rounds'] =10

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

