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

5/3/2023/nj, laste update 7/14/2023
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

    def close(self):
        self.pst.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')
    #--------------------------------------
    #producer state template
    def pst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'ct':[], 'new': True, 'crst': False, 'loop': True}#not used here, rather in aprod.py
        p2c= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'pt':[], 'new': True, 'urst': False, 'update': False}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}


    #cdu to N4 (mode 1,3)
    def p2c_cdu13(self, seq, mseq, pt):
        return {'id': self.id, 'chan': self.conf['pub'][1], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'pt':pt}
    #cdu to N4 (mode 2)
    def p2c_cdu2(self, seq):
        return {'id': self.id, 'chan': self.conf['pub'][1], 'key': self.conf['key'], 'seq':seq}
    #------------------------------------
    #device TX
    def transmit(self, rcdu, note, sdu = dict()): 
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
        print(note,f'on N{sub_topic}:', message)
        return sub_topic, cdu

    def p_mode0(self):
        print("mode 0 needs no prod.py")

    def p_mode1(self)->None:                                            #N4/6
        while True: #slot 1
            sub_topic, cdu = self.receive('rx:')
            if sub_topic == self.conf['sub'][1]:                        #N6
                if cdu['ct']:
                    cdu['ct'].append(time.time_ns())
                    if len(cdu['ct']) == 4:
                        self.pst['ctr']['ct'] = cdu['ct'].copy()       
                        self.pst['ctr']['mseq'] = cdu['mseq']               
                        self.pst['ctr']['new'] = True                    
                    else:
                        self.pst['ctr']['ct'].clear()

                if cdu['seq'] > self.pst['p2c']['seq']:                  #for N4
                    self.pst['p2c']['seq'] = cdu['seq']                  #ack N6
                    self.pst['p2c']['new'] = True 

            if self.pst['p2c']['new']:                              #to N4
                self.pst['p2c']['new'] = False 
                if len(self.pst['p2c']['pt']) == 2:
                    rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'])
                    rcdu['pt'].append(time.time_ns())
                    self.transmit(rcdu, 'tx p2c N4:')
                else:
                    self.pst['p2c']['pt'].clear()
            time.sleep(self.conf['dly'])

    def p_mode2(self)->None:
        while True: 
            #transmit sdu+cdu
            cdu = self.p2c_cdu2(self.pst['p2c']['seq']+1)
            self.transmit(cdu, 'tx:', self.get_sdu())
            #receive cdu
            sub_topic,cdu = self.receive('rx:') 
            if sub_topic == self.conf['sub'][1]:
                if cdu['seq'] > self.pst['p2c']['seq']: #to N6
                    self.pst['p2c']['seq'] = cdu['seq']
            time.sleep(self.conf['dly'])

    def p_mode3(self)->None:
        while True:
            if self.pst['p2c']['new']:                          #to N4
                self.pst['p2c']['new'] = False 
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'])
                if len(rcdu['pt']) == 2:
                    rcdu['pt'].append(time.time_ns())
                else:
                    rcdu['pt'].clear()
                self.transmit(rcdu, 'tx p2c-N4:', self.get_sdu()) 
                #----------   refresh user plane
                if self.pst['p2c']['update']:
                    self.pst['p2c']['update'] = False
                    if self.pst['p2c']['urst']:
                        self.conf['mode'] = 1       #turn source off
                        self.pst['p2c']['seq'] = 0
                    else:
                        self.conf['mode'] = 3       #turn source on
                #----------- end user-plane refresh
            sub_topic, cdu = self.receive('rx:')
            if sub_topic == self.conf['sub'][1]:  #for N6
                if cdu['ct']:
                    cdu['ct'].append(time.time_ns())
                    if len(cdu['ct']) == 4:
                        self.pst['ctr']['ct'] = cdu['ct'].copy()
                        self.pst['ctr']['mseq'] = cdu['mseq']  
                        self.pst['ctr']['new'] = True 
                    else:
                        self.pst['ctr']['ct'].clear()

                if cdu['seq'] > self.pst['p2c']['seq']: 
                    self.pst['p2c']['seq'] = cdu['seq']
                    self.pst['p2c']['new'] = True 

            time.sleep(self.conf['dly'])

    def p_run(self):
        print('mode: ', self.conf['mode'])
        match self.conf['mode']:
            case 0: #moves to Slave 
                self.p_mode0()
            case 1:
                self.p_mode1()
            case 2: 
                thr =[Thread(target=self.source), Thread(target=self.p_mode2)]
                for t in thr: t.start()
                for t in thr: t.join()
            case 3:
                thr =[Thread(target=self.source), Thread(target=self.p_mode3)]
                for t in thr: t.start()
                for t in thr: t.join()
            case _: 
                print('unknown mode in prod.py')
                exit()

    #--------------------------------Prod-TX-RX------------------------
    def get_sdu(self): 
        if self.pubsdu and self.conf['mode'] in [2,3]: 
            return self.pubsdu.popleft() #{'seq':self.seq, 'pld': self.pubsdu.popleft()}
        else:
            return dict()
    #----------------------User application interface ---------------
    #obain user payload
    def source(self):
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
from contrtest import P_CONF as CONF 
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
        inst.p_run()
        inst.close()
    else:              
        print('usage: python3 prod.py mode (0,1,2,3,4)')
        print('usage: python3 prod.py -local mode (use local p.conf)')
        exit()

