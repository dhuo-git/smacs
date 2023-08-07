'''
smacs1/cons.py <-consumer.py 
    to be used by acons.py

major method:

    3.) receive SDU+CDU from producer (N104/4)
    4.) transmit CDU to producer (N6/106)
    5.) deliver received payloa SDU

TX-message: {'cdu':, 'sdu':} on N6, 
RX-message: {'cdu':, 'sdu':} on N4 

dependence: hub.py -fwd

5/2/2023/, laste update 7/14/2023
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
        print('state:',self.cst)
        #pprint.pprint( self.cst)


    def close(self):
        self.cst.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('sockets closed and context terminated')

    #producer state template: 'update' only needed for u-plane in combination with 'urst'
    def cst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'pt':[], 'new': True, 'crst': False, 'loop': True} #not used here, rather in acons.py
        c2p= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'ct':[], 'new': True, 'urst': False, 'update': False}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}


    #cdu to N6 (mode 1,3)
    def c2p_cdu13(self, seq, mseq, ct):
        return {'id': self.id, 'chan':self.conf['pub'][1], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'ct':ct}
    #cdu to N6 (mode 2)
    def c2p_cdu2(self, seq):
        return {'id': self.id, 'chan':self.conf['pub'][1], 'key': self.conf['key'], 'seq':seq}
    #-------------------------------------
    #device TX
    def transmit(self, rcdu, note, sdu = dict()):
        message = {'cdu': copy.deepcopy(rcdu), 'sdu': copy.deepcopy(sdu)}
        bstring = json.dumps(message)
        self.pub_socket.send_string("%d %s"% (rcdu['chan'], bstring)) 
        print(note, rcdu)
    def receive(self, note, c=True):
        bstring = self.sub_socket.recv()
        slst= bstring.split()
        sub_topic=json.loads(slst[0])
        messagedata =b''.join(slst[1:])
        message = json.loads(messagedata) 
        print(note, f' on N{sub_topic}:',  message)

        if c: return sub_topic, message['cdu']
        else: return sub_topic, message
    #
    def c_mode0(self):
        print("Error: mode 0 needs no cons.py")

    def c_mode1(self):                                          #N6/4
        while True: 
            sub_topic, cdu = self.receive('rx:')
            if sub_topic == self.conf['sub'][1]:                #N4
                if cdu['pt']:
                    cdu['pt'].append(time.time_ns())
                    if len(cdu['pt']) == 4:
                        self.cst['ctr']['pt'] = cdu['pt'].copy() 
                        self.cst['ctr']['mseq'] = cdu['mseq']
                        self.cst['ctr']['new'] = True
                    else:
                        self.cst['ctr']['pt'].clear()

                if cdu['seq'] > self.cst['c2p']['seq']:         #for N6
                    self.cst['c2p']['seq'] = cdu['seq']
                    self.cst['c2p']['new'] = True 

            if self.cst['c2p']['new']:                          #to N6
                self.cst['c2p']['new'] = False
                if len(self.cst['c2p']['ct']) == 2:
                    rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'])
                    rcdu['ct'].append(time.time_ns())
                    self.transmit(rcdu, 'tx c2p-N6:')
                else:
                    self.cst['c2p']['ct'].clear()
            time.sleep(self.conf['dly'])

    def c_mode2(self)->None:
        while True: 
            #receive sdu+cdu
            sub_topic, message = self.receive('rx:', False)
            if sub_topic == self.conf['sub'][1]:                       #from N4
                cdu = message['cdu']
                if cdu['seq'] > self.cst['c2p']['seq']:
                    self.cst['c2p']['seq'] = cdu['seq']
                    self.deliver_sdu(message['sdu']) #response on N6
                    rcdu = self.c2p_cdu2(self.cst['c2p']['seq'])
                    #transmit cdu
                    self.transmit(rcdu, 'tx:') 
            time.sleep(self.conf['dly'])

    def c_mode3(self)->None:
        while True: 
            sub_topic, message = self.receive('rx:', False) 
            cdu = message['cdu']
            print('rx:', sub_topic, message)
            if sub_topic == self.conf['sub'][1]:            #from N4
                if cdu['pt']:
                    cdu['pt'].append(time.time_ns())
                    if len(cdu['pt']) == 4:
                        self.cst['ctr']['pt'] = cdu['pt'].copy()
                        self.cst['ctr']['mseq'] = cdu['mseq']
                        self.cst['ctr']['new'] = True
                    else:
                        self.cst['ctr']['pt'].clear()

                if cdu['seq'] > self.cst['c2p']['seq']:
                    self.cst['c2p']['seq'] = cdu['seq']
                    self.cst['c2p']['new'] = True 
                    self.deliver_sdu(message['sdu'])

            if self.cst['c2p']['new']:  #transmit or not
                self.cst['c2p']['new'] = False
                rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'])
                if len(rcdu['ct'])== 2:
                    rcdu['ct'].append(time.time_ns())
                else:
                    rcdu['ct'].clear()
                self.transmit(rcdu, 'tx c2p N6:')
                #----------- refresh user-plane
                if self.cst['c2p']['update']:
                    self.cst['c2p']['update'] = False
                    if self.cst['c2p']['urst']:
                        self.conf['mode'] = 1               #turn sink off
                        self.cst['c2p']['seq'] = 0
                    else:
                        self.conf['mode'] = 3               #turn sink on
                #----------- refreshed
            time.sleep(self.conf['dly'])

    def c_run(self):
        print('mode :', self.conf['mode'])
        match self.conf['mode']:
            case 0: 
                self.c_mode0()
            case 1: 
                self.c_mode1()
            case 2: 
                thr=[Thread(target = self.sink), Thread(target=self.c_mode2)]
                for t in thr: t.start()
                for t in thr: t.join()
            case 3:
                thr=[Thread(target = self.sink), Thread(target=self.c_mode3)]
                for t in thr: t.start()
                for t in thr: t.join()
            case _:
                print('unknown mode')

    #----------------Cons-TX-RX ------------------
    def deliver_sdu(self, sdu):
        if self.conf['mode'] in [2,3] and len(self.subsdu) < self.subsdu.maxlen:
           self.subsdu.append(sdu)
        else:
            print('sdu receive buffer full or disabled')
    #------------------ User application  interface -------
    #deliver user payload 
    def sink(self):
        print('---- :', self.subsdu)
        while True:
            if self.subsdu:
                data = self.subsdu.popleft()
                print('delivered sdu', data)

# --------------TEST Consumer ---------------------------------------------
from contrtest import C_CONF as CONF 
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
        inst.c_run()
        inst.close()
    else: 
        print('usage: python3 cons.py mode (0,1,2,3,4)')
        print('usage: python3 cons.py -local mode (use local c.conf)')
        exit()
