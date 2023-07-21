'''
controller.py
    transmits on N0,  receives on N5 (from Producer) and N7 (from Consumer),  configured by CONF,
prerequisites:
    mongod,  hub.py
operates as client:
    0.) in mode 4: test hub
    1.) in mode 0: retrieve conf from DB, if version matches required, synchronize with Prod and Cons: send conf on N0 and receive confirmation on N5/7
    2.) in mode 1, 3: send packet with 'seq' on N0 and receive packet with the same 'seq' on N5/7 (locoal protocol)
    3.) in mode 1, 3: send packet with 'mseq' on N0 and receive packet with the same 'mseq' on N5/7 (global protocol)
    4.) in mode 1, 3: evaluate d and o using returned pt and ct from P and C
    5.) in mode 1, 3: store met vector to DB and send met on N5/N7

TX-message: {'cdu':dict(), 'sdu': dict()}    on N0
RX-message: {'cdu':dict(), 'sdu': dict()}    on N5, N7
dependence: hub.py and MongoDB 
db-name: smacs
collection: state, conf 

5/3/2023/nj, laste update 7/21/2023, PoC
'''
import zmq 
import time, sys,json, os, pprint, copy
from collections import deque
from threading import Thread
from pymongo import MongoClient

client=MongoClient('localhost', 27017)
dbase = client['smacs'] 

#==========================================================================
class Controller:
    def __init__(self, conf=None):
        self.co_state = dbase['state']
        self.co_conf =  dbase['conf']
        #prepare for state storage in DB
        if conf['mode'] == 1:
            self.tag = get_tag(self.co_state, 'Experiment 1')
            print("mode 1 db tag", self.tag)
        elif conf['mode'] == 3:
            self.tag = get_tag(self.co_state, 'Experiment 2')
            print("mode 3 db tag", self.tag)
        else:
            print('no need of state storage for mode:', conf['mode'])


        self.conf = copy.deepcopy(conf)
        self.open()


    def open(self):
        print('Controller:')
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        self.id = self.conf['id']

        self.subtopics = [self.conf["ctr_subp"], self.conf["ctr_subc"]]     #n5,n7
        for sub_topic in self.subtopics:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(sub_topic))
        self.pubtopics = [self.conf['ctr_pub']]

        self.tmp = copy.deepcopy(self.conf)#self.template_tmp(self.conf['conf'])
        print(self.tmp)
        self.state = self.template_ctr()#self.conf)

        print('sub:', self.subtopics, 'pub:', self.pubtopics)

    def close(self):
        self.state.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('Controller sockets closed and context terminated')

    #packet for mode 0
    def cdu0(self, seq, conf=dict()):
        st = {'id':self.id, 'chan': self.conf['ctr_pub'], 'key':self.conf['key']}
        return {**st,'seq':seq, 'conf':copy.deepcopy(conf), 'crst':self.state['crst'], 'urst': self.state['urst']}

   #packet for mode 1 and 3 
    def cdu1(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': self.conf['ctr_pub'], 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[]}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode']}

    def cdu3(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': self.conf['ctr_pub'], 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[]}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode']}

    def cdu_test(self, seq):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'seq': seq, 'time':time.time()}

    #state register, converted to CDU by make_cdu(key)
    def template_ctr(self): 
        st ={'id':self.id,'chan':self.conf['ctr_pub'],'key':self.conf['key'],'seq':0,'mseq':0,'tseq':[0,0],'tmseq':[0,0],'loop':True,'sent':False,'ack':[True,True]}
        st.update({'ct':[],'pt':[],'met':{},'mode':self.conf['mode'], 'cnt':self.conf['cnt'], 'crst': False,'urst': False})
        print('state:', st) #pprint.pprint(st)
        return st
    #
    ''' 
    #key=(pi,cid): pair of producer and consumer of interest
    #mseq: sequence number of measurement
    #seq: sequence number on N0 for A, to controll reissue of measurement request
    #ct: stamps holder of right circle
    #pt: stamps holder of left circule
    #met: adaptation list
    #cnt: measurement counter maximum, no more than cnt consecutive measurements
    #tmseq: temporary mseq for multicast (global) (mseq for Producer, mseq for Consumer)
    #tseq: temporary seq for multicast (local), (seq for Producer, seq for Consumer)
    #ack: multi-cast acknowledgement state (ack for Producer, ack for Consumer)
    #conf: configuration object {'p': P-CONF, 'c': C-CONf}
    #crst: controll-plane reset indicator (F/T)
    #urst: user-plane reset indicator (F/T) from F to T and from T to F, based on conf['uperiod']
    '''
    def run(self): #respond-receive
        if self.conf['mode'] == 0:
            thread = [Thread(target=self.Mode0Tx), Thread(target=self.Mode0Rx)]
        elif self.conf['mode'] == 1:
            thread = [Thread(target=self.Mode1Tx), Thread(target=self.Mode1Rx)]
        elif self.conf['mode'] == 3:
            thread = [Thread(target=self.Mode3Tx), Thread(target=self.Mode3Rx)]
        elif self.conf['mode'] == 4:
            thread = [Thread(target=self.TestTx), Thread(target=self.TestRx)]
        else:
            print('mode invalid in controller', self.conf['mode'])
            return
        for t in thread: t.start()
        for t in thread: t.join()
        self.close()
    #basic TX device
    def transmit(self, cdu, note):
        message = {'cdu': copy.deepcopy(cdu)}
        bstring = json.dumps(message)
        self.pub_socket.send_string("%d %s"% (cdu['chan'], bstring)) 
        print(note, cdu) 
        self.state['ack']= [False, False]
    #basic RX device
    def receive(self, note):
        bstring = self.sub_socket.recv()
        slst= bstring.split()
        sub_topic=json.loads(slst[0])
        messagedata =b''.join(slst[1:])
        message = json.loads(messagedata) 
        cdu = message['cdu']
        print(note, cdu)
        return sub_topic, cdu
    #---Mode Test
    def TestTx(self):
        print('mode Test')
        while self.state['seq'] < self.conf['cnt']:#True: 
            cdu = self.cdu_test(self.state['seq']+1)
            self.transmit(cdu, 'test tx:')
            time.sleep(self.conf['dly'])
    def TestRx(self):
        while self.state['seq'] < self.conf['cnt']: 
            sub_topic, cdu = self.receive('test rx:')
            self.state['seq'] = cdu['seq']
            time.sleep(self.conf['dly'])
    #---Mode 0
    def Mode0Rx(self):
        while self.state['loop']:
            sub_topic, cdu = self.receive('rx:\n')
            if sub_topic == self.conf['ctr_subp']:
                if cdu['seq'] > self.state['seq']: 
                    self.state['tseq'][0] = cdu['seq']
                    self.state['ack'][0] = True
            elif sub_topic == self.conf['ctr_subc']:
                if cdu['seq'] > self.state['seq']: 
                    self.state['tseq'][1] = cdu['seq']
                    self.state['ack'][1] = True
        else:
            print('RX stopped')
            return

    def Mode0Tx(self):

        while self.state['loop']:
            if not (self.state['ack'][0] and self.state['ack'][1]):
                continue
            elif self.state['tseq'][0] == self.state['tseq'][1]:# and self.state['tseq'][0] > self.state['seq']: 

                self.state['seq'] = self.state['tseq'][0]
                if self.state['crst']:                                          #third step of hand-shake 
                    print('finally \n')
                    self.state['loop'] = False 
                    print('reset, leaving state:\n', self.state) 

                if self.state['sent']:                                          #second step of hand-shake: last update, to avoid intial state
                    self.conf = copy.deepcopy(tmp) #implement the change
                    self.state['crst'] = True   
                    cdu = self.cdu0(self.state['seq']+1, dict())

                    self.state['sent'] = False                                      #internal control
                    self.transmit(cdu, 'tx:\n')
                else:                   #not sent yet
                    tag = {"ver":self.conf['ver']+1}                                   #real deployment, need GUI for MongoDB to input new conf
                    #tag = {'ver':self.conf['ver']}                                      #for test only
                    doc = self.co_conf.find_one(tag)                                  #and check DB for the latest update
                    if doc:
                        print("got 'conf' from DB")
                        doc.pop('_id')                                             #drop MongoDB specific header #self.tmp = copy.deepcopy(self.conf)
                        tmp = copy.deepcopy(doc)
                    else:
                        print(f"no version {self.conf['ver']} found in DB, use current", self.conf)
                        tmp = copy.deepcopy(self.conf)

                    cdu = self.cdu0(self.state['seq']+1, tmp) 
                    self.transmit(cdu, 'tx:\n')
                    self.state['sent'] = True                                       #first step of hand-shake

                self.state['tseq'] = [0,0]
            time.sleep(self.conf['dly'])
        else:
            print('TX stopped')
            return
    #--- Mode 1
    def Mode1Rx(self):
        print('mode 1,3, Rx', self.state['mode'])
        while self.state['loop']: #True: 
            sub_topic, cdu = self.receive('rx cdu:')
            #RX-P
            if sub_topic == self.conf['ctr_subp']: #producer n5
                if cdu['seq'] > self.state['seq']:            #local
                    self.state['tseq'][0] = cdu['seq']
                    self.state['ack'][0]= True
                if cdu['mseq']> self.state['mseq']:             #measurement
                    cdu['ct'].append(time.time_ns())
                    self.state['ct'] = copy.deepcopy(cdu['ct'])
                    self.state['tmseq'][0] =  cdu['mseq']
                    self.state['ack'][0]= True
            #RX-C
            if sub_topic == self.conf['ctr_subc']:  #consumer on n7
                if cdu['seq'] > self.state['seq']:             #local protocol
                    self.state['tseq'][1] = cdu['seq']
                    self.state['ack'][1]= True
                if cdu['mseq']> self.state['mseq']:             #measurement 
                    cdu['pt'].append(time.time_ns())
                    self.state['pt'] = copy.deepcopy(cdu['pt'])
                    self.state['tmseq'][1] =  cdu['mseq']
                    self.state['ack'][1]= True
            time.sleep(self.conf['dly']) 
        else:
            print("\n RX stopped,  state at Rx", self.state)
            return 
    #handle receive CDU : RX+Handler
    def Mode1Tx(self):
        print('mode 1,3, Tx', self.state['mode'])
        while self.state['loop']: #True: #implement previous change request #handle multicast
            if not (self.state['ack'][0] and self.state['ack'][1]):
                continue
            elif self.state['tseq'][0] == self.state['tseq'][1]:          #accept acknoledgement
                self.state['seq'] = self.state['tseq'][0]
                if self.state['crst']:
                    self.state['loop'] = False
                    print('reset, leaving state:\n', self.state) 

            if self.state['tmseq'][0] == self.state['tmseq'][1]:
                self.state['mseq'] = self.state['tmseq'][0]
                if self.met():                                          #self.state['met'] filled with new
                    if self.state['mseq'] > self.conf['cnt']: 
                            self.state['crst'] = True #if self.state['sent']:
                    cdu = self.cdu1(self.state['seq']+1, self.state['mseq']+1)         #deliver met
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'post met tx:')
                    self.state['met'].clear()
                    self.state['tmseq'] = [0,0]
                else:
                    cdu = self.cdu1(self.state['seq'], self.state['mseq']+1)
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'priori met tx:')
                    self.state['tmseq'] = [0,0]
                print('next measurement', self.state['mseq'])
        else:
            print("\n TX stopped,  state at Tx", self.state)
            return 
    #Mode 3
    def Mode3Rx(self):
        print('mode 3, Rx', self.state['mode'])
        while self.state['loop']:
            sub_topic, cdu = self.receive('rx cdu:')
            #RX-P
            if sub_topic == self.conf['ctr_subp']: #producer n5
                if cdu['seq'] > self.state['seq']:            #local
                    self.state['tseq'][0] = cdu['seq']
                    self.state['ack'][0]= True
                if cdu['mseq']> self.state['mseq']:             #measurement
                    cdu['ct'].append(time.time_ns())
                    self.state['ct'] = cdu['ct'].copy() #copy.deepcopy(cdu['ct'])
                    self.state['tmseq'][0] =  cdu['mseq']
                    self.state['ack'][0]= True
            #RX-C
            if sub_topic == self.conf['ctr_subc']:  #consumer on n7
                if cdu['seq'] > self.state['seq']:             #local protocol
                    self.state['tseq'][1] = cdu['seq']
                    self.state['ack'][1]= True
                if cdu['mseq']> self.state['mseq']:             #measurement 
                    cdu['pt'].append(time.time_ns())
                    self.state['pt'] = cdu['pt'].copy() #copy.deepcopy(cdu['pt'])
                    self.state['tmseq'][1] =  cdu['mseq']
                    self.state['ack'][1]= True
            time.sleep(self.conf['dly'])
        else:
            print("\n RX stopped,  state at Rx", self.state)
            return 
    #handle receive CDU : RX+Handler
    def Mode3Tx(self):
        print('mode 3, Tx', self.state['mode'])
        if self.conf['uperiod']:
            utimer = time.time()

        while self.state['loop']:
            if not (self.state['ack'][0] and self.state['ack'][1]):
                continue
            elif self.state['tseq'][0] == self.state['tseq'][1]:          #accept acknoledgement
                self.state['seq'] = self.state['tseq'][0]
                if self.state['crst']:
                    self.state['loop'] = False
                    print('reset, leaving state:\n', self.state) 

            if self.state['tmseq'][0] == self.state['tmseq'][1]:
                self.state['mseq'] = self.state['tmseq'][0]
                if self.met():                                          #self.state['met'] filled with new
                    if self.state['mseq'] > self.conf['cnt']:
                        self.state['crst'] = True
                    cdu = self.cdu3(self.state['seq']+1, self.state['mseq']+1)#, self.state['crst'],self.state['urst'])

                    # for u-plane only
                    if self.conf['uperiod'] and time.time() > utimer:
                        utimer += self.conf['uperiod']
                        self.state['urst'] = not self.state['urst']
                        cdu['urst'] = self.state['urst']
                    #u-plane end

                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'post met tx:')
                    self.state['met'].clear()
                    self.state['tmseq'] = [0,0]
                else:
                    cdu = self.cdu3(self.state['seq'], self.state['mseq']+1)
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'priori met tx:')
                    self.state['tmseq'] = [0,0]

                print('next measurement', self.state['mseq'])
        else:
            print("\n TX stopped,  state at Tx", self.state)
            return 

    #compute measurement, estimation and tracking
    def met(self):

        if len(self.state['pt'])==6 and len(self.state['ct'])==6:
            pc = [self.state['pt'][3] - self.state['pt'][2], self.state['ct'][3] - self.state['ct'][2]]

            l = [self.state['pt'][1]-self.state['pt'][0],  self.state['pt'][5]-self.state['pt'][4]]
            r = [self.state['ct'][1]-self.state['ct'][0],  self.state['ct'][5]-self.state['ct'][4]]
            
            self.state['met']={'m-offset':(pc[0]-pc[1])/2.0,'m-delay':(pc[0]+pc[1])/2.0,'l-offset':(l[0]-r[1])/2.0,'l-delay':(l[0]+r[1])/2.0,'r-offset':(r[0]- l[1])/2.0,'r-delay':(r[0]+l[1])/2.0}
            #stored to DB the measurement states for producer and consumer 
            if add_data(self.co_state, self.tag, self.state['met']):
                print('computed and saved')
                return True
            else:
                print('computed but not saved')
                return True
        else:
            return False
#-----------
#get tag for state data in DB
def get_tag(col, mark=None):
    if mark:
        tag = {'tag': mark}
    else:
        tag = {'tag': 'default'}# tag = {'mark': time.ctime()[4:]}

    print('input filter tag', tag)

    rec=col.find_one(tag)
    if rec and isinstance(rec['data'], list):
        tag = {"_id": rec["_id"]}
        print('found', rec["_id"])
    else:
        rst=col.insert_one({**tag, 'data':[]})
        tag = {"_id":rst.inserted_id}
        print('inserted id', rst.inserted_id)
    print('new tag', tag)
    return tag
#add state data to DB
def add_data(col, tag, entry):
    doc = col.find_one(tag)
    if doc and isinstance(doc['data'], list):
        doc['data'].append(entry)
        rst=col.replace_one(tag, doc) #rst=col.update_one(tag, doc)
        print('modified ', rst.modified_count)
        return True
    else:
        doc = {**tag, 'data': entry}
        rst = col.insert_one(doc)
        print('saved with', rst.inserted_id, rest.acknowledged)
        return True 
    return False
    
#save given conf in DB as version v, default v=0
def update_conf_db(conf, v=0):
    col= dbase['conf']
    #v = max(v, conf['ver'])
    doc =  col.find_one({'ver':v}) 
    if doc:
        #conf['ver'] = doc['ver'] +1
        rst = col.replace_one(doc, conf)
        if rst.modified_count:
            pprint.pprint(conf)
            print('version updated:', rst.modified_count)
        else:
            print('failed to update', doc)
            return False
    else:                                   #add new entry with new _id
        conf['ver'] = v
        rst = col.insert_one(conf)
        if rst.acknowledged:
            pprint.pprint(col.find_one({'ver':conf['ver']}))
            print("conf saved:", rst.acknowledged)
        else: 
            print('failed to save conf')
            return False
        return True

    print("collections:", dbase.list_collection_names())
#conf passed as reference
def set_mode(conf, m):
    conf['mode'] = m
    conf['pc_conf']['p']['mode'] = m
    conf['pc_conf']['c']['mode'] = m
    print('CONF mode set to ', m)
def test_db():
    col =  dbase['test']
    tag = get_tag(col, "Experiment 11")
    rec=col.find_one(tag)
    doc={**tag, 'data':[]}
    if not rec:
        print('not found', rec)
        rst=col.insert_one(doc)
        print('inserted', rst.inserted_id)
        tag = {"_id":rst.inserted_id}
    else:
        tag ={"_id": doc["_id"]}
        print('found', rec)
    print('new tag', tag)

    print('tag', tag)
    doc1 =col.find_one(tag)
    print('doc,doc1',doc, doc1)
    if isinstance(doc1['data'], list):
        for i in range(3):
            doc1['data']+=list(range(5))
            rst1 = col.replace_one(tag, doc1)
            print('modeified', rst1.modified_count)
            doc2= col.find_one(tag)
            print('updated:',doc2)
#------------------------------ TEST -------------------------------------------
ipv4= "127.0.0.1" 
#ipv4= "192.168.1.204"               #system76
#ipv4= "192.168.1.99"               #lenovo P21
#ipv4= "192.168.1.37"
P_CONF = {'ipv4':ipv4, 'sub_port': "5570", 'pub_port': "5568", 'key':[1, 2], 'dly':0., 'maxlen': 4, 'print': True, 'mode': 0}
P_CONF.update({'ctr_sub': 0, 'ctr_pub': 5, 'u_sub': 6, 'u_pub': 4})
C_CONF = {'ipv4':ipv4 , 'sub_port': "5570", 'pub_port': "5568", 'key':[1,2], 'dly':0., 'maxlen': 4,  'print': True, 'mode': 0}
C_CONF.update({'ctr_sub': 0, 'ctr_pub': 7, 'u_sub': 4, 'u_pub': 6})

#from producer import CONF as P_CONF
#from consumer import CONF as C_CONF

CONF = {"ipv4":ipv4 , "sub_port": "5570", "pub_port": "5568", "id":0, "dly":0, "print": True, "ver": 0,  'cnt':20, "mode":0, "uperiod": 0}
CONF.update({"ctr_pub": 0,  "ctr_subp": 5, "ctr_subc":7, "key":[1,2], 'pc_conf':{'p': P_CONF, 'c':C_CONF}})

#ctr_pub: multicast interface
#ctr_subp: multicast reverse channel of producer
#ctr_subc: multicast reverse channel of consumer
#key: (pid, cid) should be idential to that in producer.py and consumer.py, i.e. P_CONF and C_CONF
#ver: configuration version, to be modified by GUT on DB to trigger update in mode 0
#cnt: count number on N0 for retransmit measurement request
#mode: operation mode 0,1,2,3,4(test)
#uperiod: periods length (seconds) for u-plane flip-flop (experiment), 0 disables this feature

if __name__ == "__main__":
    #test DB
    if '-testdb' in sys.argv:
        test_db()
        exit()    
    #prepare configuration entry in DB
    if '-prepdb' in sys.argv:
        set_mode(CONF, 0)
        if len(sys.argv) <3:
            print('usage: python3 controller.py -prepdb version (default 0)')
            update_conf_db(CONF)
        else:
            update_conf_db(CONF, int(sys.argv[2]))
        exit()
    if len(sys.argv) > 1:
        CONF['mode'] = int(sys.argv[1])
        print(sys.argv)
        inst=Controller(CONF)
        inst.run()
        inst.close()
    elif len(sys.argv) > 2:
        print(sys.argv)
        print('usage: python3 controller.py (mode 0, 1,2,3,4, or default mode 0')
        print('usage: python3 controller.py -testdb/prepdb')
        exit()
    else:                       #default mode 0
        print(sys.argv)
        inst=Controller(CONF)
        inst.run()
        inst.close()
