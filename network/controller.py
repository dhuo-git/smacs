'''
controller.py
    implemented multicast service for producer.py and consumer.py,  transmits on N0,  receives on N5 (from Producer) and N7 (from Consumer)
    configured by CONF,  may need medium.py in background for channel simulation
prerequisites:
    medium.py/hub.py, mongod
operates as client:
    0.) in mode 4: test hub
    1.) in mode 0: send conf to N0 and receive confirmation on N5/7
    2.) in mode 0: retrieve conf from DB, if difers from self.conf, update itself and Prod and Cons
    3.) in mode 1, 3: send packet with 'seq' on N0 and receive packet with the same 'seq' on N5/7 (locoal protocol)
    4.) in mode 1, 3: send packet with 'mseq' on N0 and receive packet with the same 'mseq' on N5/7 (global protocol)
    5.) in mode 1, 3: evaluate d and o using returned pt and ct from P and C
    6.) in mode 1, 3: store state for each key=(pid,cid) to DB and send adaptation command on N5/N7
    7.) in mode 1, 3: check for update of conf in DB and update conf, including 'cnt' for measurement 

TX-message: {'cdu':dict(), 'sdu': dict()}    on N0
RX-message: {'cdu':dict(), 'sdu': dict()}    on N5, N7

Mode Test, 0, 1,3: Independent TX and RX (ITR)

    DB name : smacs
    collection: state, conf #e.g. smacs.state.insert(state), smacs.conf.find_one({'tag':tagitem})
5/3/2023/nj, laste update 7/3/2023
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
        self.tag = get_tag(self.co_state, "July 2")
        print("db tag", self.tag)

        self.conf = conf
        self.open()
        #if self.tmp !=  self.conf: print('tmp:', self.tmp)


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
    def cdu0(self, seq, conf=dict()): #, crst=False, urst=False):
        st = {'id':self.id, 'chan': self.conf['ctr_pub'], 'key':self.conf['key']}
        return {**st,'seq':seq, 'conf':conf, 'crst':self.state['crst'], 'urst': self.state['urst']}

   #packet for mode 1 and 3 
    def cdu13(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': self.conf['ctr_pub'], 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[]}
        return {**st,  'crst': self.state['crst'], 'urst': self.state['urst'],  'met':self.state['met'].copy(), 'mode':self.state['mode']}
        #return {'id': self.id, 'chan': self.conf['ctr_pub'], 'key':self.conf['key'], 'seq':seq, 'mseq':mseq, 'ct':[], 'pt':[], 'met':met, 'mode':mode, 'reest': False}

    def cdu_test(self, seq):
        return {'id': self.id, 'chan': self.conf['ctr_pub'], 'seq': seq, 'time':time.time()}

    #state register, converted to CDU by make_cdu(key)
    def template_ctr(self): #, crst=False, urst=False): #for mode 1,3
        st ={'id':self.id,'chan':self.conf['ctr_pub'],'key':self.conf['key'],'seq':0,'mseq':0,'tseq':[0,0],'tmseq':[0,0],'loop':True,'sent':False,'ack':[True,True]}
        st.update({'ct':[],'pt':[],'met':{},'mode':self.conf['mode'], 'cnt':self.conf['cnt'],'msr':True, 'conf': self.conf, 'crst': False, 'urst':False})
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
    #urst: user-plane reset indicator (F/T)
    '''
    def run(self): #respond-receive
        #rst=self.co_conf.insert_one(self.conf)              #prepare DB with current version
        #print('conf stored in db', rst.inserted_id)
        if self.conf['mode'] == 0:
            update_mongo(self.conf)
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
        message = {'cdu': cdu}
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
        while True: 
            cdu = self.cdu_test(self.state['seq']+1)
            self.transmit(cdu, 'tx:')
            time.sleep(self.conf['dly'])
    def TestRx(self):
        while True: 
            sub_topic, cdu = self.receive('rx:')
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

                if self.state['crst']: #self.state['seq'] = 0 #self.state['crst'] = False
                    self.state['loop'] = False 
                    print('reset, leaving state:\n', self.state) #self.state=self.template_ctr()
                else:

                    if self.state['sent']: #last update
                        tmp.pop('conf')
                        self.conf = copy.deepcopy(tmp) #implement the change
                        self.state['crst'] = True   #cdu = self.cdu0(self.state['seq']+1, dict(), self.state['crst'],self.state['urst'])
                        cdu = self.cdu0(self.state['seq']+1, dict())#, self.state['crst'],self.state['urst'])
                        self.transmit(cdu, 'tx:\n')
                    else:                   #not sent yet
                        #tag = {"ver":self.conf['ver']+1}                                   #real deployment, need GUI for MongoDB to input new conf
                        tag = {'ver':self.conf['ver']}                                      #for test only
                        tmp = self.co_conf.find_one(tag)                                  #and check DB for the latest update
                        if tmp:
                            print("got 'conf' from DB")
                            tmp.pop('_id')                                             #drop MongoDB specific header #self.tmp = copy.deepcopy(self.conf)
                            self.tmp = tmp['conf'].copy()
                            cdu = self.cdu0(self.state['seq']+1, self.tmp) #, self.state['crst'],self.state['urst'])
                            message = {'cdu': cdu}
                            bstring = json.dumps(message)
                            self.pub_socket.send_string("%d %s"% (cdu['chan'], bstring))       #multi-cast to ctr_pub
                            self.state['sent'] = True
                            print('tx:\n',message )
                self.state['tseq'] = [0,0]
                self.state['ack']= [False, False]

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
            elif sub_topic == self.conf['ctr_subc']:  #consumer on n7
                if cdu['seq'] > self.state['seq']:             #local protocol
                    self.state['tseq'][1] = cdu['seq']
                    self.state['ack'][1]= True
                if cdu['mseq']> self.state['mseq']:             #measurement 
                    cdu['pt'].append(time.time_ns())
                    self.state['pt'] = copy.deepcopy(cdu['pt'])
                    self.state['tmseq'][1] =  cdu['mseq']
                    self.state['ack'][1]= True
        else:
            print("\n RX stopped,  state at Rx", self.state)
            return #$time.sleep(self.conf['dly'])
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
                    cdu = self.cdu13(self.state['seq']+1, self.state['mseq']+1)         #deliver met
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'post met tx:')
                    self.state['met'].clear()
                    self.state['tmseq'] = [0,0]
                else:
                    cdu = self.cdu13(self.state['seq'], self.state['mseq']+1)
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'priori met tx:')
                    self.state['tmseq'] = [0,0]
                print('next measurement', self.state['mseq'])
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
                    self.state['ct'] = copy.deepcopy(cdu['ct'])
                    self.state['tmseq'][0] =  cdu['mseq']
                    self.state['ack'][0]= True
            #RX-C
            elif sub_topic == self.conf['ctr_subc']:  #consumer on n7
                if cdu['seq'] > self.state['seq']:             #local protocol
                    self.state['tseq'][1] = cdu['seq']
                    self.state['ack'][1]= True
                if cdu['mseq']> self.state['mseq']:             #measurement 
                    cdu['pt'].append(time.time_ns())
                    self.state['pt'] = copy.deepcopy(cdu['pt'])
                    self.state['tmseq'][1] =  cdu['mseq']
                    self.state['ack'][1]= True
        else:
            print("\n RX stopped,  state at Rx", self.state)
            return #$time.sleep(self.conf['dly'])
    #handle receive CDU : RX+Handler
    def Mode3Tx(self):
        print('mode 3, Tx', self.state['mode'])
        while self.state['loop']:
            if not (self.state['ack'][0] and self.state['ack'][1]):
                continue
            elif self.state['tseq'][0] == self.state['tseq'][1]:          #accept acknoledgement
                self.state['seq'] = self.state['tseq'][0]
                if self.state['crst']:
                    self.state['loop'] = False
                    print('reset, leaving state:\n', self.state) #self.state['crst'] = False #self.state['sent'] = False
            if self.state['tmseq'][0] == self.state['tmseq'][1]:
                self.state['mseq'] = self.state['tmseq'][0]
                if self.met():                                          #self.state['met'] filled with new
                    if self.state['mseq'] > self.conf['cnt']:
                        self.state['crst'] = True
                    cdu = self.cdu13(self.state['seq']+1, self.state['mseq']+1)#, self.state['crst'],self.state['urst'])
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'post met tx:')
                    self.state['met'].clear()
                    self.state['tmseq'] = [0,0]
                else:
                    cdu = self.cdu13(self.state['seq'], self.state['mseq']+1)
                    t = time.time_ns()
                    cdu['ct']= [t]
                    cdu['pt']= [t]
                    self.transmit(cdu, 'priori met tx:')
                    self.state['tmseq'] = [0,0]
                print('next measurement', self.state['mseq'])

    #compute measurement, estimation and tracking
    def met(self):

        if len(self.state['pt'])==6 and len(self.state['ct'])==6:
            dp = self.state['pt'][3] - self.state['pt'][2]
            dc = self.state['ct'][3] - self.state['ct'][2]

            pl = [self.state['pt'][1]-self.state['pt'][0],  self.state['ct'][5]-self.state['ct'][4]]
            cr = [self.state['ct'][1]-self.state['ct'][0],  self.state['pt'][5]-self.state['pt'][4]]
            
            self.state['met'] = {'pco':  (dp-dc)/2.0, 'pctm': (dp+dc)/2.0, 'lo': (pl[0]-pl[1])/2.0, 'ltm':(pl[0]+pl[1])/2.0, 'ro': (cr[0]- cr[1])/2.0, 'rtm':(cr[0]+cr[1])/2.0}
            print('computed, to save')
            #stored to DB the measurement states for producer and consumer 
            add_data(self.co_state, self.tag, self.state['met'])
            return True
        return False
#-----------
def get_tag(col,  mark=None):
    if not mark:
        tag = {'mark': time.ctime()[4:]}
    else:
        tag = {'mark': mark}

    doc = {**tag, 'data':[]}
    try:
        rst = col.find_one(tag)
        if rst.acknowledged:
            rst = col.replace_one(tag, doc)
            print('replaced ', rst.modified_count)
    except:
        rst = col.insert_one(doc)
        print('inserted with', rst.inserted_id, rst.acknowledged)
    finally:
        return tag
        #doc = col.find_one(tag)
        #print('initial doc', doc) 
        #return {"_id": doc["_id"]}

def add_data(col, tag, entry):
    doc = col.find_one(tag)
    if doc:
        doc['data'].append(entry)
        rst=col.replace_one(tag, doc) #rst=col.update_one(tag, doc)
        print(f'saved {rst.modified_count} in collection', rst.acknowledged)
    else:
        print('not saved')


    #option: check DB before start and add "_id" to the input conf in DB
def update_mongo(conf):
    col= dbase['conf']
    try:                   
        doc =  col.find_one() #doc =  self.co_conf.find_one(tag)
        if doc and doc[0]:
            if doc[0]['ver'] < conf['ver']:
                rst = col.replace_one(conf)
                print("replace an entry of collection 'conf' in DB:", rst.acknowledged)
                return True 
        else:
            rst = col.insert_one(conf)
            print("saved conf to collection 'conf' in DB: ", rst.acknowledged)
            return True
    except:
        print("cannot update DB", doc)
    finally:
        print("collections:", dbase.list_collection_names())
    
#------------------------------ TEST -------------------------------------------
from producer import CONF as P_CONF
from consumer import CONF as C_CONF

CONF = {"ipv4":"127.0.0.1" , "sub_port": "5570", "pub_port": "5568", "id":0, "dly":0,  "maxlen": 4, "print": True, "ver": 0, 'cnt':4, "mode":0}
CONF.update({"ctr_pub": 0,  "ctr_subp": 5, "ctr_subc":7, "key":[1,2], 'conf':{'p': P_CONF, 'c':C_CONF}})

#ctr_pub: multicast interface
#ctr_subp: multicast reverse channel of producer
#ctr_subc: multicast reverse channel of consumer
#key: (pid, cid) should be idential to that in producer.py and consumer.py, i.e. P_CONF and C_CONF
#cnt: count number on N0 for retransmit measurement request

#seq: (pseq, cseq) sequence numbers of producer and consumer
if __name__ == "__main__":
    if len(sys.argv) > 1:
        CONF['mode'] = int(sys.argv[1])
        print(sys.argv)
        inst=Controller(CONF)
        inst.run()
        inst.close()
    elif len(sys.argv) > 2:
        print(sys.argv)
        print('usage: python3 controller.py')
        exit()
    else:
        print(sys.argv)
        inst=Controller(CONF)
        inst.run()
        inst.close()
