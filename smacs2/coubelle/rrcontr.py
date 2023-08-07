'''
smacs2/rrcontr.py <- smacs1/acontr.py
    transmits on N0,  receives on N5 (from Producer) and N7 (from Consumer),  configured by CONF,
prerequisites:
    mongod, 
operates as client:
    0.) '-clt': test Master with Slaves (aprod.py and acons.py)
    1.) in mode 0: retrieve conf from DB, if version matches required, synchronize with Prod and Cons: send conf on N0 and receive confirmation on N5/7
    2.) in mode 1, 3: send packet with 'seq' on N0 and receive packet with the same 'seq' on N5/7 (locoal protocol)
    3.) in mode 1, 3: send packet with 'mseq' on N0 and receive packet with the same 'mseq' on N5/7 (global protocol)
    4.) in mode 1, 3: evaluate d and o using returned pt and ct from P and C
    5.) in mode 1, 3: store met vector to DB and send met on N5/N7

TX-message: cdu=dict()    on N0
RX-message: cdu=dict()    on N5, N7
DB name : smacs1
collection: state, conf 
compared to network/controller, asyncio REQ-REP is deployed for N0/5/7 as Master (client)

5/3/2023/nj, laste update 7/25/2023
'''
#import asyncio
import zmq, time, sys,json, pprint, copy
from collections import deque
from threading import Thread
from pymongo import MongoClient
#from queue import SimpleQueue


client=MongoClient('localhost', 27017)
dbase = client['smacs'] 

#import pdb
#==========================================================================

class Master:
    """ Client: 
                sends multicast requests to multiple (2) servers with different addresses
                including  time list 't' , while receiving time stamp list 'ct'  or 'pt' from two servers
    """
    def __init__(self, ipv4s:list, ports:list, cid:int, qlength:int)->None:
        #self.ips = ipv4s
        #self.ports = ports
        self.id = cid 
        self.loop = True
        print('client:', ipv4s, ports, cid)
        #multicast parameter
        self.svrs= list(zip(ipv4s, ports))       #takes the shorter list length of both
        self.nsize = len(self.svrs)
        

        self.context = zmq.Context()

        self.socket,  self.rxq = [0]*self.nsize, [0]*self.nsize #self.imap=dict()
        self.maxlen = qlength
        for i in range(self.nsize): 
            self.socket[i] = self.context.socket(zmq.REQ)
            #self.socket[i] = self.context.socket(zmq.PAIR)
            self.socket[i].connect("tcp://{}:{}".format(ipv4s[i],ports[i]))
            self.rxq[i]= deque(maxlen=qlength)

        self.txq= deque(maxlen=qlength)
        self.seq = 0            #seq is handled in state_update, to avoid double advance due to unicasts
        
    def close(self):
        for s in self.socket: s.close()
        self.context.term()
        self.txq.clear()
        self.rxq.clear()

    def client(self): 
        #thr = [Thread(target=self.unicast, args=(0,)), Thread(target=self.unicast, args=(1,)), Thread(target=self.multicast)]
        #thr = [Thread(target=self.mcast_svr, args=(0,)), Thread(target=self.mcast_svr, args=(1,))]
        thr = [Thread(target=self.transport, args=((1,2),)), Thread(target=self.multicast)]
        for t in thr: t.start()
        for t in thr: t.join()
    #transport is the layer above multicast (MAC layer)
    def transport(self, key:set)->None:
        rcvds={key[0]:dict(), key[1]: dict()}
        message = self.clt_handler(rcvds)
        while self.loop:
            if message: # and not self.txq[0].full() and not self.txq[1].full():
                self.txq.append(message)
            if len(self.rxq[0]) >0: #< self.maxlen:
                response = self.rxq[0].popleft()
                rcvds[response['id']] = response
            if len(self.rxq[1]) > 0:#< self.maxlen: 
                response = self.rxq[1].popleft()
                rcvds[response['id']] = response
            if len(rcvds) == 2:
                message = self.clt_handler(rcvds)
                rcvds.clear()
        self.close()

    #two branches are tied together, using a single time stamp [t]! retransmission with time stamp update included thanks to the buffer txq 
    #request={'proto':0/1, 't'}, response={'proto':0/1, 'ct'/'pt': []}
    def multicast(self):
        #request = {}       
        response = [{}, {}]
        #for i in range(10): # #i=0 #
        while True:
            if len(self.txq) >0:
                request = self.txq.popleft()
                try:
                    if request['proto']:
                        request['t'] = [time.time_ns()]
                    self.socket[0].send_json(request) #{'user':1, 'data':"Hello 1"})
                    self.socket[1].send_json(request) #{'user':1, 'data':"Hello 2"})
                    #print(f"Client, round {i}  Sent", request)
                except:
                    request['t'].clear()
                    self.txq.appendleft(request)        #moved retransmit to lower layer
                    #print(f"round {i} sent no packet")
            #receve from server 1`
            
            if len(self.rxq[0]) < self.maxlen:
                try: #if 1:
                    response[0] = self.socket[0].recv_json()
                    if response[0]['proto']: 
                        response[0]['ct'].append(time.time_ns())
                    self.rxq[0].append(response[0])
                    #print(f"Client 1, round {i}, Received ",  response[0])
                except:
                    #print(f"Client 1, round {i}, received no message")
                    continue
            #receive from server 2
            if len(self.rxq[1]) < self.maxlen:
                try: #if 1:
                    response[1] = self.socket[1].recv_json()
                    if response[1]['proto']: 
                        response[1]['pt'].append(time.time_ns())
                    self.rxq[1].append(response[1])
                    #print(f"Client 2, round {i}, Received ",  response[1])
                except:
                    #print(f"Client 2, round {i}, received no message")
                    continue
            #i+=1
    #give time stamp at receiver
    #rcvds={pid: pkt=dict(), cid: pkt=dict()}, return tx=pkt=dict()
    def clt_handler(self, rcvds: dict)->dict:
        print('Received', rcvds)
        proto = 1
        if self.seq==0: #not isinstance(rcvds[1],dict) or not rcvds[1] or not isinstance(rcvds[2],dict) or rcvds[2]:
            tx={'id': self.id, 'seq': self.seq,   'tm': time.strftime("%x"), 'proto':0, 't':[],  'proto':proto}
            print('not received, but sent:', tx)
        else:
            tx={'id': self.id, 'seq': self.seq,    't':[],  'proto':proto}
            print('received and send before time stamping:', tx)
        self.seq += 1
        return tx

#CONF = {"ips":ips, "ports": ports, "id":0,  "key":[1,2], "dly":0, "ver": 0,  'cnt':20, "mode":0, "uperiod": 0, 'p':dict, 'c':dict}
class MasterContr(Master):
    def __init__(self, conf):
        super().__init__(conf['ips'], conf['ports'], conf['id'], conf['maxlen'])
        self.conf = copy.deepcopy(conf)
        #DB:
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

        #State:
        self.open()

    def open(self):
        print('MasterContr:')
        self.tmp = copy.deepcopy(self.conf)
        self.state = self.template_ctr()

    def close(self):
        self.state.clear()
        self.tmp.clear()
        print('closed')
        exit()

    #packet for mode 0
    def cdu0(self, seq, conf=dict()):
        st = {'id':self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'], 'proto':0}  #where ct and pt is dummy here,as c-plane needs it
        return {**st,'seq':seq, 'conf':copy.deepcopy(conf), 'crst':self.state['crst'], 'urst': self.state['urst']}

   #packet for mode 1 and 3 
    def cdu1(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 't':[], 'proto':0} #starts with proto=0
        #st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[], 'proto':1}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode']}

    def cdu3(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[], 'proto':1}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode'], 'urst': False}

    #state register, converted to CDU by make_cdu(key)
    def template_ctr(self): 
        #st ={'id':self.id,'chan':self.conf['ctr_pub'],'key':self.conf['key'],'seq':0,'mseq':0,'tseq':[0,0],'tmseq':[0,0],'loop':True,'sent':False,'ack':[True,True]}
        st ={'id':self.id,'chan':(self.conf['ips'], self.conf['ports']),'key':self.conf['key'],'seq':0,'mseq':0,'loop':self.loop,'sent':False, 'pkt':{'p':{}, 'c':{} }} 
        st.update({'ct':[],'pt':[],'met':{},'mode':self.conf['mode'], 'cnt':self.conf['cnt'], 'crst': False,'urst': 0})
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
    #sent: indicate the copy is already sent, to interpret the ack as receipt(used only by mode 0)
    #ack: multi-cast acknowledgement state (ack for Producer, ack for Consumer), deprecated due to coros
    #conf: configuration object {'p': P-CONF, 'c': C-CONf}
    #crst: controll-plane reset indicator (F/T)
    #urst: user-plane reset indicator (F/T) from F to T and from T to F, based on conf['uperiod']
    #proto: 0 local protocol, 1 measurement protocol
    '''
    #overriding the parent method Master.client()
    def client(self):
        thr = [Thread(target=self.transport, args=(self.conf['key'],)), Thread(target=self.multicast)]
        for t in thr: t.start()
        for t in thr: t.join()
    #rcvds={pid: pkt=dict(), cid: pkt=dict()}, return tx=pkt=dict()
    def clt_handler(self, rcvd: dict)->dict:
        match self.conf['mode']:
            case 0:
                return self.mode0(rcvd)
            case 1:
                return self.mode1(rcvd)
            case 2:
                return self.mode2(rcvd)
            case 3:
                return self.mode3(rcvd)
            case _: 
                print('mode unknown')
                exit()
        time.sleep(self.conf['dly'])

    def mode0(self, rcv_data:dict)->dict:
        print(f'Received: {rcv_data!r}')
        if not rcv_data[self.conf['key'][0]] or not rcv_data[self.conf['key'][1]]:
            return self.cdu0(self.state['seq']+1, self.tmp)
        pcdu, ccdu = rcv_data[self.conf['key'][0]].copy(), rcv_data[self.conf['key'][1]].copy()
        print(pcdu, ccdu)
        #if not prcdu or not ccdu: return dict()
        if ccdu['seq'] > self.state['seq'] and pcdu['seq'] > self.state['seq'] and ccdu['seq'] == pcdu['seq']:
            self.state['seq'] = pcdu['seq']
            if self.state['crst']:                                          #third step of hand-shake 
                print('finally \n')
                self.state['loop'] = False 
                self.loop = self.state['loop']                             #exit 
                print('reset, leaving state:\n', self.state) 
            if self.state['sent']:                                          #second step of hand-shake: last update, to avoid intial state
                self.conf = copy.deepcopy(self.tmp) #implement the change
                self.state['crst'] = True   
                cdu = self.cdu0(self.state['seq']+1, dict())
                self.state['sent'] = False                                      #internal control
                return cdu 
            else:                   #not sent yet
                tag = {"ver":self.conf['ver']+1}                                   #real deployment, need GUI for MongoDB to input new conf
                doc = self.co_conf.find_one(tag)                                  #and check DB for the latest update
                if doc:
                    print("got 'conf' from DB")
                    doc.pop('_id')                                             #drop MongoDB specific header #self.tmp = copy.deepcopy(self.conf)
                    self.tmp = copy.deepcopy(doc)
                else: 
                    print(f"no version {self.conf['ver']} found in DB, use current", self.conf) 
                    self.tmp = copy.deepcopy(self.conf)

                cdu = self.cdu0(self.state['seq']+1, self.tmp) 
                self.state['sent'] = True                                       #first step of hand-shake
                return cdu 
        else:
            self.tmp = copy.deepcopy(self.conf)
            cdu = self.cdu0(self.state['seq']+1, self.tmp)                           #cdu = self.cdu0(self.state['seq']+1, self.tmp) 
            return cdu                                                          # json.dumps(cdu)
        #return self.cdu0(self.state['seq']+1, self.tmp) #print('first cdu', cdu)

    def mode1(self, rcvd:dict)->dict:
        print(f'Received: {rcvd!r}')
        if not rcvd[self.conf['key'][0]] or not rcvd[self.conf['key'][1]]:
            return self.cdu1(self.state['seq']+1, self.state['mseq'])
        pcdu, ccdu = rcvd[self.conf['key'][0]].copy, rcvd[self.conf['key'][1]].copy()
        print(pcdu, ccdu)
        if pcdu['proto'] == 0 and ccdu['proto']==0:
            if ccdu['seq'] > self.state['seq'] and pcdu['seq'] > self.state['seq'] and ccdu['seq'] == pcdu['seq']:
                self.state['seq'] = pcdu['seq'] 
                if self.state['crst']:
                    self.state['loop'] = False
                    self.loop = self.state['loop']                                  #exit
                    print('reset, leaving state:\n', self.state) 
                    cdu = None
                else:
                    cdu = self.cdu1(self.state['seq'], self.state['mseq']+1)
                    cdu['proto'] = 1
            else:
                cdu = self.cdu1(self.state['seq']+1, self.state['mseq'])
                cdu['proto'] = 0

        elif pcdu['proto'] == 1 and ccdu['proto'] == 1:
            if ccdu['mseq'] > self.state['mseq'] and pcdu['mseq'] > self.state['mseq'] and ccdu['mseq'] == pcdu['mseq']:
                self.state['mseq'] = pcdu['mseq']
                self.state['ct'], self.state['pt'] = pcdu['ct'], ccdu['pt']

                if self.met():                                          #self.state['met'] filled with new
                    if self.state['mseq'] > self.conf['cnt']: 
                        self.state['crst'] = True                   #if self.state['sent']:
                        cdu = self.cdu1(self.state['seq']+1, self.state['mseq'])         #deliver met
                        cdu['proto'] = 0
                        self.state['met'].clear()
                    else:
                        cdu = self.cdu1(self.state['seq'], self.state['mseq']+1)        #retransmit
                        cdu['proto'] = 1
                else:
                    cdu = self.cdu1(self.state['seq'], self.state['mseq']+1)        #retransmit
                    cdu['proto'] = 1
        else:
            print('unknown protocol', rcvd)
            exit()

        """
        if cdu['proto'] == 1:
            t= time.time_ns()
            cdu['pt'] =[t]
            cdu['ct'] =[t]
        """
        return cdu

    def mode2(self, rcv_data:list)->dict:
        print("Error: mode 2 does not need acontr.py")
        exit()
        #return {} #json.dumps( {})

    def mode3(self, rcv_data:list)->dict:
        print(f'Received: {rcv_data!r}')
        pcdu = rcvd_data['p'].copy()
        ccdu = rcvd_data['c'].copy()
        rcvd_data.clear()

        if pcdu['proto'] == 0 and ccdu['proto']==0:
            if ccdu['seq'] > self.state['seq'] and pcdu['seq'] > self.state['seq'] and ccdu['seq'] == pcdu['seq']:
                self.state['seq'] = pcdu['seq'] #self.state['tseq'][0]
                if self.state['crst']: 
                    self.state['loop'] = False
                    self.loop = self.state['loop']
                    print('reset, leaving state:\n', self.state) 
                    cdu = None 
                else:
                    cdu = self.cdu3(self.state['seq'], self.state['mseq']+1)
                    cdu['proto'] = 1
            else:
                cdu = self.cdu3(self.state['seq']+1, self.state['mseq'])
                cdu['proto'] = 0

        elif pcdu['proto'] == 1 and ccdu['proto'] == 1:
            if ccdu['mseq'] > self.state['mseq'] and pcdu['mseq'] > self.state['mseq'] and ccdu['mseq'] == pcdu['mseq']:
                self.state['ct'], self.state['pt'] = pcdu['ct'], ccdu['pt']

                if self.met():
                    self.state['mseq'] = pcdu['mseq']
                    if self.state['mseq'] > self.conf['cnt']:
                        self.state['crst'] = True
                        cdu = self.cdu3(self.state['seq']+1, self.state['mseq'])#, self.state['crst'],self.state['urst'])
                        cdu['proto'] = 0
                        self.state['met'].clear()
                    else:
                        cdu = self.cdu3(self.state['seq'], self.state['mseq']+1)        #retransmit
                        cdu['proto'] = 1
                else:
                    cdu = self.cdu3(self.state['seq'], self.state['mseq']+1)        #retransmit
                    cdu['proto'] = 1

                # for u-plane only
                if self.conf['uperiod'] and time.time() > utimer:
                    utimer += self.conf['uperiod']
                    self.state['urst'] = not self.state['urst']
                    cdu['urst'] = self.state['urst']
                #u-plane end

            else:
                print("repeat local")#\n TX stopped,  state at Tx", self.state)
                cdu = self.cdu3(self.state['seq'], self.state['mseq']+1)
        else:
            print('unknown protocol', rcvd)
            exit()

        if cdu['proto'] == 1:
            t= time.time_ns()
            cdu['pt'] =[t]
            cdu['ct'] =[t]

        return cdu

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
    conf['conf']['p']['mode'] = m
    conf['conf']['c']['mode'] = m
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
""" CONF:
#ips=(p_ip, c_ip): ip addresses for N5 and N7
#ports=(p_port, c_port): ports for N5 and N7
#hub_ip: subpub hub address for u-plane
#key: (pid, cid) should be idential to that in producer.py and consumer.py, i.e. P_CONF and C_CONF
#ver: configuration version, to be modified by GUT on DB to trigger update in mode 0
#cnt: count number on N0 for retransmit measurement request
#mode: operation mode 0,1,2,3,4(test)
#uperiod: periods length (seconds) for u-plane flip-flop (experiment), 0 disables this feature
P/C-CONF:
#sub=(sub_port, sub_topic):  u-plane subscrition port and topic
#pub=(pub_port, pub_topic):  u-plane publication port and topic
#ctraddr = (ips, ports) = ([ip1,ip2], [port1,port2]): ip address and port for sockets for N5 and N7
"""
#ipv4= "192.168.1.204"               #system76 #ipv4= "192.168.1.99"               #lenovo P21 #ipv4= "192.168.1.37"
#ipv4= "192.168.1.204"   #system76
#ipv4= "192.168.1.99"    #lenovo P15
ipv4p="192.168.1.37"    #lenovo T450

#ips = ["127.0.0.1", "127.0.0.1"]    #c-plane addresses
ips = ["192.168.1.37", "192.168.1.37"]    #c-plane addresses
ports = [5555, 6666]                #c-plane ports

hub_ip = "127.0.0.1"                #u-plane address
sub_port = 5570                     #u-plane ports
pub_port = 5568

CONF = {"ips":ips, "ports": ports, "id":0,  "key":[1,2], "dly":0, "ver": 0, 'maxlen':4,  'cnt':20, "mode":0, "uperiod": 0}
P_CONF = {'hub_ip':hub_ip, 'sub': (sub_port, 6), 'pub': (pub_port,4), 'key':[1, 2], 'ctraddr':(CONF['ips'][0], CONF['ports'][0]), 'dly':1., 'maxlen': 4, 'mode': 0}
C_CONF = {'hub_ip':hub_ip, 'sub': (sub_port, 4), 'pub': (pub_port,6), 'key':[1, 2], 'ctraddr':(CONF['ips'][1], CONF['ports'][1]), 'dly':1., 'maxlen': 4, 'mode': 0}
CONF['conf'] = {'p': P_CONF, 'c':C_CONF}

if __name__ == "__main__":
    print(sys.argv)
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
    if '-clt' in sys.argv:
        s=Master(CONF['ips'], CONF['ports'],CONF['id'], CONF['maxlen'])
        s.client()
        exit()
    if len(sys.argv) > 1:
        m = int(sys.argv[1])
        #CONF['mode'] = m CONF['conf']['p'] = m CONF['conf']['c'] = m
        set_mode(CONF, m)
        inst=MasterContr(CONF)
        inst.client()
        inst.close()
    else:
        print('usage: python3 rrcontr.py (mode 0, 1,2,3,4, or default mode 0')
        print('usage: python3 rrcontr.py -testdb/prepdb/svr')
        exit()
    """ 
    def unicast(self, r):
        while self.loop:
            if not self.txq[r].empty():
                request = self.tx[r].get_nowait()

                if request['proto']:
                    if r:      #consumer
                        request['ct'].append(time.time_ns())
                    else:       #producer
                        request['pt'].append(time.time_ns())
                self.socket[r].send_json(request)
                print('requset', request)
            if not self.rxq[r].full():
                response = self.socket[r].recv_json()
                if response['proto']:
                    if r:      # consumer
                        response['pt'].append(time.time_ns())
                    else:      # producer 
                        response['ct'].append(time.time_ns())
                self.rxq[r].put_nowait(response)
                print('response', response)
        self.close()

    def multicast(self):
        rcvds=list()
        message = self.clt_handler(rcvds)

        while self.loop:
            #if message and not self.txq[0].full() and not self.txq[1].full():
            self.txq[0].put_nowait(message)
            self.txq[1].put_nowait(message)

            if not self.rxq[0].empty():
                rcv = self.rxq[0].get_nowait()
                rcvds.append(rcv)
            if not self.rxq[1].empty():
                rcv = self.rxq[1].get_nowait()
                rcvds.append(rcv)
            if len(rcvds) == 2:
                message = self.clt_handler(rcvds)
                rcvds.clear()
        self.close()
    """            
    """                
    #this code emulate a multicast client via multithreads to requrest multiple servers
    def mcast_svr(self, r): #unicast client for server r 
        self.message = self.clt_handler(self.rcvds)     #self.rcvds initiated in __init__
        while self.loop:
            if len(self.rcvds) == self.nsize:            #self.nsize == 2
                if self.txdone: # all pending transmissions are done 
                    self.message= self.clt_handler(self.rcvds)
                    self.txdone = False
                try:
                    print(f'Master Send: {self.message!r}')
                    self.txtime(self.message)
                    mstring = json.dumps(self.message) #if 0 not in set(self.snt):
                    self.socket[0].send(mstring.encode()) #    self.snt.append(0) #if 1 not in set(self.snt):
                    self.socket[1].send(mstring.encode()) #    snt.append(1) #if len(self.snt) == self.nsize:  #2: #snt.clear()
                    self.rcvds.clear() #self.message.clear()
                    self.txdone = True
                except:
                    time.sleep(1)#self.conf['dly'])
                    continue
            else:   
                try:
                    msg = self.socket[r].recv()
                    rcvdm =json.loads(msg.decode())
                    self.rxtime(rcvdm)
                    self.rcvds[self.imap[rcvdm['id']]] = rcvdm.copy()
                    print(f'Received: {rcvdm!r}')
                except:
                    time.sleep(1)
    def rxtime(self, pkt):
        if pkt['proto']: #measurement
            if pkt['id'] == self.smap[0]: #CONF['key'][0]:
                pkt['ct'].append(time.time_ns())
            if pkt['id'] == self.smap[1]: #CONF['key'][1]:
                pkt['pt'].append(time.time_ns())
    #give time stamp at multicaster 
    def txtime(self, pkt):
        if pkt['proto']: #measurement
            t = time.time_ns()
            pkt['pt']= [t]
            pkt['ct']= [t]

    """
