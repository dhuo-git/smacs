'''
smacs2/rrcontr.py  (history: smacs1/acontr.py)
    transmits on N0,  receives on N5 (from Producer) and N7 (from Consumer),  configured by CONF,
prerequisites:
    mongod, 
operates as client:
    0.) '-clt': test Master with Slaves (aprod.py and acons.py)
    1.) in mode 0: retrieve conf from DB, if version matches required, synchronize with Prod and Cons: send conf on N0 and receive confirmation on N5/7
    3.) in mode 1: conditioned on rrcons.py 1 and rrrpod.py 1 are active, send measurement command and receive measurement results, without active u-plane
    4.) in mode 3: conditioned on rrcons.py 3 and rrprod.py 3 are active, send measurement command and receive measurement results, with active u-plane
    5.) prepare database: store hard coded CONF into mogngo DB with the desired version number (for use by moe 0 )

TX-message: cdu=dict()    on N0
RX-message: cdu=dict()    on N5, N7
DB name : smacs
collection: state, conf 
compared to network/controller, here the REQ-REP is deployed instread of PUB-SUB for N0/5/7 as Master (client)

7/20/2023/nj, laste update 8/7/2023
'''
import zmq, time, sys,json, pprint, copy
from collections import deque
from threading import Thread
from pymongo import MongoClient
#import pdb
client=MongoClient('localhost', 27017)
dbase = client['smacs'] 
#==========================================================================

class Master:
    """ Client: 
                sends multicast requests to multiple (2) servers with different addresses
                including  time list 't' , while receiving time stamp list 'ct'  or 'pt' from two servers
    """
    def __init__(self, ipv4s:list, ports:list, cid:int, qlength:int)->None:
        self.id = cid 
        self.loop = True
        print('client:', ipv4s, ports, cid)
        self.svrs= list(zip(ipv4s, ports))       #takes the shorter list length of both
        self.nsize = len(self.svrs)     #nsize=2 for Producer and Consumer

        self.context = zmq.Context()

        self.socket,  self.rxq = [0]*self.nsize, [0]*self.nsize 
        self.maxlen = qlength
        for i in range(self.nsize): 
            self.socket[i] = self.context.socket(zmq.REQ)
            #self.socket[i] = self.context.socket(zmq.PAIR)
            self.socket[i].connect("tcp://{}:{}".format(ipv4s[i],ports[i]))
            self.rxq[i]= deque(maxlen=qlength)

        self.txq= deque(maxlen=qlength)
        self.seq = 0            #seq is handled in state_update, to avoid double advance due to unicasts
        self.proto = 1
        
    def close(self):
        self.txq.clear()
        self.rxq.clear()
        for s in self.socket: s.close()
        self.context.term()

    def client(self): 
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

    #multicast is MAC layer: two branches are tied together, using a single time stamp [t]! retransmission with time stamp update included thanks to the buffer txq 
    #request={'proto':0/1, 't'}, response={'proto':0/1, 'ct'/'pt': []}
    def multicast(self):
        response = [{'id': self.id, 'proto':self.proto}, {'id':self.id,'proto':self.proto}]
        while self.loop:
            if len(self.txq) >0:
                request = self.txq.popleft()
                try:
                    if request['proto']:
                        request['t'] = [time.time_ns()]
                    for k in range(self.nsize):
                        self.socket[k].send_json(request.copy()) 
                    """ #print(f"Client, round {i}  Sent", request) """
                except:
                    request['t'].clear()
                    self.txq.appendleft(request)        #moved retransmit to lower layer #print(f"round {i} sent no packet")
            #receve from server 1: Producer, expect 'ct'
            if len(self.rxq[0]) < self.maxlen:
                try:
                    response[0] = self.socket[0].recv_json()
                    if response[0]['proto']: 
                        response[0]['ct'].append(time.time_ns())
                    self.rxq[0].append(response[0].copy()) #print(f"Client 1, round {i}, Received ",  response[0])
                except:
                    #print(f"Client 1, round {i}, received no message")
                    pass
            #receive from server 2: Consumer, expect 'pt'
            if len(self.rxq[1]) < self.maxlen:
                try:
                    response[1] = self.socket[1].recv_json()
                    if response[1]['proto']: 
                        response[1]['pt'].append(time.time_ns())
                    self.rxq[1].append(response[1].copy())
                    #print(f"Client 2, round {i}, Received ",  response[1])
                except:
                    #print(f"Client 2, round {i}, received no message")
                    pass
            time.sleep(CONF['dly'])
        self.close()
    #not isinstance(rcvds[1],dict) or not rcvds[1] or not isinstance(rcvds[2],dict) or rcvds[2]:
    def clt_handler(self, rcvds: dict)->dict:
        print('Received', rcvds)
        if self.seq==0: 
            tx={'id': self.id, 'seq': self.seq,   'tm': time.strftime("%x"), 'proto':0, 't':[],  'proto':self.proto}
        else:
            tx={'id': self.id, 'seq': self.seq,    't':[],  'proto':self.proto}
        self.seq += 1
        print('Sent before time stamping:', tx)
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

        self.open()
        print('state:', self.state) 

    def open(self):
        print('MasterContr:')
        self.tmp = copy.deepcopy(self.conf)
        self.state = self.template_ctr()

    def close(self):
        self.state.clear()
        self.tmp.clear()
        super().close()
        #print('closed')
        exit()

    #packet for mode 0
    def cdu0(self, seq, conf=dict()):
        st = {'id':self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'], 'proto':0} 
        return {**st,'seq':seq, 'conf':copy.deepcopy(conf), 'crst':self.state['crst'], 'urst': self.state['urst']}

   #packet for mode 1 and 3 
    def cdu1(self, cseq, mseq, proto):
        st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'], 'seq':cseq, 'mseq':mseq, 't':[], 'proto':proto} 
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode']}

    def cdu3(self, cseq, mseq, proto):
        st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 't':[], 'proto':proto}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode'], 'urst': False}
    def template_ctr(self): 
        #st ={'id':self.id,'chan':(self.conf['ips'], self.conf['ports']),'key':self.conf['key'],'seq':0,'mseq':0,'loop':self.loop,'sent':False}
        st ={'id':self.id,'chan':(self.conf['ips'], self.conf['ports']),'key':self.conf['key'],'seq':0,'mseq':0,'sent':False}
        st.update({'ct':[],'pt':[],'met':{},'mode':self.conf['mode'], 'cnt':self.conf['cnt'], 'crst': False,'urst': 0})
        return st
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
            case 3:
                return self.mode3(rcvd)
            case _: 
                print('mode unknown')
                exit()
        time.sleep(self.conf['dly'])

    def mode0(self, rcv_data:dict)->dict:
        #print(f'Received: {rcv_data!r}')
        if not rcv_data[self.conf['key'][0]] or not rcv_data[self.conf['key'][1]]:
            return self.cdu0(self.state['seq']+1, self.tmp)
        pcdu, ccdu = rcv_data[self.conf['key'][0]].copy(), rcv_data[self.conf['key'][1]].copy()
        print('\np:', pcdu,'\nc:',  ccdu)
        if ccdu['seq'] > self.state['seq'] and pcdu['seq'] > self.state['seq'] and ccdu['seq'] == pcdu['seq']:
            self.state['seq'] = pcdu['seq']
            if self.state['crst']:                           
                                                        #third step of hand-shake 
                print('finally \n')
                #self.state['loop'] = False 
                self.loop = False #self.state['loop']          #exit 
                print('reset, leaving state:\n', self.state) 
            if self.state['sent']:                      #second step of pause: last update, to avoid intial state
                self.conf = copy.deepcopy(self.tmp)     #copy the new conf
                self.state['crst'] = True   
                cdu = self.cdu0(self.state['seq']+1, dict())
                #self.state['sent'] = False              #is sent 
                return cdu 
            else:                                       #not sent yet
                tag = {"ver":self.conf['ver']+1}        #(tbd) need GUI for MongoDB to input new conf
                doc = self.co_conf.find_one(tag)        #and check DB for the latest update
                if doc:
                    print("got 'conf' from DB")
                    doc.pop('_id')                      #drop MongoDB header 
                    self.tmp = copy.deepcopy(doc)
                else: 
                    print(f"no version {self.conf['ver']} found in DB, use current", self.conf) 
                    self.tmp = copy.deepcopy(self.conf)

                cdu = self.cdu0(self.state['seq']+1, self.tmp) 
                self.state['sent'] = True                #mark as sent
                return cdu 
        else:
            self.tmp = copy.deepcopy(self.conf)
            cdu = self.cdu0(self.state['seq']+1, self.tmp)           
            return cdu                                              

    def mode1(self, rcvd:dict)->dict:
        #print(f'Received: {rcvd!r}')
        self.proto = 1
        pcdu, ccdu = rcvd[self.conf['key'][0]].copy(), rcvd[self.conf['key'][1]].copy()
        print('\np:', pcdu,'\nc:',  ccdu)
        if not pcdu or not ccdu:
            cdu = self.cdu3(self.state['seq'], self.state['mseq']+1, self.proto)
            return cdu

        if pcdu['proto'] == 1 and ccdu['proto'] == 1:
            if ccdu['mseq'] > self.state['mseq'] and pcdu['mseq'] > self.state['mseq'] and ccdu['mseq'] == pcdu['mseq']:
                self.state['ct'], self.state['pt'] = pcdu['ct'], ccdu['pt']
                if self.met():                                          #self.state['met'] filled with new
                    self.state['mseq'] = pcdu['mseq']
                    if self.state['mseq'] > self.conf['cnt']: 
                        self.state['crst'] = True                   #if self.state['sent']:
                        cdu = self.cdu1(self.state['seq']+1, self.state['mseq'], 0)         #deliver met
                        self.state['met'].clear()
                    else:
                        cdu = self.cdu1(self.state['seq'], self.state['mseq']+1, self.proto)        #next measurement 
                    return cdu

            cdu = self.cdu1(self.state['seq'], self.state['mseq']+1, self.proto)        #retransmit of last measurement

        elif pcdu['proto'] == 0 and ccdu['proto']==0:
            if ccdu['seq'] > self.state['seq'] and pcdu['seq'] > self.state['seq'] and ccdu['seq'] == pcdu['seq']:
                self.state['seq'] = pcdu['seq'] 
                if self.state['crst']:
                    print('\nLast state:', self.state) 
                    print('\nMeasurement ends and ....  \n')#, self.state) 
                    self.state['seq'] = 0 #self.state['loop'] = False
                    self.loop = False 
                    cdu = None
                    sys.exit()
                else:
                    cdu = self.cdu1(self.state['seq']+1, self.state['mseq'], 0)     #next message 
                return cdu
            else:
                cdu = self.cdu1(self.state['seq']+1, self.state['mseq'], 0)         #retransmit for last message
        else:
            print('unknown protocol', rcvd)
            exit()
        return cdu

    def mode2(self, rcv_data:list)->dict:
        print("Error: Controller does not controll mode 2. Use rrcons.py and rrprod.py")
        exit()

    def mode3(self, rcvd:list)->dict:
        print(f'Received: {rcvd!r}')
        self.proto = 1
        utimer = time.time()
        #if not rcvd[self.conf['key'][0]] or not rcvd[self.conf['key'][1]]: return self.cdu3(self.state['seq'], self.state['mseq']+1, self.proto)
        pcdu, ccdu = rcvd[self.conf['key'][0]].copy(),  rcvd[self.conf['key'][1]].copy()
        print('\np:', pcdu, '\nc:', ccdu)
        if not pcdu or not ccdu:
            cdu = self.cdu3(self.state['seq'], self.state['mseq']+1,self.proto)
            return cdu

        if pcdu['proto'] == 1 and ccdu['proto'] == 1:
            if ccdu['mseq'] > self.state['mseq'] and pcdu['mseq'] > self.state['mseq'] and ccdu['mseq'] == pcdu['mseq']:
                self.state['ct'], self.state['pt'] = pcdu['ct'], ccdu['pt']
                if self.met():
                    self.state['mseq'] = pcdu['mseq']
                    if self.state['mseq'] > self.conf['cnt']:
                        self.state['crst'] = True                                   #reset
                        cdu = self.cdu3(self.state['seq']+1, self.state['mseq'], 0)
                        self.state['met'].clear()
                    else:
                        cdu = self.cdu3(self.state['seq'], self.state['mseq']+1, self.proto)#next measurement 
                # for u-plane only
                if self.conf['uperiod'] and time.time() > utimer:
                    utimer += self.conf['uperiod']
                    self.state['urst'] = not self.state['urst']
                    cdu['urst'] = self.state['urst']
                #u-plane end
                return cdu
            cdu = self.cdu3(self.state['seq'], self.state['mseq']+1, self.proto)        #retransmit

        elif pcdu['proto'] == 0 and ccdu['proto']==0:
            if ccdu['seq'] > self.state['seq'] and pcdu['seq'] > self.state['seq'] and ccdu['seq'] == pcdu['seq']:
                self.state['seq'] = pcdu['seq'] #self.state['tseq'][0]
                if self.state['crst']: 
                    print('\nLast state:', self.state) 
                    print('\ncomplete and reset.\n')
                    self.state['loop'] = False
                    self.loop = False
                    cdu = None 
                    sys.exit()
                else:                                                            #should not be here, but if
                    #self.state['crst'] = True
                    cdu = self.cdu3(self.state['seq']+1, self.state['mseq'], 0)  #next message, or re-do reset
                return cdu
            else:
                cdu = self.cdu3(self.state['seq']+1, self.state['mseq'], 0)     #was not received by peer, retransmit
        else:
            print('unknown protocol', rcvd)
            exit()
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
#mode: operation mode 0,1,2,3
#uperiod: periods length (seconds) for u-plane flip-flop (experiment), 0 disables this feature
P/C-CONF:
#sub=(sub_port, sub_topic):  u-plane subscription port and topic
#pub=(pub_port, pub_topic):  u-plane publication port and topic
#ctraddr = (ips, ports) = ([ip1,ip2], [port1,port2]): ip address and port for sockets for N5 and N7
"""
#ipv4= "192.168.1.204"               #system76 #ipv4= "192.168.1.99"               #lenovo P21 #ipv4= "192.168.1.37"
#ipv4= "192.168.1.204"   #system76
#ipv4= "192.168.1.99"    #lenovo P15
ipv4="192.168.1.37"    #lenovo T450
#ipv4="127.0.0.1"    #local
#ipv4 = "0.0.0.0"

#ips = ["127.0.0.1", "127.0.0.1"]    #c-plane addresses
ips = ["192.168.1.37", "192.168.1.37"]    #c-plane addresses
ports = [5555, 6666]                #c-plane ports

hub_ip = ipv4 #"127.0.0.1"                #u-plane address
sub_port = 5570                     #u-plane ports
pub_port = 5568

CONF = {"ips":ips, "ports": ports, "id":0,  "key":[1,2], "dly":1, "ver": 0, 'maxlen':4,  'cnt':12, "mode":0, "uperiod": 0}
P_CONF = {'hub_ip':hub_ip, 'sub': (sub_port, 6), 'pub': (pub_port,4), 'key':[1, 2], 'ctraddr':(ips[0], ports[0]),'psrc_port': ports[0]+2,'esrc': False,  'dly':CONF['dly'], 'maxlen': 4, 'mode': 0}
C_CONF = {'hub_ip':hub_ip, 'sub': (sub_port, 4), 'pub': (pub_port,6), 'key':[1, 2], 'ctraddr':(ips[1], ports[1]),'csrc_port': ports[1]+2,'esnk': False,  'dly':CONF['dly'], 'maxlen': 4, 'mode': 0}
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
        m = int(sys.argv[1]) #CONF['mode'] = m CONF['conf']['p'] = m CONF['conf']['c'] = m
        set_mode(CONF, m)       #set mode value consistently across CONF, P_CONF, C_CONF
        inst=MasterContr(CONF)
        inst.client()
        inst.close()
    else:
        print('usage: python3 rrcontr.py (mode 0, 1,2,3')
        print('usage: python3 rrcontr.py -testdb(test db)/prepdb(prepare db)/clt(test req-rep)')
        exit()
