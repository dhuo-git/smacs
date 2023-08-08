'''
contrtest.py <- acontrtest.py (for testing acontr.py ) <-smacs1/acontr.py
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

5/3/2023/nj, laste update 7/20/2023
'''
#import asyncio
import zmq, time, sys,json, pprint, copy
from collections import deque
from threading import Thread
from pymongo import MongoClient

client=MongoClient('localhost', 27017)
dbase = client['smacs1'] 

import pdb
#==========================================================================

class Master:
    """ Client: inititates transmit to multiple servers  and handle received from multiple servers"""
    def __init__(self, ipv4s, ports, cid):
        self.ips = ipv4s
        self.ports = ports
        self.id = cid 
        self.loop = True
        self.seq = 0
        self.svrs= list(zip(ipv4s, ports))       #takes the shorter list length of both
        self.nsize = len(self.svrs)
        print('client:', ipv4s, ports, cid)

        self.context = zmq.Context()
        self.sockets = [ self.context.socket(zmq.REQ),  self.context.socket(zmq.REQ)]
        for i in range(2):
            self.sockets[i].connect("tcp://{}:{}".format(ipv4s[i],ports[i]))


    def close(self):
        for s in self.sockets: s.close()
        self.context.term()

    def client(self):
        rcvd_data = []
        while self.loop:
            message= self.clt_handler(rcvd_data)
            mstring = json.dumps(message)
            for s in self.sockets:
                s.send(mstring.encode())
            time.sleep(1)
            rcvd = [s.recv() for s in self.sockets]
            rcvd_data = list(map(json.loads, [s.decode() for s in rcvd]))
            print('received', rcvd_data)
        self.close()

                
    #to be overriden by child class
    def clt_handler(self, rcv_data: list)->dict:
        rcvid = list() #if not self.seq:
        if not rcv_data:
            tx={'id': self.id, 'seq': self.seq, 'date':time.strftime("%x"),'pt':[], 'ct':[],  'rcvid': rcvid}
        else:
            print(f'Received: {rcv_data!r}') #print('Close the connection')
            if len(rcv_data) == self.nsize:
                for i in range(self.nsize):
                    message = rcv_data[i]
                    rcvid.append(message['id']) #print("list of received id", rcvid)
            tx={'id': self.id, 'seq': self.seq, 'rcvid': rcvid, 'pt':[], 'ct':[]}
        print(f'Send: {tx!r}')
        self.seq += 1
        return tx 

#CONF = {"ips":ips, "ports": ports, "id":0,  "key":[1,2], "dly":0, "ver": 0,  'cnt':20, "mode":0, "uperiod": 0}
class MasterContr(Master):
    def __init__(self, conf):
        super().__init__(conf['ips'], conf['ports'], conf['id'])

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
        print('MasterContr:')
        self.tmp = copy.deepcopy(self.conf)
        self.state = self.template_ctr()

    def close(self):
        self.state.clear()
        print('closed')
        exit()

    #packet for mode 0
    def cdu0(self, seq, conf=dict()):
        st = {'id':self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'], 'pt':[], 'ct':[]}  #where ct and pt is dummy here,as c-plane needs it
        return {**st,'seq':seq, 'conf':copy.deepcopy(conf), 'crst':self.state['crst'], 'urst': self.state['urst']}

   #packet for mode 1 and 3 
    def cdu1(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[]}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode']}

    def cdu3(self, cseq, mseq):#, met, mode):
        st = {'id': self.id, 'chan': (self.conf['ips'], self.conf['ports']), 'key':self.conf['key'].copy(), 'seq':cseq, 'mseq':mseq, 'ct':[], 'pt':[]}
        return {**st,  'crst': self.state['crst'], 'met':copy.deepcopy(self.state['met']), 'mode':self.state['mode']}

    #def cdu_test(self, seq): return {'id': self.id, 'chan': self.conf['ctr_pub'], 'seq': seq, 'time':time.time()}

    #state register, converted to CDU by make_cdu(key)
    def template_ctr(self): 
        #st ={'id':self.id,'chan':self.conf['ctr_pub'],'key':self.conf['key'],'seq':0,'mseq':0,'tseq':[0,0],'tmseq':[0,0],'loop':True,'sent':False,'ack':[True,True]}
        st ={'id':self.id,'chan':(self.conf['ips'], self.conf['ports']),'key':self.conf['key'],'seq':0,'mseq':0,'tseq':[0,0],'tmseq':[0,0],'loop':True,'sent':False} #,'ack':[True,True]}
        st.update({'ct':[],'pt':[],'met':{},'mode':self.conf['mode'], 'cnt':self.conf['cnt'], 'crst': False,'urst': False})
        print('state:', st) #pprint.pprint(st)
        return st
    #
    #def run(self): #respond-receive self.client()
    #def clt_handler(self, rcv_data:list)->str:
    def clt_handler(self, rcv_data:list)->dict: #a_mode0(cdu)
        if not rcv_data: #self.state['seq']:
            cdu = self.cdu0(self.state['seq']+1, self.tmp)
            print('first cdu', cdu)
            return cdu#json.dumps(cdu)
        elif len(rcv_data) == self.nsize:
            print(f'Received: {rcv_data!r}')
            #rcvd = map(json.loads, rcvd_data)
            #pcdu, ccdu = list(map(str.decode, rcvd))
            pcdu, ccdu =  rcv_data

            print('pcdu:\n', pcdu,'ccdu:\n', ccdu)
            #i = 0#self.conf['key'][0]
            #pcdu = json.loads(rcv_data[i]).decode()
            #i = 1#self.conf['key'][1]
            #ccdu = json.loads(rcv_data[i]).decode()
    
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
                    return cdu #json.dumps(cdu)
                else:                   #not sent yet
                    tag = {"ver":self.conf['ver']+1}                                   #real deployment, need GUI for MongoDB to input new conf
                    #tag = {"ver":self.conf['ver']}                                   #real deployment, need GUI for MongoDB to input new conf
                    doc = self.co_conf.find_one(tag)                                  #and check DB for the latest update
                    if doc:
                        print("got 'conf' from DB")
                        doc.pop('_id')                                             #drop MongoDB specific header #self.tmp = copy.deepcopy(self.conf)
                        tmp = copy.deepcopy(doc)
                    else:
                        print(f"no version {self.conf['ver']} found in DB, use current", self.conf)
                        tmp =  self.tmp                                             #copy.deepcopy(self.conf)

                    cdu = self.cdu0(self.state['seq']+1, tmp) 
                    self.state['sent'] = True                                       #first step of hand-shake
                    return cdu                                                      #json.dumps(cdu)
            else:
                return {}
                #tmp = copy.deepcopy(self.conf)
                #cdu = self.cdu0(self.state['seq']+1, tmp) 
                #return cdu# json.dumps(cdu)

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
#ipv4p="192.168.1.37"    #lenovo T450

ips = ["127.0.0.1", "127.0.0.1"]    #c-plane addresses
ips = ["192.168.1.37", "192.168.1.37"]    #c-plane addresses
ports = [5555, 6666]                #c-plane ports
hub_ip = "127.0.0.1"                #u-plane address
sub_port = 5570                     #u-plane ports
pub_port = 5568

CONF = {"ips":ips, "ports": ports, "id":0,  "key":[1,2], "dly":0, "ver": 0,  'cnt':20, "mode":0, "uperiod": 0}
P_CONF = {'hub_ip':hub_ip, 'sub': (sub_port, 6), 'pub': (pub_port,4), 'key':[1, 2], 'ctraddr':(CONF['ips'][0], CONF['ports'][0]), 'dly':1., 'maxlen': 4, 'mode': 0}
C_CONF = {'hub_ip':hub_ip, 'sub': (sub_port, 4), 'pub': (pub_port,6), 'key':[1, 2], 'ctraddr':(CONF['ips'][1], CONF['ports'][1]), 'dly':1., 'maxlen': 4, 'mode': 0}
CONF['conf'] = {'p': P_CONF, 'c':C_CONF}

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
    if '-clt' in sys.argv:
        s=Master(CONF['ips'], CONF['ports'],CONF['id'])
        s.client()
        exit()
    if len(sys.argv) > 1:
        CONF['mode'] = int(sys.argv[1])
        print(sys.argv)
        inst=MasterContr(CONF)
        inst.client()
        inst.close()
    elif len(sys.argv) > 2:
        print(sys.argv)
        print('usage: python3 controller.py (mode 0, 1,2,3,4, or default mode 0')
        print('usage: python3 controller.py -testdb/prepdb')
        exit()
    else:                       #default mode 0
        print(sys.argv)
        inst=Controller(CONF)
        inst.client()
        inst.close()
