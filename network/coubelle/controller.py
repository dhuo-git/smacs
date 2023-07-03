'''
controller.py
    works with producer.py and consumer.py
    configured by CONF, or obtain CON from DB
    assumes medium.py in background
    receives on channel n5, n7
    transmit on channel n105, n107
prerequisites:
    medium.py, mongod
operations :
    1.) listen to consumer (N7) and receives CDU
    2.) listens to producer (N5) and receive CDU 
    3.) transmit CDU to consumer (N107)
    4.) transmit CDU to producer (N105)
    5.) store data to DB, or retrieve data from DB
    6.) receive and incorporate data from Analyzer (tbd)

TX-message: {'id':, 'chan':, 'cdu':}
RX-message: {'id':, 'chan':, 'cdu':}

an interface is uniquely identified by (pid,cid, pubtoic)

    DB name : smacs
    collection: state, conf #e.g. smacs.state.insert(state), smacs.conf.find_one({'key':key})
5/3/2023/nj, laste update 5/29/2023
'''
import zmq 
import time, sys,json, os
from collections import deque
#from threading import Thread
from pymongo import MongoClient

client=MongoClient('localhost', 27017)
db = client['smacs'] 

#==========================================================================
class Controller:
    def __init__(self, conf):
        self.co_state = db['state']
        self.co_conf =  db['conf']
        tag = {'ver':0}
        conf.update(tag)
        try:#difference between original conf and retrived from DB is the additional '_id' tag 
            doc =  self.co_conf.find_one(tag)
            if doc:
                rst = self.co_conf.replace_one(tag, conf.copy())
                print(f'saved conf to {self.co_conf}: ', rst.acknowledged)
                self.conf = self.co_conf.find_one(tag)
            else:
                rst = self.co_conf.insert_one(conf.copy())
                print(f'saved conf to {self.co_conf}: ', rst.acknowledged)
                self.conf = self.co_conf.find_one(tag)
                #self.conf = conf.copy()
        except:
            print('collections:', db.list_collection_names())
            self.conf = conf.copy()

        print('Controller:', self.conf)
        self.id = self.conf['id']
        self.open()
        self.matched = False

        print('sub=', self.subtopics, 'pub=', self.pubtopics)


    def open(self):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        self.subtopics = [self.conf["sub_ptopic"], self.conf["sub_ctopic"]]     #n5,n7
        for sub_topic in self.subtopics:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(sub_topic))

        self.pubtopics = [self.conf["rt"][str(key)] for key in self.subtopics]       #n105,n107
        #outgoing CDUs for all pubtopics, buffer of size 1
        self.cdu ={topic: dict() for topic in self.pubtopics}

        self.ustart = time.time()+ self.conf['ust']     #start timer for U-inteface traffic
        self.txtime = time.time()+ self.conf['txslot']         #enable less TX and more RXs

        self.prod_state=dict()
        self.cons_state=dict()
        for p in self.conf['pids']:
            for c in self.conf['cids']:
                self.prod_state[(p,c)]=self.prod_template(p, self.conf['ptopic'])
                self.cons_state[(p,c)]=self.cons_template(c, self.conf['ctopic'])

        print('pub:', self.pubtopics)
        print('sub:', self.subtopics)
        
        print('prod_state:', self.prod_state)
        print('cons_state:', self.cons_state)
    def close(self):
        self.prod_state.clear()
        self.cons_state.clear()
        self.cdu.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('Controller sockets closed and context terminated')
    # P-CDU
    def prod_template(self, pid, pub_topic): 
        return {'pid': pid, 'p-chan':pub_topic, 'mseq': 0, 'pseq':0, 'ct123':[], 'adapt': dict(),'u': False, 'p-ctime': time.ctime(self.txtime)}
    # C-CDU
    def cons_template(self, cid, pub_topic): 
        return {'cid': cid, 'c-chan':pub_topic, 'mseq': 0, 'cseq':0, 'pt123':[], 'adapt':dict(), 'u': False, 'c-ctime': time.ctime(self.txtime)}

    def run(self): #respond-receive
        while True: 
            #rx message
            bstring = self.sub_socket.recv()
            slst= bstring.split()
            sub_topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata) 
            if message and message['cdu']:
                self.rx_handler(message['cdu'], sub_topic)

            #tx message
            if time.time() >= self.txtime or self.matched:  #set timer to give time for producer and consumer to respond, unless already received both
                self.txtime = time.time()+ self.conf['txslot'] #reset timer
                for pub_topic in self.pubtopics: 
                    message = self.tx_handler(pub_topic)
                    bstring = json.dumps(message)
                    self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 

            #time.sleep(self.conf['dly'])
    #handle receive CDU 
    #expected from prod:{'id': , 'chan':, 'peer', 'pseq':,'mseq':, 'ct123':[] }
    #expected from cons:{'id': , 'chan':, 'peer', 'cseq':,'mseq':, 'pt123':[] }
    def rx_handler(self, cdu, sub_topic):
        if self.conf['print']: 
            print('Controller id={} received on chan {}: {} '.format(self.id, sub_topic, cdu))
        #print(f'received on {sub_topic}:', cdu)
        if sub_topic == self.conf['sub_ptopic']: #producer n5
            key = (cdu['id'], cdu['peer'])  #pid,cid
            print('from producer:', key)
            if time.time() > self.ustart and not self.prod_state[key]['u']: 
                self.prod_state[key]['u'] = True
                print('warm up timer expired and let N4 start:', self.prod_state)

            if cdu['pseq'] > self.prod_state[key]['pseq']:
                self.prod_state[key]['pseq'] = cdu['pseq']

                if len(cdu['ct123'])==3 and cdu['mseq'] > self.prod_state[key]['mseq']:
                    self.prod_state[key]['ct123'] = cdu['ct123']
                    self.estimate(key)
                    if self.matched:
                        self.prod_state[key]['mseq'] = cdu['mseq']
                        print('Ctroller: measured:', self.cons_state[key])
                    else:
                        print('wait for pt123')
                        self.prod_state[key]['mseq'] = max(self.cons_state[key]['mseq'], self.prod_state[key]['mseq'])
                        self.prod_state[key]['ct123'].clear()
                else:
                    print('insufficient ct123')

                pub_topic = self.conf['rt'][str(sub_topic)]
                self.cdu[pub_topic] = self.prod_state[key]      #overwrite transmit buffer

        elif sub_topic == self.conf['sub_ctopic']:  #consumer on n7
            key = (cdu['peer'], cdu['id'])  #pid, cid
            print('from consumer:', key)
            if time.time() > self.ustart and not self.cons_state[key]['u']: 
                self.cons_state[key]['u'] = True
                print('warm up timer expired and let N6 start:', self.prod_state)

            if cdu['cseq'] > self.cons_state[key]['cseq']:
                self.cons_state[key]['cseq'] = cdu['cseq']

                if len(cdu['pt123'])==3 and cdu['mseq'] > self.cons_state[key]['mseq']:
                    self.cons_state[key]['pt123'] = cdu['pt123']
                    if not self.matched: 
                        self.estimate(key)
                    if self.matched:
                        self.cons_state[key]['mseq'] = cdu['mseq']
                        print('Ctroller: measured:', self.cons_state[key])
                    else:
                        print('wait for ct123')
                        self.cons_state[key]['mseq'] = max(self.cons_state[key]['mseq'], self.prod_state[key]['mseq'])
                        self.cons_state[key]['pt123'].clear()
                else:
                    print('insuffiient pt123')

                pub_topic = self.conf['rt'][str(sub_topic)]
                self.cdu[pub_topic] = self.cons_state[key]          #overwrite transmit buffer

    #handle transmit CDU
    def tx_handler(self,  pub_topic):
        cdu = self.cdu[pub_topic]
        cdu['mseq'] += 1
        self.matched = False
        if self.conf['print']: print("Controller id={} sent CDU {}".format(self.id,  cdu))
        return  {'id': self.id, 'chan': pub_topic, 'cdu': cdu}

    ''' 
    def resynchronize(self):
        self.prod_state[index]['reset'] = True
        self.cons_state[index]['reset'] = True
        self.prod_state[index]['mseq'] = 0
        self.cons_state[index]['mseq'] = 0
    '''
    def estimate(self, index):
        if self.cons_state[index]['mseq'] != self.prod_state[index]['mseq']:
            self.matched = False
            print('mseq does not mach between n5 and n7')
        elif len(self.cons_state[index]['pt123'])==3 and len(self.prod_state[index]['ct123'])==3:
            dp = self.prod_state[index]['pt123'][1]- self.prod_state[index]['pt123'][0]
            dc = self.cons_state[index]['ct123'][1]- self.cons_state[index]['ct123'][0]
            adapt = {'offset':  (dp-dc)/2.0, 'latncy': (dp+dc)/2.0, 'bp':0, 'bc':0}
            self.prod_state[index]['adapt'] = adapt
            self.prod_state[index]['adapt'].pop('bc')       #for producer only
            self.cons_state[index]['adapt'] = adapt
            self.cons_state[index]['adapt'].pop('bp')       #for consumer only
            self.matched = True
            self.store(index)
            #stored to DB the measurement states for producer and consumer 
        else:
            print('incomplete data', self.prod_state[index]['ct123'], self.cons_state[index]['pt123'])
        #return self.matched 
    #store states in DB
    def store(self, key):
        if self.prod_state[key]['mseq']== self.cons_state[key]['mseq']:# and self.prod_state[key]['mseq'] > self.mseq:
            record = {'peers': key, **self.prod_state[key], **self.cons_state[key]}

            rst=self.co_state.insert_one({'tag': 1, 'data':record})
            if rst.acknowledged:
                print('state saved to DB', rst.inserted_id)

            self.prod_state[key].clear()
            self.cons_state[key].clear()
            print('saved to DB:', rst.insert_id)
            conf = None
            try:
                conf = self.co_conf.co_conf.find_one()
            except:
                print('cannot access conf data in DB')
            finally:
                if conf and conf['conf'] != self.conf:
                    self.conf = conf['conf'].copy()
                else:
                    print('conf is not updated by DB')
                



#------------------------------ TEST -------------------------------------------
CONF = {"ipv4":"127.0.0.1" , "sub_port": "5570", "pub_port": "5568", "id":0, "txslot":2.0,  "ust": 3600, "maxlen": 4, "print": True, "ver": 0}
#CONF = {"ipv4":"127.0.0.1" , "sub_port": "5570", "pub_port": "5568", "id":0, 'dly':1.,"txslot":1.0,  "ust": 3, "maxlen": 4, "print": True, "ver": 0}
CONF.update({"rt": {"5":105, "7":107}, "sub_ptopic": 5, "sub_ctopic":7, "ptopic": 4, "ctopic": 6,  "pids":[1], "cids":[2]}) 
#'ust': U start time (wait time before starting U-interface)
#'rt': route 'n5' to 'n105', and 'n7' to 'n107', return path for sub_topics
#'sub_ptoic: subscribed topic from producer
#'sub_ctoic: subscribed topic from consumer
#'ptopic: outgoing interface for producer
#'ctopic: outgoing interface for consumer 
#'pids': list of producer nodes, the source nodes
#'cids': list of consumer nodes, the sink nodes, where pids and cids are paired in bipartitie
#
if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) > 1:
        print('usage: python3 controller.py')
        exit()
    inst=Controller(CONF)
    inst.run()
    inst.close()
