'''
controller.py
    works with producer.py and consumer.py
    configured by CONF, or obtain CON from DB
    assumes medium.py in background
    receives on channel n5, n7
    transmit on channel n105, n107
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

#import pymongo
from pymongo import MongoClient
#import pprint
#import datetime , random

client=MongoClient('localhost', 27017)
#client=MongoClient('mongodb://localhost:27017/')
#db = client.smacs,  col = client['smacs']['conf'], col=client.smacs.conf
db = client['smacs'] 

#==========================================================================
class Controller:
    def __init__(self, conf):
        self.co_state = db['state']
        self.co_conf =  db['conf']
        try:#difference between original conf and retrived from DB is the additional '_id' tag 
            rst = self.co_conf.insert_one(conf.copy())
            print(f'saved conf to {self.co_conf}: ', rst.acknowledged)
            self.conf = self.co_conf.find_one({'ver':0})
        except:
            print(f'cannot save conf to {self.co_conf} :', rst.acknowledged)
            self.conf = conf.copy()
        #print('collections:', db.list_collection_names())

        self.id = self.conf['id']
        self.open()

        print('Controller:', self.conf)
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

        if self.conf['maxlen']:
            self.cdu= {pub: deque(maxlen=self.conf['maxlen']) for pub in self.pubtopics}
        else: 
            self.cdu= {pub: dequeue([]) for key in self.subtopics}

        self.ustart = time.time()+ self.conf['ust']

        self.prod_state={(p[0],p[1]): self.prod_template(p[0], p[2]) for p in self.conf['pset']}
        self.cons_state={(c[0],c[1]): self.cons_template(c[0], c[2]) for c in self.conf['cset']}
        


    def close(self):
        self.cdu.clear()

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()
        print('Controller sockets closed and context terminated')
    # P-CDU
    def prod_template(self, pid, pub_topic): 
        return {'pid': pid, 'p-chan':pub_topic, 'mseq': 0, 'pseq':0, 'ct123':[], 'adapt': dict(),'u': False,  'p-ctime': time.ctime()}
    # C-CDU
    def cons_template(self, cid, pub_topic): 
        return {'cid': cid, 'c-chan':pub_topic, 'mseq': 0, 'cseq':0, 'pt123':[], 'adapt':dict(), 'u': False,  'c-ctime': time.ctime()}
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
            for pub_topic in self.pubtopics: 
                message = self.tx_handler(pub_topic)
                if message:
                    bstring = json.dumps(message)
                    self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 

            time.sleep(self.conf['dly'])
    #handle receive CDU 
    #expected from prod:{'id': , 'chan':, 'peer', 'pseq':,'mseq':, 'ct123':[] }
    #expected from cons:{'id': , 'chan':, 'peer', 'cseq':,'mseq':, 'pt123':[] }
    def rx_handler(self, message, sub_topic):
        if sub_topic == self.conf['sub_ptopic']: #producer n5
            if sub_topic != message['chan']:
                print('channel mismatch, rx_handler ', message)
                return
            key = (message['id'], message['peer'])  #pid,cid
            pub_topic = self.conf['rt'][str(sub_topic)]


            self.prod_state[key]['pseq'] = message['pseq']
            self.prod_state[key]['p-chan'] = message['chan']

            if self.prod_state[key]['mseq'] == message['mseq']: 
                self.prod_state[key]['ct123'] = message['ct123']
                if self.estimate(key):
                    self.prod_state[key]['mseq'] += 1
            
            self.cdu[pub_topic].append(self.prod_state[key])
        elif sub_topic == self.conf['sub_ctopic']:  #consumer on n7
            key = (message['peer'], message['id'])  #pid, cid
            pub_topic = self.conf['rt'][str(sub_topic)]


            self.cons_state[key]['cseq'] = message['cseq']
            self.prod_state[key]['c-chan'] = message['chan']

            if self.cons_state[key]['mseq'] == message['mseq']:
                self.cons_state[key]['pt123'] = message['pt123']
                if self.estimate(key):
                    self.cons_state[key]['mseq'] += 1
        else:
            print('unknown interface', sub_topic)

        self.cdu[pub_topic].append(self.cons_state[key])
        '''
        #medium moved to DB
        '''

        if self.conf['print']: 
            print('Controller id={} received from chan {}: {} '.format(self.id, sub_topic, message))
    #handle transmit CDU
    def tx_handler(self,  pub_topic):
        tx = {'id': self.id, 'chan': pub_topic, 'cdu':dict()}
        if self.cdu[pub_topic]: 
            tx['cdu'] = self.cdu[pub_topic].popleft()
            tx['cdu'].pop('ct123')
            tx['cdu'].pop('adapt')
            if time.time() > self.ustart:
                tx['cdu']['u'] = True



        if self.conf['print']: 
            print("Controller id={} sent {}".format(self.id,  tx))
        return tx

    def estimate(self, index):
        if self.cons_state[index]['mseq'] != self.prod_state[index]['mseq']:
            print('mseq does not mach between n5 and n7')
            return False
        if len(self.cons_state[index]['pt123'])==3 and len(self.prod_state[index]['ct123'])==3:
            dp = self.prod_state[index]['pt123'][1]- self.prod_state[index]['pt123'][0]
            dc = self.cons_state[index]['ct123'][1]- self.cons_state[index]['ct123'][0]
            adapt = {'offset':  (dp-dc)/2.0, 'latncy': (dp+dc)/2.0, 'bp':0, 'bc':0}
            self.prod_state[index]['adapt'] = adapt
            self.prod_state[index]['adapt'].pop('bc')       #for producer only
            self.cons_state[index]['adapt'] = adapt
            self.cons_state[index]['adapt'].pop('bp')       #for consumer only
            #store to DB the measurement states for producer and consumer 

            self.store(index)
            return True
        else:
            print('incomplete data', self.prod_state[index]['ct123'], self.cons_state[index]['pt123'])
            return False
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
CONF = {"ipv4":"127.0.0.1" , "sub_port": "5570", "pub_port": "5568", "id":0, 'dly':1., "ust": 0, "maxlen": 4, "print": True, "ver": 0}
CONF.update({"rt": {"5":105, "7":107}, "sub_ptopic": 5, "sub_ctopic":7, "pset": [(1,2,4)], "cset":[(1,2,6)]}) 
#'ust': U start time (wait time before starting U-interface)
#'rt': route 'n5' to 'n105', and 'n7' to 'n107'
#'pset'=list((pid, cid, pub_ptopic))
#'cset'=list((pid, cid, pub_ctopic))
if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) > 1:
        print('usage: python3 controller.py')
        exit()
    inst=Controller(CONF)
    inst.run()
    inst.close()
