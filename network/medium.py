
from threading import Thread
from collections import deque
import zmq 
import time, sys,json, os, pprint

import pub, sub

#class PubFwdSub:
class Medium:
    '''Topic is a string as ASCII
       configuration paramter:  conf=dict()
       requirements: 1) hub.py -fwd
                     2) sender.py
                     3) receiver.py
                     4) controller.py

       module sub_and_pub: receives payload on topic==1, receives contr-info on topic==3, processes payload based on contr-info and send result as sdu to topic=2
        '''
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Medium Configuration:', self.conf)
        self.sub_active = True
        self.pub_active = True
        self.queue = deque([]) #len(self.queue) is not limited
        self.tmplat = 0 #temporary latency

        self.context = zmq.Context()
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.connect("tcp://{0}:{1}s".format(self.conf['ipv4'], self.conf['pub_port']))

        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.connect("tcp://{0}:{1}s".format(self.conf['ipv4'], self.conf['sub_port']))

        print('Medium {} publishes on port {} for topics {}'.format(self.conf['pub_id'], self.conf['pub_port'], self.conf['pubtopics'])) 

    #note ! time_out value set >=0 at 
    #setsockopt(zmq.RCVTIMEO,t) for NONBLOCK: using the same context
    #consecutive sub and pub, others than hub.pub_and_sub #def pub_and_sub(self, pub_id, sub_id):

    def sub_and_pub(self):
        threads = [Thread(target=self.sub_loop), Thread(target=self.pub_loop)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.socket_pub.close()
        self.socket_sub.close()
        self.context.term()
        print('sub_and_socket closed and context terminated')

    def sub_loop(self): # set sub topics
        for i in self.conf['subtopics']:
            topicfilter = str(i) 
            self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
            print('Medium {} subscribes to port {} on topics {}'.format(self.conf['sub_id'], self.conf['sub_port'], topicfilter)) 
        socket_sub.setsockopt(zmq.RCVTIMEO, 0)
        while True:
            try:
                bstring = socket_sub.recv()                             #sub receives
                slst= bstring.split()
                topic=json.loads(slst[0])
                messagedata =b''.join(slst[1:])
                message = json.loads(messagedata)
                self.sub_handler(topic, message['sdu']) 
            except zmq.ZMQError:
                print('Medium receiver failed in sub_loop')

    def pub_loop(self):
        while True:
            try:
                for topic in self.conf['pubtopics']:
                    payload = self.pub_handler(topic) 
                    bstring = json.dumps(payload)
                    self.socket_pub.send_string("%d %s"% (topic, bstring))  #pub transmits
            except zmq.ZMQError:
                print('Medium receiver failed in pub_loop')


    #publsiher payload: back end facing server #generate publications 
    def pub_handler(self,  topic):
        tx = {'pid': self.conf['pub_id'], 'mtm': time.perf_counter(), 'topic':topic}#, 'issuer':self.conf['pub_id']} #pdu = sdu+overhead
        if len(self.queue):
            tx['sdu'] = self.queue.popleft()
           

        if self.tmplat:
            time.sleep(self.tmplat) # add needd temporary latency
        print('Medium sends:', tx)
        return tx

    #subscriber sink: facing client, consuming received subspription according to topiccs
    def sub_handler(self,  topic,  sdu):
        print('Medium {} receives sdu for topic {}:'.format(self.conf['sub_id'], topic))
        if sdu and topic == 3: #for controller: remove packet, add latency
            if 'laty' in sdu:     #set current latency, if present, before drop the packet if needed
                self.tmplat = sdu['laty']
            if 'lost' not in sdu: #lost packet, if
                self.queue.append(sdu)
        else:
            self.queue.append(sdu)

'''
#packet format definition:
    pkt ={'pid': publisher_id, 'topic':, 'issuer':, 'stm':, 'sdu', sdu}
    if pid(publisher_id) ==3:
        expect: sdu = {'data': binary_blob, 'laty':, 'lost',:}
    else: #from other topics
        exepcet sdu ={'data': binary_blob}

    received from topic==1
    binary_blob untouched, to be opened by receiver under topic==2
'''

if __name__=="__main__":
    CONF = {'ipv4':'127.0.0.1' , 'sub_port': 5570, 'pub_port': "5568", 'subtopics': [1,3], 'pubtopics':[2], 'pub_id':2, 'sub_id': 2, 'dly':0.}
    inst = Medium(CONF)
    inst.sub_and_pub() 
    """
    sinst = pub.Pub(S_CONF)
    rinst = sub.Sub(R_CONF)
    cinst = sub.Sub(C_CONF)
    threads = [Thread(target=sinst.publisher), Thread(target=rinst.subscriber), Thread(target=cinst.subscriber)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    sinst.close()
    rinst.close()
    cinst.close()
    """

