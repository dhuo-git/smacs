
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
            print('{} {} subscribes to port {} on topics {}'.format(self.conf['name'], self.conf['sub_id'], self.conf['sub_port'], topicfilter)) 
        socket_sub.setsockopt(zmq.RCVTIMEO, 0)
        while True:
            try:
                bstring = socket_sub.recv()                             #sub receives
                slst= bstring.split()
                topic=json.loads(slst[0])
                messagedata =b''.join(slst[1:])
                message = json.loads(messagedata)
                print('received', message)
                time.sleep(self.conf['dly'])
                if 'sdu' in message:
                    self.sub_handler(topic, message['sdu']) 
                else:
                    print('{} {} received no sdu for topic {}:'.format(self.conf['name'],self.conf['sub_id'], topic))
            except zmq.ZMQError:
                print('{} receiver failed in sub_loop'.format(self.conf['name']))

    def pub_loop(self):
        while True:
            try:
                for topic in self.conf['pubtopics']:
                    payload = self.pub_handler(topic) 
                    bstring = json.dumps(payload)
                    self.socket_pub.send_string("%d %s"% (topic, bstring))  #pub transmits
            except zmq.ZMQError:
                print('{} receiver failed in pub_loop'.format(self.conf['name']))

    #publsiher payload: back end facing server #generate publications 
    def pub_handler(self,  topic):
        tx = {'pid': self.conf['pub_id'], 'topic':topic}
        print(self.queue)
        if len(self.queue)>0:
            tx['sdu'] = self.queue.popleft()
        print('{} sent {}'.format(self.conf['name'], tx))
        return tx

    #subscriber sink: facing client, consuming received subspription according to topiccs
    def sub_handler(self,  topic,  sdu):
        self.state(sdu)
        print('{} {} received an sdu for topic {}:'.format(self.conf['name'],self.conf['sub_id'], topic))

    def state(self, sdu): #local state generator
        time.sleep(random.expovariate(self.conf['latency'])) #exponentail delay
        if random.random() > self.conf['loss']: #good packet passes wehn number is greater than loss rate
            sdu['mtm'] = time.time_ns()
            self.queue.append(sdu)
        print(self.queue)



if __name__=="__main__":
    S2R_CONF = {'ipv4':'127.0.0.1' , 'sub_port': 5570, 'pub_port': "5568", 'subtopics': [1,102], 'pubtopics':[2,101], 'pub_id':2, 'sub_id': 2,'dly': 1., 'name':'Medium', 'latency':1.1, 'los': 0.2}
    inst = Medium(S2R_CONF)
    inst.sub_and_pub() 

