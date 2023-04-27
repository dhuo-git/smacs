'''
sub.py is a stand-alone subscriber to be used by hubtest.py
configured by CONF
This serves the purpose of unitest (ut.py), together with hubtest.py, sub.py
'''
import zmq 
import time
import sys, json, os
#==========================================================================
class Sub:
    '''Topic is a string as ASCII '''
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Sub-Conf', self.conf)
        self.context = zmq.Context()
        self.sub_active = True
        if 'maxlen' in self.conf:
            self.queue = deque(maxlen=self.conf['maxlen'])
        else:
            self.queue = deque([])

        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        #self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        #self.socket.setsockopt(zmq.SNDHWM, 2) #water mark set to 2

    def close(self):
        self.socket.close()
        self.context.term()
        print('test_sub socket closed and context terminated')

    def subscriber(self, queue=None): 
        if queue == None:
            queue = self.queue
        for i in self.conf['subtopics']:
            topicfilter = str(i) 
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
            print('{} {} subscribes to port {} on topics {}'.format(self.conf['name'], self.conf['sub_id'], self.conf['sub_port'], topicfilter))
        while self.sub_active:
            bstring = self.socket.recv()
            slst= bstring.split()
            topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata)
            self.sub_handler(message, queue)

    def sub_handler(self, message,  queue):
        if 'sdu' in message:
            message['sdu']['atm'] = time.time_ns() #arrival time
            if queue.maxlen:
                if len(queue) < queue.maxlen:
                    queue.append(message['sdu'])
                    print('{} sid={} received and buffered {} '.format(self.conf['name'], self.conf['sub_id'], message))
                else:
                    print('{} sid={} received {}, buffer full'.format(self.conf['name'], self.conf['sub_id'], message))
            else:
                print('{} sid={} received {}, no buffer'.format(self.conf['name'], self.conf['sub_id'], message))
        else:
            print('{} sid={} received {} '.format(self.conf['name'], self.conf['sub_id'], message))

        time.sleep(self.conf['dly'])




#-------------------------------------------------------------------------
#CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':node_id, 'dly': latency for dev, 'name': node_name}
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':1, 'dly':1., 'name': 'Client', 'maxlen':10}
from collections import deque
if __name__ == "__main__":
    print(sys.argv)
    inst=Sub(CONF)
    Q = deque(maxlen=2)
    inst.subscriber()
    inst.close()
