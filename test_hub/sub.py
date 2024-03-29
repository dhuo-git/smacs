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
        print('Sub', self.conf)
        self.context = zmq.Context()
        self.sub_active = True

        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
        #self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        #self.socket.setsockopt(zmq.SNDHWM, 2) #water mark set to 2
        if self.conf['buffer']: self.buffer = list()

    def close(self):
        self.socket.close()
        self.context.term()
        print('test_sub socket closed and context terminated')

    #subscriber: front facing client
    def subscriber(self): #context = zmq.Context()
        print('client {} subsribes from port {} on topics {}'.format(self.conf['sub_id'], self.conf['sub_port'], self.conf['subtopics'])) 
        for i in self.conf['subtopics']:
            topicfilter = str(i) 
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
            print('client {} subscribes to port {} on topics {}'.format(self.conf['sub_id'], self.conf['sub_port'], topicfilter))
        while self.sub_active:
            bstring = self.socket.recv()
            slst= bstring.split()
            topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata)
            self.sub_handler(self.conf['sub_id'], topic, message['sdu'])
            time.sleep(self.conf['dly'])


    def sub_handler(self, sub_id, topic,  sdu):
        if self.conf['buffer']: self.buffer.append(sdu)
        else: print('Client: {} for topic {}  subscriber'.format(sub_id, topic), sdu)

    def subtest(self):
        topic = 0
        if topic in self.conf['subtopics']:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
            bstring = self.socket.recv()
            slst= bstring.split()
            if topic == json.loads(slst[0]):
                message = json.loads(b''.join(slst[1:]))['sdu']
                print('client {} subscribes to port {} on topics {}'.format(self.conf['sub_id'], self.conf['sub_port'], topic)) 
                if self.conf['buffer']: self.buffer.append(message)
            else:
                print('client {} subscribes on topics {}'.format(self.conf['sub_id'], topic)) 
                message = None
        else:
            print('client {} cannot subscribe to port {} on topics {}'.format(self.conf['sub_id'], self.conf['sub_port'], topic))
            message = None
        return message

#-------------------------------------------------------------------------
#PFS_CONF = {'fwd_port':"5566" , 'pub_port': "5568", 'sub_port': "5570", 'pubtopics':[0,1,2,3,4], 'sub_usrs':[0,1,4], 'dly':1.}
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':0, 'dly':1., 'buffer': False}

if __name__ == "__main__":
    print(sys.argv)
    conf=CONF
    if '-loop' in sys.argv:
        inst=Sub(conf)
        inst.subscriber()
        inst.close()
    else:
        for _ in range(3):
            inst=Sub(conf)
            print('received', inst.subtest())
            inst.close()
