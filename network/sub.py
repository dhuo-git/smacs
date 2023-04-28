'''
sub.py is a stand-alone subscriber to be used by hubtest.py
configured by CONF
This serves the purpose of unitest (ut.py), together with hubtest.py, sub.py
'''
import zmq 
import time
import sys, json, os
from collections import deque
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

    def subscriber(self, fifo =None): 
        if fifo == None:
            fifo = self.queue
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
            self.sub_handler(message, fifo)

    def sub_handler(self, message, fifo):
        if 'sdu' in message:
            message['sdu']['atm'] = time.time_ns() #arrival time
            if fifo.maxlen:
                if len(fifo) < fifo.maxlen:
                    fifo.append(message['sdu'])
                    result = '{} sid={} received and buffered {} '.format(self.conf['name'], self.conf['sub_id'], message)
                else:
                    result = '{} sid={} received {}, buffer full'.format(self.conf['name'], self.conf['sub_id'], message)
            else:
                result = '{} sid={} received {}, no buffer'.format(self.conf['name'], self.conf['sub_id'], message)
        else:
            result = '{} sid={} received {} '.format(self.conf['name'], self.conf['sub_id'], message)

        if self.conf['print']: print(result)

        time.sleep(self.conf['dly'])

    def output(self, queue= None):
        if queue == None:
            queue = self.queue

        print('---- start external output  loop with fifo:', list(queue))
        while True:
            if queue: 
                print('fifo top:', queue.popleft())
            else: 
                if self.conf['print']: print('fifo empty', queue)
                time.sleep(2)

#-------------------------------------------------------------------------
#CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':node_id, 'dly': latency for dev, 'name': node_name}
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':1, 'dly':1., 'name': 'Client', 'maxlen':10, 'print': False}
#CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':1, 'dly':1., 'name': 'Client', 'maxlen':10, 'print': True}
if __name__ == "__main__":
    ''' multiprocessing does not work, it seems to have probem to access the Q from two different processes
    from multiprocessing import Process
    process= [Process(target=inst.output, args=(Q,)), Process(target=inst.subscriber, args=(Q,))]
    process= [Process(target=inst.subscriber, args=(Q,)),Process(target=inst.output, args=(Q,))]
    for t in process: t.start()
    '''
    print(sys.argv)
    inst=Sub(CONF)
    if len(sys.argv) >1 and sys.argv[1]=='-ex': #access result from out side the process
        Q = deque(maxlen=4)
        from threading import Thread
        thread = [Thread(target=inst.subscriber, args=(Q,)), Thread(target=inst.output, args=(Q,))]
        #thread = [Thread(target=inst.subscriber), Thread(target=inst.output)]
        for t in thread:
            t.start()
        for t in thread:
            t.join()
    else:       #keep results inside the process, can be viewed by setting 'print'=True 
        inst.subscriber()
    inst.close()
