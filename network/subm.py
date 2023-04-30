'''
sub.py is a stand-alone subscriber 
configured by CONF
packet from each arriving channel is put into an individual buffer queue[n_subtopics], added with arrival time
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
        if 'maxlen' in self.conf:
            self.queue ={key: deque(maxlen=self.conf['maxlen']) for key in self.conf['subtopics']}
        else: #self.queue = deque([])
            self.queue ={key: deque([]) for key in self.conf['subtopics']}

        self.id = conf['sub_id']
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        #self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        #self.socket.setsockopt(zmq.SNDHWM, 2) #water mark set to 2

    def close(self):
        self.socket.close()
        self.context.term()
        print('test_sub socket closed and context terminated')

    #def subscriber(self, fifo =None, mutex=None): 
    def subscriber(self):#, fifo =None): 
        '''
        if fifo == None:
            fifo = self.queue
        elif len(fifo) != len(self.conf['subtopics']):
            print("provided #fofos is less than required", self.conf['subtopics'])
            exit()
        '''

        for topic in self.conf['subtopics']: #topicfilter = str(topic)  #because we used integer for topics
            self.socket.setsockopt_string(zmq.SUBSCRIBE, str(topic))
            print('{} {} subscribes to port {} on topics {}'.format(self.conf['name'], self.id, self.conf['sub_port'], topic))
        while True:
            bstring = self.socket.recv()
            slst= bstring.split()
            topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata)
            '''
            if mutex:
                mutex.acquire()
                self.sub_handler(message, topic, fifo)
                mutex.release()
            else:
            '''
            self.sub_handler(message, topic)#, self.queue)


    def sub_handler(self, message, topic):#, queue):
        queue = self.get_lstqueue()
        if 'sdu' in message:
            if self.conf['tstmp']:         #add receive time to the packet
                message['sdu'][f'rtm{self.id}'] = time.time_ns() #arrival time
            if len(queue[topic]) < queue[topic].maxlen: 
                queue[topic].append(message['sdu'])
                result = '{} sid={} received for chan{} and buffered {} '.format(self.conf['name'], self.id, topic, message)
            else: 
                result = '{} sid={} received for chan{} {}, buffer full'.format(self.conf['name'], self.id, topic, message)
        else:
            result = '{} sid={} received for chan{}: {}, no sdu '.format(self.conf['name'], self.id, topic, message)

        if self.conf['print']: print(result)

        time.sleep(self.conf['dly'])

    #def output_loop(self, queue= None, mutex=None):
    #def output_loop(self, queue= None):
        '''
        if queue == None:
            queue = self.queue
        elif not isinstance(queue, list) or len(queue) != len(self.conf['subtopics']):
            print('internal buffer is not defined in subm.output()')
            exit()
        ''' 
    def get_lstqueue(self):
        return self.queue
    def output_loop(self):#, queue= None):
        lstqueue = self.get_lstqueue()
        print('---- received from fifos:', lstqueue)# self.queue)
        while True:
            '''
            if mutex:
                mutex.acquire()
                self.output()
                mutex.release()
            else:
            '''
            self.output(lstqueue)#self.queue)

    def output(self, queue):
        for key in self.conf['subtopics']:
            if len(queue[key]) >0: 
                print(f'node {self.id} receivd fom fifo:', queue[key].popleft())
            else: 
                time.sleep(self.conf['dly']) 
                print(f'node {self.id} fifo[{key}] is empty', queue[key])


#-------------------------------------------------------------------------
CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':2, 'dly':1., 'name': 'Client', 'tstmp': True, 'maxlen':10, 'print': False}
#CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[0,1,2,3,4], 'sub_id':2, 'dly':1., 'name': 'Client', 'tstmp': True, 'maxlen':10, 'print': True}
if __name__ == "__main__":
    ''' multiprocessing does not work, it seems to have probem to access the Q from two different processes
    from multiprocessing import Process
    process= [Process(target=inst.output, args=(Q,)), Process(target=inst.subscriber, args=(Q,))]
    process= [Process(target=inst.subscriber, args=(Q,)),Process(target=inst.output, args=(Q,))]
    for t in process: t.start()
    '''
    print(sys.argv)
    if len(sys.argv) > 1:
        print("usage: python3 sub.py")
        exit(0)
    inst=Sub(CONF) #if len(sys.argv) >1 and sys.argv[1]=='-ex': #access result from out side the process
    if CONF['print']:   #using sdio
        inst.subscriber()
    else:               # load results to an external buffer Q and print
        Q = {name: deque(maxlen=4) for name in CONF['subtopics']}
        from threading import Thread, Lock
        lock = None #Lock()
        #thread = [Thread(target=inst.subscriber, args=(Q,)), Thread(target=inst.output_loop, args=(Q,))]
        #thread = [Thread(target=inst.subscriber, args=(Q,lock,)), Thread(target=inst.output_loop, args=(Q,lock,))]
        thread = [Thread(target=inst.subscriber), Thread(target=inst.output_loop)]
        for t in thread:
            t.start()
        for t in thread:
            t.join()
    inst.close()
