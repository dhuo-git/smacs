'''
pubm.py is a stand-alone publisher, to be used by end points, such as producer.py or consumer.py
configured by CONF shown below in test section
code runs only when python3 hub.py -fwd is active in background
each channel has individual buffer: self.queue[n_topics], allowing more user share the same publisher
4/30/2023/nj
'''
import zmq 
import time, sys,json, os
from collections import deque
#==========================================================================
#PFS_CONF = {'fwd_port':"5566" , 'pub_port': "5568", '"sub_port': "5570", 'pubtopics':[0,1,2,3,4], 'subtopics':[0,4]}

class Pub:
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Pub-Conf', self.conf)
        self.context = zmq.Context()

        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        self.id = self.conf['pub_id']

        #self.socket.setsockopt(zmq.SNDHWM, 2)

    def prepare(self):
        if self.conf['maxlen']:
            self.queue = {name: deque(maxlen=self.conf['maxlen']) for name in self.conf['pubtopics']}   #input data FIFO buffer 
        else: 
            self.queue = {name: deque([]) for name in self.conf['pubtopics']}   #input data FIFO buffer, no limit
        self.seq = {name:name for name in self.conf['pubtopics']}

    #in case data is to be imported from outside
    def get_lstqueue(self):
        return self.queue

    def publisher(self, lstfifo =None):
        if lstfifo == None or not isinstance(lstfifo, dict):
            self.prepare()
            print('prepared in publisher()')
        elif not isinstance(lstfifo, dict) or len(lstfifo) != len(self.conf['pubtopics']):
            print('mismatch in input buffers in pubm.publisher()')
            exit()
        else:
            self.queue = lstfifo
            print('imported_fifo or is_not_origin', lstfifo)

        for topic in self.conf['pubtopics']:
            print('{} {} publishes to port {} on topics {}'.format(self.conf['name'], self.id, self.conf['pub_port'], topic))

        if 'rounds' in self.conf:
            for _ in range(self.conf['rounds']):
                message = self.pub_handler(topic)
                bstring = json.dumps(message)
                self.socket.send_string("%d %s"% (topic, bstring)) 
            self.close()
            print('closed after sent:', self.conf['rounds'], self.socket.closed)
            return

        while True: 
            for topic in self.conf['pubtopics']:
                message = self.pub_handler(topic)
                bstring = json.dumps(message)
                self.socket.send_string("%d %s"% (topic, bstring)) 
            time.sleep(self.conf['dly'])

    def pub_handler(self,  topic):#,  queue): #self.conf['sdu']['stm']=time.perf_counter()
        #if self.conf['tstmp']:#queue == None: #internal packet generation sdu = self.generator(topic).popleft()
        #elif self.queue[topic]: sdu = self.queue[topic].popleft()  #extract sdu: queue=fifo (external), queue=self.queue (internal: empty to start)
        # else: sdu = dict()  #no data

        #tx = {'pid': self.id, 'chan': topic, 'sdu': sdu, 'cdu':{f'stm{self.id}': time.time_ns()}} #sdu can be from external, or from self.conf
        #elif self.queue[topic]:
        tx = {'id': self.id, 'chan': topic, 'cdu':self.cdu(topic), 'sdu':dict()}
        if  self.queue[topic]:
            tx['sdu']= self.queue[topic].popleft() 
        else:
            tx['sdu']= {}
            self.generate(topic)


        if self.conf['print']: print("{} id={} sent {}".format(self.conf['name'], self.id,  tx))
        return tx

    def cdu(self, pub_topic):
        cdu ={'id': self.id, 'chan': pub_topic, 'seq': self.seq[pub_topic], f'stm{self.id}': time.time_ns()} 
        self.seq[pub_topic] += 1
        return cdu

    def generate(self, key):
        self.queue[key].append({f'date{self.id}': time.ctime()})
        return self.queue[key]

    def close(self):
        self.socket.close()
        self.context.term()
        print('pub socket closed and context terminated')
#------------------------------ TEST -------------------------------------------
CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[0], 'pub_id':1,'dly':2., 'name': 'Server','tstmp':True,  'maxlen': 4, 'sdu':{'seq':0}, 'print': True} #template configuration 
#CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[0,1,2,3,4], 'pub_id':1,'dly':2., 'name': 'Server','tstmp':True,  'maxlen': 4, 'sdu':{'seq':0}, 'print': True} #template configuration 

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) >1:
        CONF['rounds'] = int(sys.argv[1])
    conf=CONF
    inst=Pub(conf)
    inst.publisher()
    inst.close()
