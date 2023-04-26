
'''
hub.py <= ring.py = node.py
contains a single class PubFwdSub(), which comprises functions
1. .forwarder(), serving as the hub queue
2. .subscriber(), keeping  the client loop by subscribing on multiple topics
3. .publisher(), keeping the server loop by publishing on multiple topics
4. .pubsub(), maitain simultaneous sub-loop and pub-loop as parallel threas/processes (in one shell stdout)
'''
import zmq 
import time, json, os, sys
from  multiprocessing import Process
from threading import Thread
#==========================================================================
class PubFwdSub:
    '''Topic is a string as ASCII
       configuration paramter:  conf=dict()
       module subscriber: front end, facing client
       module publsiher:  back end, facing server
       module forwarder:  queue from publisher to subscriber (poses as sub to publisher and pub to subscriber)
       module pub_and_sub: publisher and subscriber work alternately in a single loop 
       module pubsub: publisher and subscriber works simultaneously in two separate threads
        '''
    def __init__(self, conf):
        self.conf = conf.copy()
        print('PubFwdSub', self.conf)
        self.context = zmq.Context()
        self.sub_active = True
        self.pub_active = True

    def subscriber(self, sub_id): 
        print('client {} subsribes from port {} on topics {}'.format(sub_id, self.conf['sub_port'], self.conf['subtopics'][sub_id])) 
        socket = self.context.socket(zmq.SUB)
        #socket.connect (dish, "udp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
        socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
        socket.setsockopt(zmq.SUBSCRIBE, b'')
        socket.setsockopt(zmq.SNDHWM, 2)
        for i in self.conf['subtopics'][sub_id]:
            topicfilter = str(i) 
            socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
            print('client {} subscribes to port {} on topics {}'.format(sub_id, self.conf['sub_port'], topicfilter)) 
        while self.sub_active:
            bstring = socket.recv()
            slst= bstring.split()
            topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            message = json.loads(messagedata)
            self.sub_handler(sub_id, topic, message['sdu'])
            time.sleep(self.conf['dly'])

        socket.close()
        self.context.term()

    #subscriber sink: facing client, consuming received subspription according to topiccs
    def sub_handler(self, subscriber_id, topic,  message):
        print('Client {} receives for topic {}:'.format(subscriber_id, topic), message)

    def publisher(self, pub_id): 
        print('server {} publishes from port {} on topics {}'.format(pub_id, self.conf['pub_port'], self.conf['pubtopics'][pub_id])) 
        socket = self.context.socket(zmq.PUB)
        #socket.connect (dish, "udp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
        socket.setsockopt(zmq.SNDHWM, 2)
        while self.pub_active:
            for topic in self.conf['pubtopics'][pub_id]:
                payload = self.pub_handler(pub_id, topic) 
                message={'sdu': payload, 'topic':topic, 'issuer':pub_id}
                bstring = json.dumps(message)
                socket.send_string("%d %s"% (topic, bstring)) #messagedata = socket.send_json([topic,message])
            time.sleep(self.conf['dly'])
        socket.close()
        self.context.term()

    #publsiher payload: back end facing server #generate publications 
    def pub_handler(self, publisher_id, topic):
        tx = {'pid': publisher_id,'top': topic, 'tm': time.perf_counter()}
        print('Server sends:', tx)
        return tx

    def forwarder(self):
        print('forwarder from publisher {} to subscriber {}'.format(self.conf['pub_port'], self.conf['sub_port']))
        try: 
            frontend = self.context.socket(zmq.SUB) 
            #frontend.bind(dish, "udp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
            frontend.bind("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))
            frontend.setsockopt_string(zmq.SUBSCRIBE, "") # Socket facing services
            backend = self.context.socket(zmq.PUB) 
            #backend.bind(dish,"udp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
            backend.bind("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))
            zmq.device(zmq.FORWARDER, frontend, backend)
        except Exception:
            print ("bringing down zmq device")
            exit(0)
        finally:
            frontend.close()
            backend.close()
            self.context.term()

    #note ! time_out value set >=0 at 
    #setsockopt(zmq.RCVTIMEO,t) for NONBLOCK: using the same context
    #consecutive pub and sub
    def pub_and_sub(self, pub_id, sub_id):
        socket_pub = self.context.socket(zmq.PUB)
        socket_sub = self.context.socket(zmq.SUB)
        #socket_pub.connect(dish, "udp://{0}:{1}s".format(self.conf['ipv4'], self.conf['pub_port']))
        socket_pub.connect("tcp://{0}:{1}s".format(self.conf['ipv4'], self.conf['pub_port']))
        #socket_sub.connect(dish, "udp://{0}:{1}s".format(self.conf['ipv4'], self.conf['sub_port']))
        socket_sub.connect("tcp://{0}:{1}s".format(self.conf['ipv4'], self.conf['sub_port']))
        print('server {} publishes from port {} on topics {}'.format(pub_id, self.conf['pub_port'], self.conf['pubtopics'][pub_id])) 
        # set sub topics
        for i in self.conf['subtopics'][sub_id]:
            topicfilter = str(i) 
            socket_sub.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
            print('Client {} subscribes to port {} on topics {}'.format(sub_id, self.conf['sub_port'], topicfilter)) 
        socket_sub.setsockopt(zmq.RCVTIMEO, 0)
        while True:
            for topic in self.conf['pubtopics'][pub_id]:
                payload = self.pub_handler(pub_id, topic) 
                message={'sdu': payload, 'topic':topic, 'issuer':pub_id}
                bstring = json.dumps(message)
                socket_pub.send_string("%d %s"% (topic, bstring)) 
            time.sleep(self.conf['dly'])
            try:
                bstring = socket_sub.recv()
                slst= bstring.split()
                topic=json.loads(slst[0])
                messagedata =b''.join(slst[1:])
                message = json.loads(messagedata)
                self.sub_handler(sub_id, topic, message['sdu']) 
            except zmq.ZMQError:
                print('Client receives nothing')
        socket_pub.close()
        socket_sub.close()
        self.context.term()
    #simultaneous pub and sub implemented as two Threads, it works also as two Processes
    def pubsub(self, pub_id=0, sub_id=0):
        # threads =[ Process(target=self.subscriber, args=(sub_id,)), Process(target=self.publisher, args=(pub_id,))]
        i = 0
        while True:
            threads =[ Thread(target=self.subscriber, args=(sub_id,)), Thread(target=self.publisher, args=(pub_id,))]
            if i%2:
                threads[0].start()
                threads[1].start()
            else:
                threads[1].start()
                threads[0].start()
            for t in threads: t.join()
            time.sleep(2)

        #Thread(target=self.subscriber, args=(sub_id,), daemon=True).start()
        #self.publisher(pub_id)
#-------------------------------------------------------------------------
#PFS_CONF = {'fwd_port':"5566" , 'pub_port': "5568", 'sub_port': "5570", 'pubtopics':[0,1,2,3,4], 'sub_usrs':[0,1,4], 'dly':1.}
#pub=ord(os.urandom(1))%2 #only 2 options, see CONF nm =os.urandom(2)
#P2F2S_CONF = {'ipv4': '127.0.0.1', 'pub_port': "5568", 'sub_port': "5570", 'pubtopics':[[0,1,2,3,4],[10,11,12,13]], 'subtopics':[[0,1,4], [10,13]], 'dly':2.}

ipv4 = '0.0.0.0' #any ip on the current container or host
AUTOGET = False 
if AUTOGET: #takes ip of the current container or host
    import socket
    ipv4 = socket.gethostbyname(socket.gethostname())

P2F2S_CONF = {'ipv4': ipv4, 'pub_port': "5568", 'sub_port': "5570", 'pubtopics':[[0,1,2,3,4],[10,11,12,13]], 'subtopics':[[0,1,4], [10,13]], 'dly':2.}
P2F2S_CONF = {'ipv4': ipv4, 'pub_port': "5568", 'sub_port': "5570", 'pubtopics':[[0,1,2,3,4],[10,11,12,13]], 'subtopics':[[0,1,4], [10,13]], 'dly':0.}

if __name__ == "__main__":
    print(sys.argv)
    pub,sub=0, 0
    if '-fwd' in sys.argv:
        conf=P2F2S_CONF
        inst=PubFwdSub(conf)
        inst.forwarder()
    elif '-pub' in sys.argv:
        conf=P2F2S_CONF
        inst=PubFwdSub(conf)
        inst.publisher(pub)
    elif '-sub' in sys.argv:
        conf=P2F2S_CONF
        inst=PubFwdSub(conf)
        inst.subscriber(sub)
    elif '-pubsub' in sys.argv:
        conf=P2F2S_CONF
        inst=PubFwdSub(conf)
        inst.pub_and_sub(pub,sub)
    else:
        print('python ring.py -fwd/-pub/-sub/pubsub')
