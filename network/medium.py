'''
medium.py 
    routes incoming traffic (sub) to outgoing traffic (pub)

topology: producerm/producer - medium - consumerm/consumer;  later: producer-controller-consumer

differentiation between traffic and signalling is made based on the following  numbering scheme:

    traffic channel numer: 4*i (first leg) and 4*i + 2(second leg)
    signalling channel number: 4*i+1 (first leg) and 4*i+3 (second leg)
    exception: 0,1,2,3 are reserved for internal use by the controller
    
can be improved with async in the future for more incoming and outing traffics

It also included the hub server in multi-threading

4/30/2023/nj, last update 4/30/2023/nj

'''
import hub 
from threading import Thread
import zmq, json, time, random

class Medium:
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Medium configured', self.conf)

        self.context = zmq.Context()

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        for sub_topic in self.conf['subtopics']:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, str(sub_topic))
            print('{} {} subscribes to port {} on topics {}'.format(self.conf['name'], self.conf['id'], self.conf['sub_port'], sub_topic))

    def run(self):
        while True: 
            bstring = self.sub_socket.recv()
            slst= bstring.split()
            sub_topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            received = json.loads(messagedata)
            self.pub_handler(received)

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()

    def pub_handler(self, message):
        if 'chan' in message:
            sub_topic = message['chan']
            tx ={'id': self.conf['id'], 'from': sub_topic, 'sdu': {}} 

            if not (sub_topic< 4 or sub_topic%2>0): 
                if random.random() < self.conf['loss']: 
                    tx['state'] = 'lost'
                else: 
                    tx['state'] = 'passed'
                    time.sleep(random.expovariate(self.conf['lambda'])) #random latency is  exponentailly distributed 
        else: 
            tx ={} 
        try:
            for pub_topic in self.conf['route'][sub_topic]: 
                tx['to'] = pub_topic
                if 'sdu' in message: 
                    tx['sdu']  = message['sdu'].copy()

                bstring = json.dumps(tx)
                self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 

            time.sleep(self.conf['dly'])
        except:
            print('publishing error in Medium')
        finally:
            if self.conf['print']: print('in medium:', tx)
#------------------TEST----------------
'''
total received topics must equal to total to transmitted topics: 
'''
if __name__ == "__main__":
    CONF= {'ipv4':"127.0.0.1" , 'sub_port': 5570, 'pub_port': 5568, 'id': 2, 'subtopics':[4,106], 'route': {4:[6], 106: [104]}, 'dly': 1., 'name':'Medium', 'lambda':10., 'loss': 0.2, 'print': True}
    #-----
    med= Medium(CONF)
    #med.run() # for manual operation and need python3 hub.py -fwd
    fwd=hub.PubFwdSub(hub.P2F2S_CONF)

    thread = [Thread(target=med.run), Thread(target=fwd.forwarder)]
    for t in thread:
        t.start()
    for t in thread:
        t.join()
