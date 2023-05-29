'''
medium.py 

    this is a simuilator for channel condition between producer and receiver
    works for producerm with consumerm,  or for producer with consumer to gether with controller

    it forwards incoming traffic (sub) to outgoing traffic (pub), w/o impairment given by parameter CONF['medium']
    configured either by DB or locally by CONF, it can be made async in the future

4/30/2023/nj, last update 5/29/2023/nj

'''
import hub 
from threading import Thread
import zmq, json, time, random
from pymongo import MongoClient

client=MongoClient('localhost', 27017)


class Medium:
    def __init__(self, conf):
        if conf['medium']:
            self.co = client['smacs']['medium']

            try:#difference between original conf and retrived from DB is the additional '_id' tag 
                self.conf = self.co.find_one({'ver':0})
                rst = co.replace_one({"ver":0}, conf)
                print(f'upserted to {self.co}: ', rst.upserted_id, rst.acknowledged) #
            except:
                rst = self.co.insert_one(conf.copy())
                print(f'saved conf to {self.co}: ', rst.inserted_id,  rst.acknowledged)
            finally:
                self.losscnt = 0                                                #counter for the lost packets
                self.latency = []                                               #list of latency values

            self.conf = self.co.find_one({'ver':0})
            if self.conf['medium']['stdev']:
                self.timer = time.time() + random.expovariate(self.conf['medium']['lambda'])    #static latency
        else:
            self.conf = conf.copy()
        #connection
        self.context = zmq.Context()

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['sub_port']))

        #for sub_topic in self.conf['subtopics']:
        for sub_topic in self.conf['rt']:
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, sub_topic)
            print('{} {} subscribes to port {} on topics {}'.format('Medium', self.conf['id'], self.conf['sub_port'], sub_topic))


        print('Medium configured', self.conf)
        self.state = self.template()
        #self.seq = 0                                        #used for communication with Controller only

    def template(self):
        tmp = dict()
        for sub_topic in self.conf['rt']: #for topic in self.conf['rt'][key]:# [self.conf['sub_ptopic'], self.conf['sub_ctopic'], self.conf['sub_ctr']]:
            pub_topic =  str(self.conf['rt'][sub_topic])
            tmp[pub_topic]={'id': self.conf['id'], 'chan': pub_topic}
        print(tmp)
        return tmp


    def run(self):
        while True: 
            bstring = self.sub_socket.recv()
            slst= bstring.split()
            sub_topic=json.loads(slst[0])
            messagedata =b''.join(slst[1:])
            received = json.loads(messagedata)
            if received:
                message = self.subpub_handler(received, sub_topic)

        self.sub_socket.close()
        self.pub_socket.close()
        self.context.term()

    def subpub_handler(self, message, sub_topic):
        #conver producer in to out
        ssub_topic = str(sub_topic)
        if sub_topic == self.conf['sub_ptopic']:
            if self.conf['medium']:
                self.lossy_u_from_p(str(self.conf['rt'][ssub_topic]), message)
            else:
                self.u_from_p(str(self.conf['rt'][ssub_topic]), message)
        #conver consumer in to out
        elif sub_topic == self.conf['sub_ctopic']:
            self.u_from_c(str(self.conf['rt'][ssub_topic]), message)
        else:
            print('should not get here: observed', sub_topic, message) 

        pub_topic = self.conf['rt'][ssub_topic]  #int
        tx_message = self.state[str(pub_topic)]
        bstring = json.dumps(tx_message)
        self.pub_socket.send_string("%d %s"% (pub_topic, bstring)) 

        if self.conf['print']: 
            print('received', message, '\n sent:', tx_message)

    def sojourn(self):
        if time.time() > self.timer:
            self.timer = random.expovariate(self.conf['medium']['lambda'])+time.time()
            self.latency.append(max(random.gauss(self.conf['dly'], self.conf['medium']['stdev']), 0.))
            time.sleep(self.latency[-1])
    #receive on U from producer
    def lossy_u_from_p(self, pub_topic, message):
        if random.random() < self.conf['medium']['lossrt']: 
            self.losscnt += 1
            self.state[pub_topic]['sdu'] = {}
            self.state[pub_topic]['cdu'] = {}
        else:
            self.u_from_p(pub_topic, message)

        if self.conf['medium']['stdev']: self.sojourn()

    def u_from_p(self, pub_topic, message):
        self.state[pub_topic]['sdu'] = message['sdu']
        self.state[pub_topic]['cdu'] = message['cdu']

    #receive on U from consumer 
    def u_from_c(self, pub_topic, message):
        self.state[pub_topic]['cdu'] = message['cdu']
#------------------TEST----------------
'''
as a forwarder, it handles U-traffic only, given by CONF['rt']={sub:pub} pairs
parameter 'dly' is used for latency model only
'''
if __name__ == "__main__":
    CONF= {'ipv4':"127.0.0.1" , 'sub_port': 5570, 'pub_port': 5568, 'id': 100,  'dly': 1., 'ver':0, 'medium':None, 'print': True}
    CONF.update({'rt': {'4': 104, '6': 106}, 'sub_ptopic': 4, 'sub_ctopic': 6})
    #CONF['medium'] ={'lambda':1000., 'stdev': 1, 'lossrt': 0.005}
    #CONF['medium'] ={'lambda':1000., 'stdev': 0, 'lossrt': 0}
    
    med= Medium(CONF)
    #med.run() # for manual operation and need python3 hub.py -fwd
    fwd=hub.PubFwdSub(hub.P2F2S_CONF)
    thread = [Thread(target=med.run), Thread(target=fwd.forwarder)]
    for t in thread:
        t.start()
    for t in thread:
        t.join()

