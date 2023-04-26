'''
pub.py is a stand-alone publisher, to be used by hubtest.py
configured by CONF
This code is for unittest(ut.py), together with hubtest.py, sub.py
'''
import zmq 
import time, sys,json, os, pprint
#==========================================================================
#PFS_CONF = {'fwd_port':"5566" , 'pub_port': "5568", '"sub_port': "5570", 'pubtopics':[0,1,2,3,4], 'subtopics':[0,4]}

class Pub:
    '''Topic is a string as ASCII '''
    def __init__(self, conf):
        self.conf = conf.copy()
        print('Pub', self.conf)
        self.context = zmq.Context()
        self.pub_active = True
        self.seq =0
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))

        #self.socket.setsockopt(zmq.SNDHWM, 2)
        #if self.conf['buffer']: self.buffer = list()

    def close(self):
        self.socket.close()
        self.context.term()
        print('test_pub socket closed and context terminated')
    def publisher(self): #context = zmq.Context()
        print('{} {} publishes from port {} on topics {}'.format(self.conf['name'], self.conf['pub_id'], self.conf['pub_port'], self.conf['pubtopics'])) 
        while self.pub_active:
            for topic in self.conf['pubtopics']:
                payload = self.pub_handler(topic) 
                message={'sdu': payload}
                bstring = json.dumps(message)
                self.socket.send_string("%d %s"% (topic, bstring)) 
            time.sleep(self.conf['dly'])

    def pub_handler(self,  topic):
        tx = {'pid': self.conf['pub_id'], 'chan': topic, 'seq':self.seq, 'stm': time.perf_counter()}# time.ctime()}
        self.seq += 1
        #if self.conf['buffer']: self.buffer.append(tx) else: 
        print(f"{self.conf['name']} sends", tx)
        return tx

    def pubtest(self):
        for topic in self.conf['pubtopics']:
            payload = self.test_handler(topic) 
            message={'sdu': payload}
            bstring = json.dumps(message)
            self.socket.send_string("%d %s"% (topic, bstring)) 
        print('{} {} publishes from port {} on topics {}'.format(self.conf['name'], self.conf['pub_id'], self.conf['pub_port'], self.conf['pubtopics'])) 
        if topic == self.conf['pubtopics'][-1]:
            return True
        else:
            print('{} {} cannot publish from port {} on topics {}'.format(self.conf['name'], self.conf['pub_id'], self.conf['pub_port'], self.conf['pubtopics'])) 
            return False 
    def test_handler(self, topic):
        tx = {'pid': self.conf['pub_id'],'chan': topic, 'seq': self.seq,'tm': time.ctime()}
        self.seq += 1 
        #if self.conf['buffer']: self.buffer.append(tx) else:
        print(f"{self.conf['name']} sends", tx)
        return tx
#-------------------------------------------------------------------------
#PFS_CONF = {'fwd_port':"5566" , 'pub_port': "5568", 'sub_port': "5570", 'pubtopics':[0,1,2,3,4], 'sub_usrs':[0,1,4], 'dly':1.}
CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[0,1,2,3,4], 'pub_id':1,'dly':1., 'name': 'Server'}
if __name__ == "__main__":
    print(sys.argv)
    conf=CONF
    if '-loop' in sys.argv:
        inst=Pub(conf)
        inst.publisher()
        inst.close()
    else:
        for i in range(2): #if True:
            inst=Pub(conf)
            print('pubtest', inst.pubtest())
            time.sleep(conf['dly'])
            inst.close()
        #if conf['buffer']: pprint.pprint(inst.buffer)
