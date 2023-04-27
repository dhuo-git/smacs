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
        print('Pub-Conf', self.conf)
        self.context = zmq.Context()
        self.pub_active = True
        self.seq =0
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect ("tcp://{0}:{1}".format(self.conf['ipv4'], self.conf['pub_port']))

        #self.socket.setsockopt(zmq.SNDHWM, 2)

    def close(self):
        self.socket.close()
        self.context.term()
        print('pub socket closed and context terminated')
    def publisher(self, queue=None): #context = zmq.Context()
        #print('{} {} publishes from port {} on topics {}'.format(self.conf['name'], self.conf['pub_id'], self.conf['pub_port'], self.conf['pubtopics'])) 
        while self.pub_active:
            for topic in self.conf['pubtopics']:
                message = self.pub_handler(topic, queue) #message={'sdu': payload}
                bstring = json.dumps(message)
                self.socket.send_string("%d %s"% (topic, bstring)) 
            time.sleep(self.conf['dly'])

    def pub_handler(self,  topic, queue): #self.conf['sdu']['stm']=time.perf_counter()
        if queue != None and len(queue)>0:
            self.conf['sdu'] = queue.popleft()
            self.conf['sdu']['mstm']=time.time_ns() #used as inermediate node
        else:
            self.conf['sdu']['stm']=time.time_ns() #used as initial node
        tx = {'pid': self.conf['pub_id'], 'chan': topic, 'sdu': self.conf['sdu']}
        self.conf['sdu']['seq']+=1
        print("{} pid={} sent {}".format(self.conf['name'], self.conf['pub_id'],  tx))
        return tx

#-------------------------------------------------------------------------
CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[0,1,2,3,4], 'pub_id':0,'dly':2., 'name': 'Server', 'sdu':{'stm':0, 'seq':0}} #sdu holder incase no external queue
if __name__ == "__main__":
    print(sys.argv)
    conf=CONF
    inst=Pub(conf)
    inst.publisher()
    inst.close()
