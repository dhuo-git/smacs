'''
Media: consists of 
    1) a Sub
    2) a Pub
    3) a med_loop, that takes packet from sub-buffer to pub-buffer and 
    changes the state of the packets that are from channels less than 4 and not even
4/18/2023/nj
'''
import pub, sub
from threading import Thread
import time, random
class Medium:
    def __init__(self, sub_conf, pub_conf, med_conf):
        self.sub_conf = sub_conf
        self.pub_conf = pub_conf
        self.med_conf = med_conf
        print("Medium Conf", self.sub_conf, self.pub_conf, self.med_conf)
        self.sub = sub.Sub(self.sub_conf)
        self.pub = pub.Pub(self.pub_conf)
        self.queue = self.sub.queue #use sub default receive buffer 
    def sub_and_pub(self):
        #threads = [Thread(target=self.sub.subscriber, args=(self.queue,)), Thread(target=self.pub.publisher, args=(self.queue,)), Thread(target=self.medium_loop)]
        threads = [Thread(target=self.sub.subscriber ), Thread(target=self.pub.publisher, args=(self.queue,)), Thread(target=self.medium_loop)]
        for t in threads:
            t.start()
        for t in threads: t.join()

        self.sub.close()
        self.pub.close()
        if self.med_conf['print']: print('sub_and_sub closed and context terminated')
    def medium_loop(self):
        while True:
            if len(self.queue)>0:
                self.state_handler()
            time.sleep(self.med_conf['dly'])
    #channel condition is added to all channels uniformly without differentiation of direction, desitation and origin
    #packets from all channels are queued in a single FIFO: self.queue, assuming symmetrical channel, reflecting network congestion if
    def state_handler(self): 
        if self.queue and 'sdu' in self.queue[0] and (self.queue[0]['sdu']['chan'] < 4 or self.queue[0]['sdu']['chan']%2>0):
            if self.med_conf['print']: 
                print('signalling packet passed')
            return
        
        if random.random() < self.med_conf['loss']: # error occurs 
            if self.queue:
                item = self.queue.popleft()
            else:
                item = None
            if self.med_conf['print']: 
                print('lost packet', item)

        time.sleep(random.expovariate(self.med_conf['lambda'])) #random latency is  exponentailly distributed 
        
        if self.med_conf['print']: print('medium buffer state', len(self.queue))

if __name__ == "__main__":
    Sub_CONF= {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[4,106], 'sub_id':2, 'dly':2., 'name': 'Mrx', 'maxlen': 10,  'print': True}#use external buffer
    Pub_CONF= {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[104,6], 'pub_id':2, 'dly':2., 'name': 'Mtx', 'maxlen': 10,  'sdu':{'seq':0}, 'print': True}
    Med_CONF= {'ipv4':'127.0.0.1' , 'sub_port': 5570, 'pub_port': "5568", 'pub_id':2, 'sub_id': 2,'dly': 1., 'name':'M', 'lambda':10., 'loss': 0.2, 'maxlen':10, 'print': True}
    inst = Medium(Sub_CONF, Pub_CONF, Med_CONF)
    inst.sub_and_pub()
