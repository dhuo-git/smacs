
from threading import Thread
import sub, pub
#receiver subscribe to sender via medium 
SubS_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[6], 'sub_id':3, 'dly':1., 'name': 'Consumer','maxlen': 4,  'print': False}
#receiver publisher states for controler
PubC_CONF = {'ipv4':"127.0.0.1" , 'pub_port': "5568", 'pubtopics':[106], 'pub_id':3, 'dly':1., 'name': 'Consumer', 'is_origin': True, 'maxlen': 10, 'sdu': {'stm':0, 'seq':0}, 'print': False}


#three independent channels: receive traffic(sub), receive control(sub), send states(pub)
if __name__=="__main__":
    conf=sub.CONF
    inst =[ sub.Sub(SubS_CONF),  pub.Pub(PubC_CONF)]
    #from collections import deque
    #Q = deque([])
    #threads = [Thread(target=inst[0].subscriber, args=(Q,)), Thread(target=inst[1].publisher), Thread(target=inst[0].output, args=(Q,))]
    if SubS_CONF['print']:
        threads = [Thread(target=inst[0].subscriber), Thread(target=inst[1].publisher)]
    else:
        threads = [Thread(target=inst[0].subscriber), Thread(target=inst[1].publisher), Thread(target=inst[0].output)]


    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for item in inst: 
        item.close()
