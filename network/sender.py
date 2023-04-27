
from threading import Thread
import pub, sub
#sender publishes for topic 1 (traffic channel)
PubS_CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[1],   'pub_id':1, 'dly':1., 'name': 'Sender', 'sdu':{'stm':0., 'seq':0}}
#sender subscribes to topic 3 (controll channel)
SubC_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[101], 'sub_id':1, 'dly':1., 'name': 'Sender', 'sdu': {'stm':0., 'seq':0}}
if __name__== "__main__":
    inst = [pub.Pub(PubS_CONF), sub.Sub(SubC_CONF)]
    threads = [Thread(target=inst[0].publisher), Thread(target=inst[1].subscriber)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for item in inst:
        item.close()

