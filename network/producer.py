
from threading import Thread
import pub, sub
#sender publishes for topic 1 (traffic channel)
PubS_CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[4],   'pub_id':1, 'dly':1., 'name': 'Producer', 'maxlen': 10, 'sdu':{'seq':0}, 'print': False}
#sender subscribes to topic 3 (controll channel)
SubC_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[104], 'sub_id':1, 'dly':1., 'name': 'Producer', 'maxlen':4, 'print': False}
if __name__== "__main__":
    inst = [pub.Pub(PubS_CONF), sub.Sub(SubC_CONF)]
    if SubC_CONF['print']:
        threads = [Thread(target=inst[0].publisher), Thread(target=inst[1].subscriber)]
    else:
        threads = [Thread(target=inst[0].publisher), Thread(target=inst[1].subscriber), Thread(target=inst[1].output)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for item in inst:
        item.close()

