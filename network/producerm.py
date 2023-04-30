'''
producer.py, similar to consumer.py,  consists of a Pub and a Sub:
    Pub(pub.py): sends data from pub_deque for given 'pubtopics'
    Sub(sub.py): receives data from sub_deque for given 'subtopics'
'''

from threading import Thread
import pubm, subm
#sender publishes for topic 1 (traffic channel)
PubS_CONF = {'ipv4':'127.0.0.1' , 'pub_port': "5568", 'pubtopics':[4],   'pub_id':1, 'dly':1., 'name': 'Producer', 'tstmp': True, 'maxlen': 10, 'sdu':{'seq':0}, 'print': True}
#sender subscribes to topic 3 (controll channel)
SubC_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[104], 'sub_id':1, 'dly':1., 'name': 'Producer', 'tstmp': True, 'maxlen':4, 'print': True}
if __name__== "__main__":
    inst = [pubm.Pub(PubS_CONF), subm.Sub(SubC_CONF)]
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

