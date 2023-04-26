
from threading import Thread
import sub, pub
#receiver subscribe to sender via medium 
SubS_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[2], 'sub_id':3, 'dly':1., 'name': 'Receiver'}
#controller subscribe to controller
SubC_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[3], 'sub_id':3, 'dly':1., 'name': 'Receiver'}
#receiver publisher states for controler
PubC_CONF = {'ipv4':"127.0.0.1" , 'pub_port': "5568", 'pubtopics':[4], 'pub_id':3, 'dly':1., 'name': 'Receiver'}


#three independent channels: receive traffic(sub), receive control(sub), send states(pub)
if __name__=="__main__":
    conf=sub.CONF
    inst =[ sub.Sub(SubS_CONF),  sub.Sub(SubC_CONF),  pub.Pub(PubC_CONF)]

    threads = [Thread(target=inst[0].subscriber), Thread(target=inst[1].subscriber), Thread(target=inst[2].publisher)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for item in inst: 
        item.close()
