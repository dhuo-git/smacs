
'''
consumerm.py, similiar to producerm.py, consists of a Sub and a Pub:
    Sub(subm.py): receives data from 'subtopics'
    Pub(pubm.py): sends data to 'pubtopics'

'''
from threading import Thread
import subm, pubm

#use received data (on subm) to produce data for the controller (to pubm): measurement
def ReceiveTransmit(rxlstq, txlstq):
    while True:
        if lstq[6]:
            rx = lstq[6].popleft()
            tx = {'id': rx['id'], 'lat': rx['tstmp3']- rx['tstmp1']}
            txlstq[0].append(tx)
        else:
            time.sleep(1)

#consumer subscribe to sender via medium 
SubR_CONF = {'ipv4':"127.0.0.1" , 'sub_port': "5570", 'subtopics':[104,107], 'sub_id':2, 'dly':1., 'name': 'Consumer', 'tstmp': True, 'maxlen': 4,  'print': True}
#consumer publishe to producer signalling (latency and buffer)
PubT_CONF = {'ipv4':"127.0.0.1" , 'pub_port': "5568", 'pubtopics':[6, 7], 'pub_id':2, 'dly':1., 'name': 'Consumer', 'tstmp': True, 'maxlen': 10, 'sdu': {'seq':0}, 'print': True}


#three independent channels: receive traffic(sub), receive control(sub), send states(pub)
if __name__=="__main__":
    import sys
    print(sys.argv)
    if len(sys.argv) == 2:
        SubR_CONF['rounds'] = int(sys.argv[1])
        PubT_CONF['rounds'] = int(sys.argv[1])
    if len(sys.argv) not in [1,2]:
        print('Usage: python3 consumerm.py |int')
        exit()
    inst =[ subm.Sub(SubR_CONF),  pubm.Pub(PubT_CONF)]
    if SubR_CONF['print']:
        threads = [Thread(target=inst[0].subscriber), Thread(target=inst[1].publisher)]
    else:
        threads = [Thread(target=inst[0].subscriber), Thread(target=inst[1].publisher), Thread(target=inst[0].output)]

    for t in threads: t.start()
    for t in threads: t.join()
    for item in inst: item.close()
