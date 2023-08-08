"""
smacs1/aprod.py <- apubsub.py

class Slave=server: input ipv4(str), port(int), sid(int)
sends on N5 interface and receive on N0

2 ip addresses: for N5(as server) and N4(as client)
    (hub_ip, pub_port, sub_port)
    (p_ip, p_port)
created 7/20/2023/nj 
"""
import zmq, sys, time, json, random, copy
from threading import Thread
from prod import Prod 

class Slave:
    """ Server: received, handle received and respond
        sconf={'ipv4':str, 'port':int, 'id':intr}
    """
    def __init__(self, ip, port, sid):
        self.ip = ip#sconf['ipv4']
        self.port = port #sconf['port']
        self.id = sid#sconf['id']
        print('Slave:', ip, port, sid)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://{}:{}".format(ip, port))
        self.loop = True

    def close(self):
        self.socket.close()
        self.context.term()

    def server(self):
        while self.loop:
            rdata= self.socket.recv()
            mstring = rdata.decode()
            print('just received', mstring)
            try:
                message = json.loads(mstring)
                message['pt'].append(time.time_ns())
            except:
                message = {}
            time.sleep(1)
            response =  self.produce(message)   #send to N7 
            response['ct'].append(time.time_ns())

            #print("to be transmitted", response)
            data = json.dumps(response)
            self.socket.send(data.encode())

            #handler to be overidden by child class
    def produce(self, message:dict)-> dict: 
        tx ={'id': self.id, 'date': time.strftime("%x"), 'pt': message['pt'], 'ct': []}
        print(f"Send: {tx!r}")
        return tx

#SlaveP+Pord
class SlaveProd(Slave, Prod):
    """
    SlaveSub: provide parallel operation of a Slave (server) and a Sub (subsriber)
        includes a publisher that sends data via hub
    """
    def __init__(self, slave, prod):
        Slave.__init__(self, slave['ipv4'], slave['port'], slave['port'])
        Prod.__init__(self, prod)
        print('SlaveProd:', self.ip, self.port, self.id)


    def pst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'ct':[], 'new': True, 'crst': False, 'loop': True}#not used here, rather in aprod.py
        p2c= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'pt':[], 'new': True, 'urst': False, 'update': False}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}

    #cdu to N5
    def ctr_cdu0(self, seq): return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'pt':[], 'ct':[]}
    def ctr_cdu13(self, seq, mseq, ct): return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'ct':ct}

    def produce(self, cdu: dict)-> dict:
        print('mode ', self.conf['mode']) #cdu received from controller def a_mode0(self, cdu:dict)->dict:
        if cdu['seq'] > self.pst['ctr']['seq']:  #from N0
            self.pst['ctr']['seq'] = cdu['seq']
            if cdu['conf']:
                conf=copy.deepcopy(cdu['conf'])
                print('received conf', conf)
                self.conf = copy.deepcopy(conf['conf']['p'])
            else:   #if empty
                print('no valid conf received', cdu['conf'])
            #acknowledge any way
            rcdu = self.ctr_cdu0(self.pst['ctr']['seq'])
            if cdu['crst']:
                self.pst['ctr']['seq'] = 0
                print('producer reset and wait...') #print('new state', self.pst) 
                f = open('p.conf', 'w')
                f.write(json.dumps(self.conf))
                f.close()
            return rcdu
        return {}#self.ctr_cdu0(self.pst['ctr']['seq'])

    def run(self):
        print('mode 0 needs aprod.py')
        self.server()

""" ----TEST----- """
if __name__ == '__main__':

    from contrtest import P_CONF as N4CONF
    from contrtest import CONF 
    N5CONF ={'ipv4':CONF['ips'][0], 'port': CONF['ports'][0], 'id':CONF['key'][0]}

    print(sys.argv)

    if '-svr' in sys.argv:
        s=Slave(N5CONF['ipv4'], N5CONF['port'], N5CONF['port'])
        s.server()
    elif '-prod' in sys.argv:
        N4CONF['mode']= 2
        s=Prod(N4CONF)
        s.p_run()
    elif len(sys.argv)>1:
        N4CONF['mode']= int(sys.argv[1])
        print(N4CONF)
        s= SlaveProd(N5CONF, N4CONF)
        s.run()
    else:
        print('usage: python3 aprod.py -svr/prod')
        print('usage: python3 aprod.py mode (0,1,2,3)')
