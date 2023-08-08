"""
aconsrep.py (test module for acons.py) <-acons.py <- multicast.py (dict input and dict output: catch 4 time stamps)  <-- multcst.py (primitive, string input and strin output)
tests multicast transmit and receive using tcp sockets via asyncio
class Slave=server: input ipv4(str), port(int), sid(int)
class Master=client: input ipv4s(list of strs), ports(list of ints), cid(int), loop (continues: T, single: F=default)

with TEST=True: catch 4 time points and  slow down server for observation purpose
created 7/18/2023/nj 
"""
import zmq, sys, time, json, random, copy
from threading import Thread
from cons import Cons

class Slave:
    """ Server: received, handle received and respond
        sconf={'ipv4':str, 'port':int, 'id':intr}
    """
    def __init__(self, ip, port, sid):
        self.ip = ip
        self.port = port
        self.id = sid
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
            rdata = self.socket.recv()
            mstring = rdata.decode()
            print('just received', mstring)
            try:
                message = json.loads(mstring)
                message['ct'].append(time.time_ns())
            except:
                message = {}
            time.sleep(1)
            response =  self.consume(message)   #send to N7 
            response['pt'].append(time.time_ns())

            #print("to be transmitted", response)
            data = json.dumps(response)
            self.socket.send(data.encode())

           #time.sleep(random.randint(0,3)) # slow down to observe
    #handler to be overriden by child class
    def consume(self, message:dict)-> dict: 
        tx ={'id': self.id, 'date': time.strftime("%x"), 'pt':[], 'ct':message['ct']}
        print(f"Send: {tx!r}")
        return tx

class SlaveCons(Slave, Cons):
    """
    SlaveSub: provide parallel operation of a Slave (server) and a Sub (subsriber)
        includes a publisher that sends data via hub
    """
    def __init__(self, slave, cons):
        Slave.__init__(self, slave['ipv4'], slave['port'], slave['port'])
        Cons.__init__(self, cons)
        print('SlaveCons:', self.ip, self.port, self.id, cons)

    def cst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'pt':[], 'new': True, 'crst': False, 'loop': True} 
        c2p= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'ct':[], 'new': True, 'urst': False, 'update': False}#not used here, but at prod.py
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}
    #cdu send to N7 (mode 1,3)
    def ctr_cdu0(self, seq): return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'pt':[], 'ct':[]}

    def consume(self, cdu:dict)->dict: #i.e. mode0()
        print('mode ', self.conf['mode']) #cdu received from controller def a_mode0(self, cdu:dict)->dict:
        if cdu['seq'] > self.cst['ctr']['seq']:  #from N0
            self.cst['ctr']['seq'] = cdu['seq']
            if cdu['conf']:
                conf = copy.deepcopy(cdu['conf'])
                print('received conf:', conf)
                self.conf = copy.deepcopy(conf['conf']['c'])
            else:
                print('no valid conf received',cdu['seq'],  cdu['conf'])
            #acknowledge any way
            rcdu = self.ctr_cdu0(self.cst['ctr']['seq'])
            if cdu['crst']:     #controll state reset
                self.cst['ctr']['seq'] = 0
                print('consumer reset and wait ...') #print('new state', self.cst)
                f=open('c.conf', 'w')
                f.write(json.dumps(self.conf))
                f.close()
            return rcdu
        return {}#self.ctr_cdu0(self.cst['ctr']['seq'])
    def run(self):
        print('mode 0 needs acons.py')
        self.server()
""" ----TEST-----"""
if __name__ == '__main__':

    from contrtest import C_CONF as N6CONF
    from contrtest import CONF
    N7CONF = {'ipv4':CONF['ips'][1], 'port':CONF['ports'][1], 'id': CONF['key'][1]}
    print(N7CONF)
    print(sys.argv)

    if '-svr' in sys.argv:
        s=Slave(N7CONF['ipv4'], N7CONF['port'], N7CONF['port'])
        s.server()
    elif '-cons' in sys.argv:
        N6CONF['mode'] = 2
        s=Cons(N6CONF)
        s.c_run()
    elif len(sys.argv)>1:
        N6CONF['mode'] = int(sys.argv[1]) #works for mode =0 only
        s= SlaveCons(N7CONF, N6CONF)
        s.run()
    else:
        print('usage: python3 acons.py -svr/cons')
        print('usage: python3 acons.py mode (0,1,2,3)')

