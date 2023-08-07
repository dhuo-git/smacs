"""
acons.py <- multicast.py (dict input and dict output: catch 4 time stamps)  <-- multcst.py (primitive, string input and strin output)
tests multicast transmit and receive using tcp sockets via asyncio
class Slave=server: input ipv4(str), port(int), sid(int)
class Master=client: input ipv4s(list of strs), ports(list of ints), cid(int), loop (continues: T, single: F=default)

with TEST=True: catch 4 time points and  slow down server for observation purpose
created 7/18/2023/nj , last update 7/24/2023/nj
"""
import zmq, sys, time, json, random, copy
from threading import Thread
from cons import Cons
#import pdb #breakpoint()

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
        #self.socket = self.context.socket(zmq.PAIR)
        self.socket.bind("tcp://{}:{}".format(ip, port))
        self.loop = True

    def close(self):
        self.socket.close()
        self.context.term()

    def server(self):
        while self.loop:
            request = self.socket.recv_json()
            if request['proto']:        #received multicast
                request['t'].append(time.time_ns())
            print('received request:', request)

            response =  self.consume(request)

            if response['proto']:
                response['pt'].append(time.time_ns())
            self.socket.send_json(response) 
        self.close()

    #handler to be overriden by child class
    def consume(self, rx:dict)-> dict: 
        tx ={'id': self.id, 'date': time.strftime("%x"), 'seq':rx['seq'], 'proto': rx['proto'], 'pt':rx['t']}  #time stamp loop back for test: pt is not from ctr actually
        print(f"Slave Send before time stamping: {tx!r}")
        return tx

class SlaveCons(Slave, Cons):
    """
    SlaveSub: provide parallel operation of a Slave (server) and a Sub (subsriber)
        includes a publisher that sends data via hub
    """
    #def __init__(self, slave, cons):
    def __init__(self, conf):
        Slave.__init__(self, conf['ips'][1], conf['ports'][1], conf['key'][1])
        Cons.__init__(self, conf['conf']['c'])
        print('SlaveCons:', self.ip, self.port, self.id, self.conf)

    def cst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'pt':[], 'new': True, 'crst': False, 'loop': True} 
        c2p= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'ct':[], 'new': True, 'urst': False, 'update': False}#not used here, but at prod.py
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}
    #cdu send to N7 (mode 1,3)
    def ctr_cdu0(self, seq): return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'proto':0}
    def ctr_cdu13(self, seq, mseq, pt): 
        return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'pt':pt, 'ct':[], 'proto':1}

    def consume(self, cdu:dict)-> dict:
        print(f"SlaveCons at mode {self.conf['mode']}  with rx-CDU:", cdu)
        match self.conf['mode']:
            case 0:
                return self.a_mode0(cdu)
            case 1:
                return self.a_mode1(cdu)
            case 2:
                return self.a_mode2(cdu)
            case 3:
                return self.a_mode3(cdu)
            case _:
                print('unknown mode')
                exit()
    #components
    def a_mode0(self, cdu:dict)->dict:
        if cdu['seq'] > self.cst['ctr']['seq']:  #from N0
            self.cst['ctr']['seq'] = cdu['seq']
            if cdu['conf']:
                conf = copy.deepcopy(cdu['conf'])
                print('received conf:', conf)
                self.conf = copy.deepcopy(conf['conf']['c'])
            else:
                if not cdu['crst']:
                    print('no valid conf received',cdu['seq'],  cdu['conf'])
            #acknowledge any way
            rcdu = self.ctr_cdu0(self.cst['ctr']['seq'])
            if cdu['crst']:     #controll state reset
                self.cst['ctr']['seq'] = 0
                print('consumer reset and wait ...') #print('new state', self.cst)
                f=open('c.conf', 'w')
                f.write(json.dumps(self.conf))
                f.close()
            else:
                print('mode0 sent', rcdu)
            return rcdu
        return self.ctr_cdu0(self.cst['ctr']['seq'])

    def a_mode1(self, cdu:dict)->dict:                                  #N7/0
        if cdu['proto'] == 1:                                           #measurement
            #cdu['ct'].append(time.time_ns())
            if len(cdu['t']) == 2: #self.cst['c2p']['ct'] = cdu['ct'].copy()                #copy.deepcopy(cdu['ct'])
                self.cst['c2p']['ct'] = cdu['t'].copy()                #copy.deepcopy(cdu['ct'])
                self.cst['c2p']['mseq'] = cdu['mseq']
                self.cst['c2p']['new'] = True                               #from N0 to N6
            else:
                self.cst['c2p']['ct'].clear()

            if self.cst['ctr']['new']: #len(self.cst['ctr']['pt']) == 4:
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], self.cst['ctr']['pt'])
                self.cst['ctr']['pt'].clear()
                rcdu['proto'] = 1
                #rcdu['pt'].append(time.time_ns())
            else:
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], [])
                rcdu['proto'] =0

        elif cdu['proto'] == 0:
            if cdu['seq'] > self.cst['ctr']['seq']:                         #prepare for N7
                self.cst['ctr']['seq'] = cdu['seq']                     #ack the received from N0
                if cdu['met']:
                    self.adopt_met(cdu['met'])
                if cdu['crst']:
                    self.cst['ctr']['seq'] = 0
                    print('consumer reset and wait ...', self.cst['ctr'])
                    rcdu = None
                else:
                    rcdu = self.ctr_cdu13(self.cst['ctr']['seq'],self.cst['ctr']['mseq'],[])#self.cst['ctr']['pt'])
                    rcdu['proto'] = 0
            else:
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'],self.cst['ctr']['mseq'],[])#self.cst['ctr']['pt'])
                rcdu['proto'] = 0
        else:
            print('wrong proto in', cdu)
            exit()

        return rcdu


    def a_mode2(self, cdu:dict)->dict:
        print("Error: mode 2 does not need acons.py")
        exit()
        #return {}

    def a_mode3(self, cdu:dict)->dict:                                  #N7/0
        if cdu['ct'] and  len(cdu['ct']) == 2:                          # cdu['ct'].append(time.time_ns())
            self.cst['c2p']['ct'] = cdu['ct'].copy()
            self.cst['c2p']['mseq'] = cdu['mseq']                       #update N6 buffer with the received from N0
            self.cst['c2p']['new'] = True                               

        if cdu['seq'] > self.cst['ctr']['seq']:                         #prepare for N7
            if cdu['met']:
                self.adopt_met(cdu['met'])
                self.cst['ctr']['seq'] = cdu['seq']                     #ack the received from N0
                self.cst['ctr']['new'] = True 
            """ """
            if 'urst' in cdu:
                self.cst['c2p']['urst'] = cdu['urst']
                self.cst['c2p']['update'] = True                        #inform N6 of the new urst value

        if self.cst['ctr']['new']:                                      #response on N7, mseq is updated by slot 1 
            self.cst['ctr']['new'] = False
            rcdu = self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], self.cst['ctr']['pt'])
            if len(rcdu['pt']) == 4:                                     #rcdu['pt'].append(time.time_ns()) #self.transmit(rcdu, 'tx ctr N7:') 
                return rcdu
        #if get here, send dummy packet
        self.cst['ctr']['pt'].clear()
        return self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], self.cst['ctr']['pt'])


    #auxsilliary functions
    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation, skipped:', met)
            met.clear()
            return True
        else:
            print('unknown MET',met)
            
def acons(Conf:dict)-> None:
    s= SlaveCons(Conf)
    print('mode 0 needs aprod.py')
    match Conf['mode']:
        case 0:
            s.server()
        case 1:
            thr= [Thread(target=s.c_run), Thread(target=s.server)]
            for t in thr: t.start()
            for t in thr: t.join()
        case 2:
            thr= [Thread(target=s.c_run), Thread(target=s.sink)]
            for t in thr: t.start()
            for t in thr: t.join()
        case 3:
            thr= [Thread(target=s.c_run), Thread(target=s.sink), Thread(target=s.server) ]
            for t in thr: t.start()
            for t in thr: t.join()
        case _:
            print('unknown mode in aprod(Conf)')
            exit()
""" ----TEST-----"""
from rrcontr import set_mode
if __name__ == '__main__':

    from rrcontr import CONF
    print(sys.argv)

    if '-svr' in sys.argv:
        s=Slave(CONF['ips'][1], CONF['ports'][1], CONF['key'][1])
        s.server()
    elif len(sys.argv)>1: 
        m = int(sys.argv[1])
        set_mode(CONF, m)
        acons(CONF)
    else:
        print('usage: python3 acons.py -svr')
        print('usage: python3 acons.py mode (0,1,2,3)')

    """
    elif '-cons' in sys.argv:
        N6CONF['mode'] = 2
        s=Cons(N6CONF)
        s.c_run()
    """
