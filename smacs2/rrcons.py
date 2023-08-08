"""
smacs2/rrcons.py (history: smacs1/acons.py,  multicast.py )
class Slave=server: input ipv4(str), port(int), sid(int) cooperate with class Master=client (rrcontr.py): input ipv4s(list of strs), ports(list of ints), cid(int), 

child class: SlaveCons (Slave, Cons)
    provides mode 0,1,3 support to SMACS
sends on N7 interface and receive on N0
created 7/18/2023 , last update 8/7/2023
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

            if request['proto']:    
                request['t'].append(time.time_ns())
            response =  self.consume(request)

            while not response:
                print('wait...', response)
                time.sleep(CONF['dly'])
                response =  self.consume(request)

            if response['proto']: 
                response['pt'].append(time.time_ns())

            self.socket.send_json(response) 
        self.close()

    def half_duplex_server(self):
        while self.loop:
            if self.seq%2 == 0:             #receive slot
                request = self.socket.recv_json()
                if request['proto']:        #received multicast
                    request['t'].append(time.time_ns())
                print('Slave.server received:', request)
                response =  self.consume(request)   #send to N6
            else:                           #transmit slot
                #if not response: continue
                if response['proto']: 
                    response['pt'].append(time.time_ns())
                self.socket.send_json(response) 
            self.seq += 1
        self.close()
    #handler to be overriden by child class
    def consume(self, rx:dict)-> dict: 
        print('Slave.server received:', rx)
        tx ={'id': self.id, 'date': time.strftime("%x"), 'seq':rx['seq'], 'proto': rx['proto'], 'pt':rx['t']}  #imitates consumer time stamp: hence consumer sends pt
        print(f"Slave Send before time stamping: {tx!r}")
        return tx

class SlaveCons(Slave, Cons):
    """
    SlaveSub: provide parallel operation of a Slave (server) and a Sub (subsriber)
        includes a publisher that sends data via hub
    """
    def __init__(self, conf):
        Slave.__init__(self, conf['ips'][1], conf['ports'][1], conf['key'][1])
        Cons.__init__(self, conf['conf']['c'])
        print('SlaveCons:', self.ip, self.port, self.id, self.conf)
        self.cst = self.cst_template()
        print('\n c2p:', self.c2p, '\n ctr:', self.ctr)

    def cst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'pt':[], 'loop': True, 'proto': 1} 
        c2p= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'ct':[], 'urst': False, 'update': False, 'proto': 1}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'c2p':c2p}
    #cdu send to N7 (mode 1,3)
    def ctr_cdu0(self, seq): return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'proto':0}
    def ctr_cdu13(self, seq, mseq, pt, proto): 
        return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'mseq':mseq, 'seq':seq, 'pt':pt, 'ct':[], 'proto':proto}

    def consume(self, cdu:dict)-> dict:
        print(f"SlaveCons.consume mode {self.conf['mode']}  rx-CDU:", cdu)
        match self.conf['mode']:
            case 0:
                return self.ctr_mode0(cdu)
            case 1:
                return self.ctr_mode1(cdu)
            case 3:
                return self.ctr_mode3(cdu)
            case _:
                print('unpermissive mode', self.conf['mode'])
                exit()
    #components
    def ctr_mode0(self, cdu:dict)->dict:
        if cdu['seq'] > self.cst['ctr']['seq']:  #from N0
            self.cst['ctr']['seq'] = cdu['seq']
            if cdu['conf']:
                conf = copy.deepcopy(cdu['conf'])
                print('received conf:', conf)
                self.conf = copy.deepcopy(conf['conf']['c'])
            else:
                if not cdu['crst']:
                    print('no valid conf received',cdu['seq'],  cdu['conf'])
            rcdu = self.ctr_cdu0(self.cst['ctr']['seq'])
            #acknowledge any way
            if cdu['crst']:     #controll state reset
                self.cst['ctr']['seq'] = 0
                print('mode 0 consumer reset  ...') #print('new state', self.cst)
                f=open('c.conf', 'w')
                f.write(json.dumps(self.conf))
                f.close()
            else:
                print('mode0 sent', rcdu)
            return rcdu
        return self.ctr_cdu0(self.cst['ctr']['seq'])

    def ctr_mode1(self, cdu:dict)->dict:      #R-R-T-T
        if cdu['proto'] == 1: 
            if len(cdu['t']) == 2:                          #N0 rx: Step 1
                self.c2p.append([cdu['mseq'], cdu['t'].copy(), cdu['proto']])

            sub_topic, pcdu= self.receive('Cons,rx p2c on N4:', True) #N4 rx: Step 2
            if sub_topic == self.conf['sub'][1]:
                self.ctr.append([pcdu['mseq'], pcdu['pt'].copy(), pcdu['proto']])
                #N6: protocol advance
                if pcdu['seq'] > self.cst['c2p']['seq']: 
                    self.cst['c2p']['seq'] = pcdu['seq'] 

            if self.c2p:                                 #N6 tx: Step 3, using  self.c2p
                self.cst['c2p']['mseq'], self.cst['c2p']['ct'], self.cst['c2p']['proto'] = self.c2p.popleft()

                rcdu = self.c2p_cdu13(self.cst['c2p']['seq'], self.cst['c2p']['mseq'], self.cst['c2p']['ct'].copy(), self.cst['c2p']['proto'])
                self.transmit(rcdu, 'Cons, tx c2p on N6:')

            time.sleep(self.conf['dly'])

            if self.ctr:                               #N7 tx: Step 4, using self.ctr
                self.cst['ctr']['mseq'], self.cst['ctr']['pt'], self.cst['ctr']['proto'] = self.ctr.popleft() 
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], self.cst['ctr']['pt'], self.cst['ctr']['proto'])
                return rcdu
            else:
                return {}

        elif cdu['proto'] == 0:
            if cdu['seq'] > self.cst['ctr']['seq']:                         #prepare for N7
                self.cst['ctr']['seq'] = cdu['seq']                     #ack the received from N0
                if cdu['met']:
                    self.adopt_met(cdu['met'])
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'],self.cst['ctr']['mseq'],[], cdu['proto'])
                if cdu['crst']:
                    self.cst['ctr']['seq'] = 0
                    print('Last sate:', self.cst['ctr'])
                    print('mode 1 consumer is reset and waiting  ...\n')
                return rcdu
            else:
                return {}
        else:
            print('wrong proto in', cdu)
            sys.exit()

    """ """
    def ctr_mode3(self, cdu:dict)->dict:                                  #N7/0
        if cdu['proto'] == 1: 
            if len(cdu['t']) == 2:  #N0 rx #while not self.cst['c2p']['ready']: time.sleep(self.conf['dly'])
                self.c2p.append([cdu['mseq'], cdu['t'], cdu['proto']])
            #----------- refresh user-plane, not for mode 1
            if cdu['urst']:     #mode switch
                self.conf['mode'] = 1               #turn sink stream off
                self.cst['c2p']['seq'] = 0
            else: self.conf['mode'] = 3               #turn sink on
            #----------- refreshed
            if self.ctr:                                           #N7 tx
                self.cst['ctr']['mseq'], self.cst['ctr']['pt'], self.cst['ctr']['proto'] = self.ctr.popleft() 
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], self.cst['ctr']['pt'], self.cst['ctr']['proto'])
                return rcdu
            else:       #skip transmit
                return {} # return self.ctr_cdu13(self.cst['ctr']['seq'], self.cst['ctr']['mseq'], [], 0) #self.cst['ctr']['proto'])
            #return rcdu

        elif cdu['proto'] == 0:
            if cdu['seq'] > self.cst['ctr']['seq']:    #prepare for N7
                self.cst['ctr']['seq'] = cdu['seq']
                if cdu['met']:
                    self.adopt_met(cdu['met'])
                rcdu = self.ctr_cdu13(self.cst['ctr']['seq'],self.cst['ctr']['mseq'],[], cdu['proto'])
                if cdu['crst']:
                    self.cst['ctr']['seq'] = 0
                    print('Last state:', self.cst['ctr'])
                    print('consumer reset and wait ...\n')
                    self.cst = self.cst_template()
                    self.ctr.clear()
                    self.c2p.clear()
                return rcdu
            else: #retransmit
                return self.ctr_cdu13(self.cst['ctr']['seq'],cdu['mseq'], [], cdu['proto'])
        else:
            print('wrong proto in', cdu)
            exit()
    """ """
    #auxsilliary functions
    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation, skipped:', met)
            met.clear()
            return True
        else:
            print('unknown MET',met)
            
def rcons(Conf:dict)-> None:
    s= SlaveCons(Conf)
    print('mode', Conf['mode'])
    match Conf['mode']:
        case 0|1:
            s.server()
        case 2:
            thr= [Thread(target=s.cmode2), Thread(target=s.sink)]
            for t in thr: t.start()
            for t in thr: t.join()
        case 3:
            thr= [Thread(target=s.server), Thread(target=s.cmode3), Thread(target=s.sink)]
            for t in thr: t.start()
            for t in thr: t.join()
        case _:
            print('unknown mode in rcons(Conf)')
            exit()
""" ----TEST-----"""
from rrcontr import set_mode
if __name__ == '__main__':

    from rrcontr import CONF
    print(sys.argv)

    if '-svr' in sys.argv:
        s=Slave(CONF['ips'][1], CONF['ports'][1], CONF['key'][1])
        s.server()
    if '-hdsvr' in sys.argv:
        s=Slave(CONF['ips'][1], CONF['ports'][1], CONF['key'][1])
        s.half_duplex_server()
    elif len(sys.argv)>1: 
        m = int(sys.argv[1])
        set_mode(CONF, m)
        rcons(CONF)
    else:
        print('usage: python3 acons.py -svr')
        print('usage: python3 acons.py mode (0,1,2,3)')

