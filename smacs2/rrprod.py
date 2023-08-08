"""
smacs2/rrprod.py (history: smacs1/aprod.py,apubsub.py)

class Slave=server: input ipv4(str), port(int), sid(int)
class Slave=server: input ipv4(str), port(int), sid(int) cooperate with class Master=client (rrcontr.py): input ipv4s(list of strs), ports(list of ints), cid(int), 
sends on N5 interface and receive on N0

child class SaveProd(Slave, Prod):
    provides mode 0,1,3 support to SMACS
created 7/20/2023,  last update 8/7/2023
"""
import zmq, sys, time, json, random, copy
from threading import Thread
from prod import Prod 

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
            response =  self.produce(request)   #send to N5
            while not response: 
                print('wait...', response)
                time.sleep(CONF['dly'])
                response =  self.produce(request)   #send to N5

            if response['proto']:
                response['ct'].append(time.time_ns())
            self.socket.send_json(response)
        self.close()

    def half_duplex_server(self):
        while self.loop:
            if self.seq%2 == 0:         #receive slot
                request = self.socket.recv_json()
                if request['proto']:
                    request['t'].append(time.time_ns())
                print('Slave.server received:', request)
                response =  self.produce(request)   #send to N4
            else:
                #if not response: continue
                if response['proto']:
                    response['ct'].append(time.time_ns())
                self.socket.send_json(response)
            self.seq += 1
        self.close()
    #handler to be overidden by child class
    def produce(self, rx:dict)-> dict: 
        print('Slave.server received:', rx)
        tx ={'id': self.id, 'date': time.strftime("%x"), 'seq': rx['seq'], 'proto': rx['proto'], 'ct': rx['t']} #imitates producer time stamp, hence producer sends ct
        print(f"Slave.server send before time stamping: {tx!r}")
        return tx

class SlaveProd(Slave, Prod):
    """
    SlaveSub: provide parallel operation of a Slave (server) and a Sub (subsriber)
        includes a publisher that sends data via hub
    """# 
    def __init__(self, conf):
        Slave.__init__(self, conf['ips'][0], conf['ports'][0], conf['key'][0])
        Prod.__init__(self, conf['conf']['p'])
        print('SlaveProd:', self.ip, self.port, self.id, self.conf)
        self.pst= self.pst_template()
        print('\n p2c:',self.p2c,'\n ctr:', self.ctr)


    def pst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'ct':[],'loop': True, 'proto':1}
        p2c= {'chan': self.conf['pub'][1],'seq':0,'mseq':0,  'pt':[],'urst': False, 'update': False, 'proto': 1}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}

    #cdu to N5
    def ctr_cdu0(self, seq): 
        return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'proto': 0}
    def ctr_cdu13(self, seq, mseq, ct, proto): 
        return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'ct':ct,'pt':[],  'proto':proto}

    def produce(self, cdu: dict)-> dict:
        print(f"SlaveProd.produce mode {self.conf['mode']} rx-CDU:", cdu)
        match self.conf['mode']:
            case 0:
                return self.ctr_mode0(cdu)
            case 1:
                return self.ctr_mode1(cdu)
            case 3:
                return self.ctr_mode3(cdu)
            case _:
                print('unknown mode')
                exit()
    #components
    def ctr_mode0(self, cdu:dict)->dict:
        if cdu['seq'] > self.pst['ctr']['seq']:  #from N0
            self.pst['ctr']['seq'] = cdu['seq']
            if cdu['conf']:
                conf=copy.deepcopy(cdu['conf'])
                print('received conf', conf)
                self.conf = copy.deepcopy(conf['conf']['p'])
            else:   
                if not cdu['crst']: 
                    print('no valid conf received', cdu['conf'])
            #acknowledge any way
            rcdu = self.ctr_cdu0(self.pst['ctr']['seq'])
            if cdu['crst']:
                self.pst['ctr']['seq'] = 0
                print('mode 0 producer reset ...') #print('new state', self.pst) 
                f = open('p.conf', 'w')
                f.write(json.dumps(self.conf))
                f.close()
            else:
                print("mode 0 sent", rcdu)
            return rcdu
        return self.ctr_cdu0(self.pst['ctr']['seq'])

    def ctr_mode1(self, cdu:dict )->dict:         #R-T-R-T                        #N5/0
        if cdu['proto'] == 1:    
            if len(cdu['t']) == 2:                  #N0 rx: Step 1
                self.p2c.append([cdu['mseq'], cdu['t'], cdu['proto']])

            if self.p2c:                     #N4 tx: Step 2, uses self.p2c
                self.pst['p2c']['mseq'], self.pst['p2c']['pt'], self.pst['p2c']['proto'] = self.p2c.popleft() 
                rcdu = self.p2c_cdu13(self.pst['p2c']['seq']+1, self.pst['p2c']['mseq'], self.pst['p2c']['pt'].copy(), self.pst['p2c']['proto'])
                self.transmit(rcdu, 'tx p2c on N4:')

            sub_topic, ccdu = self.receive('rx c2p on N6:') #N6 rx: Step 3
            if sub_topic == self.conf['sub'][1]:
                self.ctr.append([ccdu['mseq'], ccdu['ct'].copy(), ccdu['proto']])
                #N4: protocoal advance
                if ccdu['seq'] > self.pst['p2c']['seq']: 
                    self.pst['p2c']['seq'] = cdu['seq']

            if self.ctr:                           #N5 tx: Step 4, uses self.ctr
                self.pst['ctr']['mseq'], self.pst['ctr']['ct'], self.pst['ctr']['proto'] = self.ctr.popleft()
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'], self.pst['ctr']['proto'])
            else:
                rcdu = {} 
            return rcdu

        elif cdu['proto'] == 0:                           #local traffic
            if cdu['seq'] > self.pst['ctr']['seq']:       #prepare for N5
                self.pst['ctr']['seq'] = cdu['seq']       #ack the receivd from N0
                if cdu['met']: 
                    self.adopt_met(cdu['met'])
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'],0, [], cdu['proto'])
                if cdu['crst']:
                    self.pst['ctr']['seq'] = 0
                    print('Last state:', self.pst['ctr'])
                    print('mode 1 producer is reset and waiting ...\n')#, self.pst['ctr'])

                return rcdu
            else:
                return {}#self.ctr_cdu13(self.pst['ctr']['seq'],0, [], cdu['proto'])
        else:
            print('wrong proto in', cdu)
            exit()

    def ctr_mode3(self, cdu:dict )->dict:                      #N5/0
        if cdu['proto'] == 1:    
            if len(cdu['t']) == 2: #N0 rx
                self.p2c.append([cdu['mseq'], cdu['t'], cdu['proto']])
            #----------   refresh user plane, not for mode 1, 
            if cdu['urst']: 
                self.conf['mode'] = 1       #turn source off
                self.pst['p2c']['seq'] = 0
            else: self.conf['mode'] = 3       #turn source on
            #----------- refreshed
            time.sleep(self.conf['dly'])

            if self.ctr:                               #N5 tx
                self.pst['ctr']['mseq'], self.pst['ctr']['ct'], self.pst['ctr']['proto'] = self.ctr.popleft()
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'], self.pst['ctr']['proto'])
                return rcdu
            else: #skip transmit 
                return  {}#return self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], [], 0)#self.pst['ctr']['proto'])

        elif cdu['proto'] == 0:
            if cdu['seq'] > self.pst['ctr']['seq']:       #prepare for N5
                self.pst['ctr']['seq'] = cdu['seq']       #ack the receivd from N0
                if cdu['met']: 
                    self.adopt_met(cdu['met'])
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'],self.pst['ctr']['mseq'], [], cdu['proto'])
                if cdu['crst']:
                    self.pst['ctr']['seq'] = 0
                    print('Last state:', self.pst['ctr'])
                    print('producer is reset and waiting ...\n')
                    self.pst= self.pst_template()
                    self.ctr.clear()
                    self.p2c.clear()
                return rcdu
            else: #retransmit 
                return self.ctr_cdu13(self.pst['ctr']['seq'], cdu['mseq'], [], cdu['proto'])
        else:
            print('wrong proto in', cdu)
            exit()

        #time.sleep(self.conf['dly'])
    """ """
    #auxsilliary functions
    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation, skipped:', met)
            met.clear()
            return True
        else:
            print('unknown MET',met)
            return False
    """control side of Producer: """
def rprod(Conf:dict)-> None:
    s= SlaveProd(Conf)
    print('mode', Conf['mode'])
    match Conf['mode']:
        case 0|1:
            s.server()
        case 2:
            thr= [Thread(target=s.pmode2), Thread(target=s.source)]
            for t in thr: t.start()
            for t in thr: t.join()
        case 3:
            thr= [Thread(target=s.server), Thread(target=s.pmode3), Thread(target=s.source)]
            for t in thr: t.start()
            for t in thr: t.join()
        case _:
            print('unknown mode in rprod(Conf)')
            exit()

""" ----TEST----- """
from rrcontr import set_mode
if __name__ == '__main__':

    from rrcontr import CONF 
    print(sys.argv)

    if '-svr' in sys.argv:
        s=Slave(CONF['ips'][0], CONF['ports'][0], CONF['key'][0])
        s.server()
    elif len(sys.argv)>1:
        m = int(sys.argv[1])
        set_mode(CONF, m)
        rprod(CONF)
    else:
        print('usage: python3 aprod.py -svr')
        print('usage: python3 aprod.py mode (0,1,2,3)')
