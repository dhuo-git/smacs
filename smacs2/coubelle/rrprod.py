"""
smacs2/rrprod.py <- smacs1/aprod.py <- apubsub.py

class Slave=server: input ipv4(str), port(int), sid(int)
sends on N5 interface and receive on N0

clas SaveProd(Slave, Prod) = producer: 
2 ip addresses: for N5(as server) and N4(as client)
    (hub_ip, pub_port, sub_port)
    (p_ip, p_port)
created 7/20/2023/nj last update 7/25/2023/nj
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
            print('received', request)

            response =  self.produce(request)   #send to N7 response['ct'].append(time.time_ns())

            if response['proto']:
                response['ct'].append(time.time_ns())
            self.socket.send_json(response)
        self.close()
    #handler to be overidden by child class
    def produce(self, rx:dict)-> dict: 
        tx ={'id': self.id, 'date': time.strftime("%x"), 'seq': rx['seq'], 'proto': rx['proto'], 'ct': rx['t']} #time stamp loop back for test
        print(f"Slave Send before time stamping: {tx!r}")
        return tx

#SlaveP+Pord
class SlaveProd(Slave, Prod):
    """
    SlaveSub: provide parallel operation of a Slave (server) and a Sub (subsriber)
        includes a publisher that sends data via hub
    """
    #def __init__(self, slave, prod):
    def __init__(self, conf):
        Slave.__init__(self, conf['ips'][0], conf['ports'][0], conf['key'][0])# N5CONF ={'ipv4':CONF['ips'][0], 'port': CONF['ports'][0], 'id':CONF['key'][0]}
        Prod.__init__(self, conf['conf']['p'])
        print('SlaveProd:', self.ip, self.port, self.id, self.conf)


    def pst_template(self):
        ctr= {'chan': self.conf['ctraddr'],'seq':0,'mseq': 0, 'ct':[], 'new': True, 'crst': False, 'loop': True}#not used here, rather in aprod.py
        p2c= {'chan': self.conf['pub'][1], 'seq':0, 'mseq':0,  'pt':[], 'new': True, 'urst': False, 'update': False}
        return {'id': self.id, 'key': self.conf['key'], 'ctr': ctr, 'p2c':p2c}

    #cdu to N5
    def ctr_cdu0(self, seq): 
        return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'proto': 0}
    def ctr_cdu13(self, seq, mseq, ct): 
        return {'id': self.id, 'chan': self.conf['ctraddr'], 'key': self.conf['key'], 'seq':seq, 'mseq':mseq, 'ct':ct,'pt':[],  'proto':1}

    def produce(self, cdu: dict)-> dict:
        print(f"SlaveProd at mode {self.conf['mode']}  with rx-CDU:", cdu)
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
                print('producer reset and wait...') #print('new state', self.pst) 
                f = open('p.conf', 'w')
                f.write(json.dumps(self.conf))
                f.close()
            else:
                print("mode 0 sent", rcdu)
            return rcdu
        return self.ctr_cdu0(self.pst['ctr']['seq'])

    def a_mode1(self, cdu:dict )->dict:                                 #N5/0
        if cdu['proto'] == 1:                                           #measurement #cdu['pt'].append(time.time_ns())
            if len(cdu['t']) == 2:
                self.pst['p2c']['pt'] =cdu['t'].copy()           
                self.pst['p2c']['mseq'] = cdu['mseq']               
                self.pst['p2c']['new'] = True
            else:
                self.pst['p2c']['pt'].clear()

            if self.pst['ctr']['new']: #len(self.pst['ctr']['ct']) == 4:
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'])
                self.pst['ctr']['ct'].clear()
                rcdu['proto'] = 1
                #rcdu['ct'].append(time.time_ns())
            else:
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], [])
                rcdu['proto'] = 0

        elif cdu['proto'] == 0:                                         #local traffic
            if cdu['seq'] > self.pst['ctr']['seq']:                         #prepare for N5
                self.pst['ctr']['seq'] = cdu['seq']                     #ack the receivd from N0
                if cdu['met']: 
                    self.adopt_met(cdu['met'])

                if cdu['crst']:
                    self.pst['ctr']['seq'] = 0
                    print('consumer reset and wait ...', self.pst['ctr'])
                    rcdu = None
                else:
                    rcdu = self.ctr_cdu13(self.pst['ctr']['seq'],self.pst['ctr']['mseq'],[])
                    rcdu['proto'] = 0
            else:
                rcdu = self.ctr_cdu13(self.pst['ctr']['seq'],self.pst['ctr']['mseq'],[])
                rcdu['proto'] = 0
        return rcdu

    def a_mode2(self, cdu:dict)->dict:
        print("Error: mode 2 does not need aprod.py")
        exit()

    def a_mode3(self, cdu:dict)->dict:                                  #N5/0
        if cdu['pt'] and  len(cdu['pt']) == 2:                          #cdu['pt'].append(time.time_ns())
            self.pst['p2c']['pt'] = cdu['pt'].copy() 
            self.pst['p2c']['mseq'] = cdu['mseq']                       #update p2c buffer with the received from N0
            self.pst['p2c']['new'] = True
        else:
            self.pst['p2c']['pt'].clear()
        #moves to Slave local
        if cdu['seq'] > self.pst['ctr']['seq']:                         #prepare for N5
            if cdu['met']: 
                self.adopt_met(cdu['met'])
                self.pst['ctr']['seq'] = cdu['seq']                     #ack the receivd from N0
                self.pst['ctr']['new'] = True                           
            """ """
            #update user plane state
            if 'urst' in cdu:
                self.pst['p2c']['urst'] = cdu['urst']                   #can be F or T
                self.pst['p2c']['update'] = True                        #infor N4 of the new urst value

        if self.pst['ctr']['new']:                                      #response on N5, no SDU
            self.pst['ctr']['new'] = False 
            rcdu = self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'])
            if len(rcdu['ct']) == 4:                                    #rcdu['ct'].append(time.time_ns()) self.transmit(rcdu, 'tx ctr N5:')
                return rcdu

        #if get here, send dummy packet
        self.pst['ctr']['ct'].clear()
        return self.ctr_cdu13(self.pst['ctr']['seq'], self.pst['ctr']['mseq'], self.pst['ctr']['ct'])


    #auxsilliary functions
    def adopt_met(self, met):                       #for the time being, clear memory
        if isinstance(met,dict):
            print('adaptation, skipped:', met)
            met.clear()
            return True
        else:
            print('unknown MET',met)
            return False
    """ """
def aprod(Conf:dict)-> None:
    s= SlaveProd(Conf)
    print('mode', Conf['mode'])
    match Conf['mode']:
        case 0:
            s.server()
        case 1:
            thr= [Thread(target=s.p_run), Thread(target=s.server)]
            for t in thr: t.start()
            for t in thr: t.join()
        case 2:
            thr= [Thread(target=s.p_run), Thread(target=s.source)]
            for t in thr: t.start()
            for t in thr: t.join()
        case 3:
            thr= [Thread(target=s.p_run), Thread(target=s.source), Thread(target=s.server) ]
            for t in thr: t.start()
            for t in thr: t.join()
        case _:
            print('unknown mode in aprod(Conf)')
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
        aprod(CONF)
    else:
        print('usage: python3 aprod.py -svr')
        print('usage: python3 aprod.py mode (0,1,2,3)')
    """
    elif '-prod' in sys.argv:
        CONF['mode']['p'] = 2
        s=Prod(CONF['p'])
        thr= [Thread(target=s.p_run),  Thread(target=s.source)]
        for t in thr: t.start()
        for t in thr: t.join()
        #s.p_run()
    """
