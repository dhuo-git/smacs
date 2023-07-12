#!/usr/bin/env python3
'''
Modules:
    tcpsever(), tcpclient(), udpserver(), udpclient()
Classes (TCP): 
    Server, Client,  Forwarder
Usage for Class Client and Server (in 2 separate shells:
    python serverclient -svr OR  python serverclient.py -svr ip-port
    python serverclient.py -clt OR  python serverclient.py -cli ip-port

Usage for Class Forwader (in 3 separate shells):
    python serverclient -svr 6001
    python serverclient -fwd
    python serverclient -clt

last update 6/20/2020 by dhuo

'''
import socket, sys, json, time, copy
import threading
from queue import Queue
#from multiprocessing import Queue

HOST = '172.17.0.1' #docker bridge
HOST = '192.168.33.11' #docker bridge
HOST = '10.0.2.15' #docker bridge
HOST = '10.0.1.2' #docker overlay network test-net
HOST = '192.168.0.52' #vagrant/work1/host1
HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
HOST = '192.168.1.204' #/Lenovo-T
CLOG = False            #cleint output: True:interactive, False: file.log
PORT = 65431        # Port to listen on (non-privileged ports are > 1023)
BUFFER_SIZE = 1024

def tcpeserver(host,port):
    print('tcpserver', host, port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            while True:
                data = conn.recv(1024)              #receive
                if not data:
                    break
                conn.sendall(data)                  #send
def tcpeclient(host,port):
    print('tcpclient', host, port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(b'Hello, world')                  #send
        data = s.recv(1024)                         #receive
        print('Received', repr(data))


def udpserver(address, port): 
    msgFromServer       = f"Server Message at {time.time_ns()} "
    bytesToSend         = str.encode(msgFromServer) # Create a datagram socket

    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM) # Bind to address and ip
    UDPServerSocket.bind((address, port))
    print("UDP server up and listening at {}:{}".format(address,port)) # Listen for incoming datagrams

    while(True):
        message, address= UDPServerSocket.recvfrom(BUFFER_SIZE)  #receive first
        clientMsg = "Client Message [{}] rcved at {}".format(message, time.time_ns())
        print(clientMsg)
        clientIP  = "Client IP Address:{}".format(address)
        print(clientIP) # Sending a reply to client
        UDPServerSocket.sendto(bytesToSend, address)            #send second

def udpclient(address, port):
    msgFromClient       = f"Client Message at {time.time_ns()} "
    bytesToSend         = str.encode(msgFromClient)

    print(msgFromClient, f"sent to {address}:{port}")

    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM) # Send to server using created UDP socket
    UDPClientSocket.sendto(bytesToSend, (address, port))        #send first

    message, address = UDPClientSocket.recvfrom(BUFFER_SIZE)     #receive second
    msg = f"Server Message [{message}] rcved at {time.time_ns()}"
    #msg = f"Message from Server {message} as {time.time_ns()}".format(message)
    print(msg)

#-----------------------------Server Class ------------------------------------------------------
class Server:
    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((host, port))
        self.threads = []
        self.sport = port
        print('server initialized by ', host, port)
    def run(self):
        while True:
            self.sock.listen(5)
            try:
                (conn, (ip, port)) = self.sock.accept()
            except ConnectionResetError or KeyboardInterrupt:
                print('Server connection lost')
                exit()
            newthread = ServerThread(ip, port, conn, self.sport)#, sharedQ)
            newthread.start()
            self.threads.append(newthread)

        for t in self.threads:
            t.join()
#-------------------------------Server Thread for an Incoming Client -------
class ServerThread(threading.Thread):
    '''
    Server side client serving instance
    can run as an temporary instance or as a stand-by daemon
    '''
    def __init__(self,ip, port, sock, sport):
        threading.Thread.__init__(self)
        self.port = port
        self.sport = sport # server port
        self.sock = sock
        self.seqn = 0
        #self.Q = sharedQ
        print('server thread: ', ip, port)

    def run(self):
        while True:                                         #permanent link
            data = self.sock.recv(BUFFER_SIZE)
            if data:
                rx = json.loads(data.decode('utf-8'))
                ''' '''
                if True:
                    tx = self.svr_message(rx)
                    try:                        #response to client 
                        self.sock.sendall(json.dumps(tx).encode('utf-8'))
                    except:
                        print('server cannot transmit')
                        break
            time.sleep(3)

    def svr_message(self, rx):
        print('server rx message', rx)
        rx['clt'] = self.port
        rx['seq'] = self.seqn
        rx['time'] = time.ctime()
        self.seqn += 1
        return rx
#----------------------------Client Class  -----------
import random
class Client(threading.Thread):
    '''
    Client side client instance
    client_run() runs as daemon
    run() runs one shot
    message_hander() works for both runs and generate transmit message from received message
    '''
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host, port))
        print('set up tcp client', host, port)
        self.host = host
        self.port = port        #serves as id in message
        self.id = random.randrange(100)
        #print('class tcp client', self.host, self.port)
    #---- daemon loop of transmit-receive -----
    def run(self):
        tx = self.message_handler(None)
        while True:                                                     #perpetual or one time
            ''' '''
            if True:
                self.s.sendall(json.dumps(tx).encode('utf-8'))
                data = self.s.recv(BUFFER_SIZE)                         #receive #print('Received', repr(data))
                print('client rx from server', data)

                if data:
                    rx = json.loads(data.decode('utf-8'))         #tx = self.message_handler(rx)
                    tx = self.message_handler(rx)
            time.sleep(3)
    #---- one singe handshake: transmit-receive ----
    def simple_run(self, tx = None):
        self.s.sendall(json.dumps(self.clt_message_handler(tx)).encode('utf-8'))
        data = self.s.recv(BUFFER_SIZE)                             #receive #print('Received', repr(data))
        if data:
            rx = json.loads(data.decode('utf-8'))
            print('receiced', rx)
        else:
            print('not data received')
    #----- generate tx message from rx message, at endpoint---
    def message_handler(self, rx = None):
        print('client rx message', rx)
        if rx:
            print('received', rx)
            tx = {'clt': self.id, 'svr': self.port, 'time': time.ctime()}
            tx['seq'] = rx['seq']+ 1
            #rx['time'] = time.time()
            #tx = rx
        else: # initial message
            tx = {'clt':self.id, 'svr': self.port, 'time': time.ctime(), 'seq':0}
        return tx
#------------------  Full Loop Forwarder Class  --------------------
class Forwarder:
    '''
    Forwarder (F) serves as a server to left side client(LC), and serves as a client to right side server(RS): 3 addresses
    Fixed address (ip, port) is assigned to F and RS, while dynamic address (ip, port) is acquired fom LC during the operation
    it interact with a client on the left and a server on the right, which has address fwd_addr=(host,port)
    LC generates data, sends data to F and receives response from F
    F receives data from LC, sends to RS,  receives response from RS and sends the received response to LC
    F mains two queues fwQ and rvQ to holdes, and forwards received data from LC and RS, respectively 

    Data are processed before saving to fwQ and rvQ by svr_fwmessage() and clt_rvsmessage(), respectively 
    Scenario: a left client (source), => forwarder(sevrer_fwd, client_fwd), =>  a right server (destination)
    Scenario: a left client (source), <= forwarder(sevrer_rvs, client_rvs), <=  a right server (destination)

    Sockets: left incomming client socket (licsock), forward server socket (svrsock), forwarder client socket (cltsock)
    '''
    def __init__(self, ip, port, target_addr):
        self.svr = (ip, port)
        self.clt = target_addr
        print('Forwarder (server, client):', self.svr, self.clt)

        self.svrsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #forwarder server socket
        self.svrsock.bind(self.svr)

        self.cltsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #forwarder client socket
        self.cltsock.connect(self.clt)

        self.threads = []

        self.fwseq = 0
        self.rvseq = 0

        self.delay = 0
        self.qsize = 10

    def run(self):
        fwQ = Queue(self.qsize)
        rvQ = Queue(self.qsize)
        while True:
            self.svrsock.listen(5)
            try:
                (conn, (ip, port)) = self.svrsock.accept()
            except ConnectionResetError or KeyboardInterrupt:
                print('Forwarder connection lost')
                exit()

            self.licsock = conn     #left incoming client socket

            newthread = threading.Thread(target= self.server_fwd, args=(fwQ,))
            newthread.start()       #run server receive thread
            self.threads.append(newthread)

            newthread = threading.Thread(target=self.client_fwd, args=(fwQ,))
            newthread.start()      #run client send thread
            self.threads.append(newthread)

            newthread = threading.Thread(target=self.client_rvs, args=(rvQ,))
            newthread.start()       #run client receive thread
            self.threads.append(newthread)

            newthread = threading.Thread(target=self.server_rvs, args=(rvQ,))
            newthread.start()       #run server send thread
            self.threads.append(newthread)

        for t in self.threads:
            t.join()

    ''' modules: server_fwd > fwQ >  client_fwd > client_rvs > rvQ >  server_rvs '''
    # step 1) server forward connection: receive from left client and put to fwQ
    def server_fwd(self, Q): 
        while True:
            data = self.licsock.recv(BUFFER_SIZE)
            print('server rx from left incomming client', data)
            if data:
                rx = json.loads(data.decode('utf-8'))
                fx = self.svr_fwdmessage(rx)
                if not Q.full():
                    Q.put(json.dumps(fx).encode('utf-8'))
                    #print('fwd queue ', Q.queue)
                else:
                    print('fwd queue is full')
                    exit()
                ''' self.loopback() '''
            time.sleep(self.delay)

    # step 2) client forward connection: take data from Q to the right hand server
    def client_fwd(self, Q):
        while True:                                                     #perpetual or one time
            #print('forwader queue by clt', Q.queue)#qsize())
            if  not Q.empty():
                data = Q.get()
            else:
                data = b''
            #print('client rx from Q', data)
            try:
                self.cltsock.sendall(data)
            except:
                print('client cannot send right server')
                exit()
            time.sleep(self.delay)

    # step 3) client reverse connection: receice data from right server and put to rvsQ
    def client_rvs(self, Q): #self.cltsock.sendall(json.dumps(tx).encode('utf-8'))
        while True:
            data = self.cltsock.recv(BUFFER_SIZE)                         #receive #print('Received', repr(data))
            print('client rx from right incomming server', data)
            if data:
                rx = json.loads(data.decode('utf-8'))         #tx = self.message_handler(rx)
                tx = self.clt_rvsmessage(rx)
            if not Q.full():
                Q.put(json.dumps(tx).encode('utf-8'))
                #print('rvs queue', Q.queue)
            else:
                print('rvs queue is full')
                exit()
            time.sleep(self.delay)

    # step 4) server reverse connection: take data from rvsQ and send back to left client (the source client)
    def server_rvs(self, Q):
        while True:
            if not Q.empty():
                data = Q.get() #rx = json.loads(data.decode('utf-8'))
            else:
                data = b''
            #print('server rx from Q', data)
            try:  #self.licsock.sendall(json.dumps(tx).encode('utf-8'))
                self.licsock.sendall(data)
            except:
                print('server cannot transmit to left client')
                exit()
            time.sleep(self.delay)

    #message handler for server
    def svr_fwdmessage(self,rx):
        tx = copy.deepcopy(rx)
        tx['fwd'] = self.fwseq
        self.fwseq += 1
        return tx
    #message handler for client 
    def clt_rvsmessage(self,rx):
        tx = copy.deepcopy(rx)
        tx['rvs'] = self.rvseq
        self.rvseq += 1
        return tx

    #for test purpose only: look back the forwarder server message to source client
    def loopback(self):
        tx = self.clt_rvsmessage(rx)
        try:                        #response to client 
            self.licsock.sendall(json.dumps(tx).encode('utf-8'))
        except:
            print('server cannot transmit')
#-----------------------bidrectional forwarder -----------------------------

if __name__ == '__main__':
    host=''
    host=HOST
    port =60000
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if '-svr' in sys.argv:
        print("ipv4:", host, port)
        server = Server(host, port)
        server.run()
    elif '-clt' in sys.argv:
        if CLOG:
            sys.stdout= open("file.log", "w")
        client = Client(host, port)
        client.run()
    elif '-fwd' in sys.argv:
        if len(sys.argv) > 2:
            port = int(sys.argv[2])
        server = Forwarder(host, port,(host, port+1))
        server.run()
    elif '-usvr' in sys.argv:
        udpserver(host, port)
    elif '-uclt' in sys.argv:
        udpclient(host, port)

#-------------------3 point system: 4 servers and 4 clients -------------------
    elif '-ldev' in sys.argv:
        ldevice = Device(host, port, (host, port+1))
        ldevice.run()
    elif '-bfwd' in sys.argv:
        bifwd = BiForwarder(host, port+1, port+2, (host,port), (host,port+3))
        bifwd.run()
    elif '-rdev' in sys.argv:
        rdevice = Device(host, port+3, (host, port+2))
        rdevice.run()
    else:
        print('usage: python serverclient.py -svr/-fwd/-clt/-usvr/-uclt/-ldev/-bfwd/-rdev none|port')
        #tcpeserver(host, port)
        #tcpeclient(host,port)
    #    print('usage: python serverclient.py -svr/-fwd/-clt none|port')
