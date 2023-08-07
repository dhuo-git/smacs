"""
mcast.py

test multicast with queues, and selected collections.deque instaed of queue.Queue
one client and two servers

Jul 29, 2023 /nj
"""
import time,sys
import zmq
from threading import Thread
from queue import Queue
#rxq=[Queue(10), Queue(10)]
from collections import deque

def server(ip, port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)        #tx/rx mutex
    #socket = context.socket(zmq.PAIR)      #no-mutex
    socket.bind("tcp://{}:{}".format(ip,  port))
    while True:
        message = socket.recv_json()
        print(f"Server at {port} Received : ", message)
        socket.send_json(message)
    socket.close()
    context.term()

txq=deque(maxlen=10)
rxq=deque(maxlen=10) #Queue(10)           #for server 1
rxq1=deque(maxlen=10) #Queue(10)          #for server 2
maxlen =10

def client(ip, port):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    #socket = context.socket(zmq.PAIR)

    socket1 = context.socket(zmq.REQ)
    #socket1 = context.socket(zmq.PAIR)

    socket.connect("tcp://{}:{}".format(ip, port))
    socket1.connect("tcp://{}:{}".format(ip, port+1))   #-------------------

    request = {}        #necessary and reduces warm up rounds to 1
    message = {}
    for i in range(10): #while True:
        if len(txq) >0:
            request = txq.popleft()
            try:
                socket.send_json(request) #{'user':1, 'data':"Hello 1"})
                socket1.send_json(request) #{'user':1, 'data':"Hello 2"})
                print(f"Client {i}  Sent", request)
            except:
                txq.appendleft(request)
                print(f"Client {i} sent no packet")
        #receve from server 1`
        
        if len(rxq) <maxlen:
            try:
                message = socket.recv_json()
                rxq.append(message)
                print(f"Client 1, round {i}, Received ",  message)
            except:
                print(f"Client 1, round {i}, received no message")
        #receive from server 2
        if len(rxq1) <maxlen:
            try:
                message1= socket1.recv_json()
                rxq1.append(message1)
                print(f"Client 2, round {i}, Received ",  message)
            except:
                print(f"Client 2, round {i}, received no message")


        """
        if not txq.empty():
            request = txq.get()
            socket.send_json(request) #{'user':1, 'data':"Hello 1"})
            socket1.send_json(request) #{'user':1, 'data':"Hello 2"})
        #receve from server 1`
        if not rxq.full():
            message = socket.recv_json()
            rxq.get(message)
        #receive from server 2
        if not rxq1.full():
            message1= socket1.recv_json()
        """
    socket.close()
    socket1.close()
    context.term()
    exit()

def source():
    for i in range(10): 
        if len(txq)< maxlen: txq.append(i+1)
    return

def multicast():
    seq = 0
    message ={}
    while True:
        if len(txq) < maxlen:
            txq.append({'server': 0, 'seq': seq, 'received': time.ctime()})
            seq += 1
        #time.sleep(1)
        if len(rxq)>0: 
            message = rxq.popleft()
        if len(rxq1) >0:
            message1 = rxq1.popleft() 
            print('Multicaster received', message, message1)
    """
        if not txq.full(): 

            txq.put({'server': 0, 'seq': seq, 'received': time.ctime()})
            seq += 1
        #time.sleep(1)
        if not rxq.empty():
            message = rxq.get()
            message1 = rxq1.get()
            print('received', message, message1)
        """

    exit()
if __name__ == "__main__":
    print('usage: python3 reqrep.py -svr/clt')
    print(sys.argv)
    ip = "127.0.0.1"
    port = 5555
    if '-svr' in sys.argv:      #server0
        server(ip, port)
    if '-svr1' in sys.argv:     #server1 for individual test of multicast
        server(ip, port+1)
    elif '-clt' in sys.argv:    #one-to-one client
        client(ip, port)
    elif '-mut' in sys.argv:    #multicast client (for 2 servers)
        multicast()
    else:
        thr= [Thread(target=server, args=(ip,port,)), Thread(target=server, args=(ip, port+1,)), Thread(target=client, args=(ip, port,)), Thread(target=multicast)]
        for t in thr:
            t.start()
        for t in thr:
            t.join()
