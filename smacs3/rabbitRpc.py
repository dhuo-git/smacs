#!/usr/bin/env python
import pika
import uuid, sys
import json, time, os
"""
rabbitrpc.py <-../tech/reqrep.py <-../tech/rpc.py
RPC: Remote Process Call
client send (publish) message and expect acknowledgement (consume) 
server respond (consume) message and send-back(publish) acknowledgement

client uses default exchange: direct, sends  on defualt queue '', receives on queue 'rpc_queue'(routing key)
client uses default exchange: direct, receives on defualt queue '', send response to queue 'rpc_queue'(routing key)

created 8/13/2023/dh, 
"""
ip = '192.168.1.99'
#rpc server (consumer)
class RpcServer(object):
    """
    Server side of RPC via RabbitMQ server, parameters: ipv4=ip address, queue_name=queue (must be same as in Client)
    """
    def __init__(self, ipv4,queue_name='rpc_queue'):
        self.connection = pika.BlockingConnection( pika.ConnectionParameters(host=ipv4))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def on_request(self, ch, method, props, body):
        response = self.svr_handler(body)

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = props.correlation_id),
                         body=response) 
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def call(self):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_request)
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('Interrupted')
            close()

    #to be overrriden by children
    def svr_handler(self, rx_bytes):
        if rx_bytes:
            rx_string = rx_bytes.decode()
            rx_json = json.loads(rx_string)
            print('server rx:',  rx_json)
            if isinstance(rx_json, dict):
                tx_json = {'seq': rx_json['seq'], 'svr_time': time.ctime(), 'ack': rx_json['clt_time']}
                tx_string= json.dumps(tx_json)
            else:
                print('invalid packet received by server', rx_json)
                os.exit(0)
        else:
            tx_string= ''#json.dumps({'seq':self.seq})
        print('server tx:', tx_string)

        return tx_string.encode()
        
#rpc client (producer)
class RpcClient(object):
    """
    Client side of RPC via RabbitMQ, parameters: ipv4=ip address, queue_name=queue (same as Server), cdly=client delay (seconds for source flow control)
    """
    def __init__(self, ipv4, routing_key='rpc_queue', cdly=0):
        self.connection = pika.BlockingConnection( pika.ConnectionParameters(host=ipv4))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.routing_key= routing_key 
        self.channel.basic_consume( queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

        self.response = None
        self.corr_id = None
        self.seq = 0
        self.cdly = cdly

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body #response from server 


    def call(self, tx:bytes)->bytes:
        #self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key=self.routing_key, properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id,), body=tx)  
        self.connection.process_data_events(time_limit=None)
        return self.response

    #to be overriden by children: rx=bytes, return tx=bytes
    def clt_handler(self, rx_bytes: bytes)-> bytes:
        if rx_bytes:
            rx_string= rx_bytes.decode()
            rx_json= json.loads(rx_string)
            print('client rx:',  rx_json)
            if isinstance(rx_json, dict):
                tx_json={'seq': self.seq, 'clt_time': time.time(), 'ack': rx_json['svr_time']}
                self.seq += 1
                tx_string = json.dumps(tx_json)
            else:
                print('invalid packet received by client', rx_json)
                close()
        else:#first packet
            tx_string = json.dumps({'seq':0, 'clt_time': time.time()})
            self.seq += 1
        print('client tx:', tx_string)
        return tx_string.encode() #bytes

    #rounds>0: finite loop, rouds=-: infinite loop
    def loop(self, rounds=0):
        message = self.clt_handler(None) #bytes
        try:
            if rounds:
                for _ in range(rounds):
                    #print(f" [x] Requesting {message}")
                    response = self.call(message)#str(i)).decode()
                    message = self.clt_handler(self.response)
                    #print(f" [.] Got {response}")
                    time.sleep(self.cdly)
            else:
                active = True
                while active:
                    response = self.call(message)
                    message = self.clt_handler(response)
                    time.sleep(self.cdly)
            close()
        except KeyboardInterrupt:
            print('Interrupted')
            active = False
            close()

def close():
    print('closing...')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
            
#----------------------TEST ------------------------
if __name__ == '__main__':
    print(sys.argv)
    if '-svr' in sys.argv:
        s =  RpcServer(ip, 'rpc_key')
        s.call()
    elif '-clt' in sys.argv:
        s =  RpcClient(ip, 'rpc_key')
        s.loop(10)
    elif '-svrclt' in sys.argv:
        from threading import Thread
        svr = RpcServer(ip, 'rpc_key')
        clt = RpcClient(ip, 'rpc_key', 1)
        try:
            thr=[Thread(target=svr.call), Thread(target=clt.loop, args=(0,))]
            for t in thr:
                t.start()
            for t in thr:
                t.join()
        except KeyboardInterrupt:
            close()
    else:
        print('usage: python3 rabbitrpc.py -svr/clt/svrclt')
