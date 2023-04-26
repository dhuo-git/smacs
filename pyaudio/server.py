# This is server code to send video and audio frames over TCP
'''
Server  runs in a thread

1) listen to host_ip, port-1
2) upon reception of client message (tbd)
3) send audio stream to the client as a sequence of CHUNKs, each of 1024 bytes

'''
import socket
import threading, wave, pyaudio,pickle,struct

host_name = socket.gethostname()
#host_ip = '192.168.1.102'#  socket.gethostbyname(host_name)
host_ip = 'localhost'#  socket.gethostbyname(host_name)
host_ip = '192.168.1.9'#  socket.gethostbyname(host_name)
print(host_ip)
port = 9611

print("host, port", host_ip, port-1)

def audio_stream():
    server_socket = socket.socket()
    server_socket.bind((host_ip, (port-1)))

    server_socket.listen(5)
    CHUNK = 1024
    #wf = wave.open("temp.wav", 'rb')
    wf = wave.open("sample1.wav", 'rb')
    
    p = pyaudio.PyAudio()
    print('server listening at',(host_ip, (port-1)))
   
    
    stream = p.open(format=p.get_format_from_width(wf.getsampwidth()),
                    channels=wf.getnchannels(),
                    rate=wf.getframerate(),
                    input=True,
                    frames_per_buffer=CHUNK)

             

    client_socket,addr = server_socket.accept()
    print("from client", addr)

    data = None
    while True:
        if client_socket:
            while True:
              
                data = wf.readframes(CHUNK)
                a = pickle.dumps(data)
                message = struct.pack("Q",len(a))+a
                client_socket.sendall(message)
                
t1 = threading.Thread(target=audio_stream, args=())
t1.start()


