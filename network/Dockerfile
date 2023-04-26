# syntax=docker/dockerfile:1
#FROM node:12-alpine
#RUN apk add --no-cache python2 g++ make
#WORKDIR /app
#COPY . .
#RUN yarn install --production
#CMD ["node", "src/index.js"]
#EXPOSE 3000
#RUN mkdir home && cd home
#COPY serverclient.py /home/serverlient.py

#FROM python:3.9
#WORKDIR /home
#ADD serverclient.py /home/serverclient.py 
#ENTRYPOINT ['python', '/home/serverclient.py', '-svrq']
#RUN python /home/serverclient.py -svrq
FROM ubuntu
RUN apt-get update -y && \
apt-get install -y build-essential && \
apt-get clean && \
apt-get install -y iproute2 && \
apt-get install -y iputils-ping && \
apt-get install -y net-tools && \
apt-get install -y git &&\
apt-get install python3 -y &&\ 
apt-get update -y &&\
apt-get install python3-zmq -y &&\
apt-get install vim -y
WORKDIR /home/test_hub
COPY . /home/test_hub
ENTRYPOINT ["python3", "hubtest.py"]
CMD ["0"]

#ENTRYPOINT ["python3", "hub.py"]
#CMD ["-fwd"]
#ENTRYPOINT ["./test.sh"]
#ENTRYPOINT ["python3", "hub.py","-fwd"]
#docker build -t testhub .
#docker insepct -f '{{ .NetworkSettings.IPAddress }}' containerId
#docker container logs containerId
#docker exec -it containerId bash
#ENTRYPOINT ["/bin/bash"]
