in /path/to/smacs3/docker/

cp ../*.py src/
docker build -t smacs3 .

configure within smacs3 container
run exec -it <container-id> bash
docker run -it smacs3


prereuisites:
(docker run -d rabbitmq (browse: http://172.17.0.2:15672/#, if this lauched first)
docker run -d mongodb  (addres: 172.17.0.3, if this launched second))
or
(1) in rabbitmq/
(2) in mongodb/

docker-compose up -d
docker-compose dn

find ip address of rabbitmq and mongodb
docker inspect <container-id>

(
make change within smacs3
/cod/rrRcontr.py
hub_ip=rabbitmq_ip
db_ip= mongodb_ip
apt update
apt install vim -y
apt install net-tools -y
apt install iputils-ping -y
mkdir tmp
change ports to [5555,5554]
) added to Dockerfile

Reference:
https://www.docker.com/blog/containerized-python-development-part-1/
