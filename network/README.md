/test_hub/ testing hub with pub + sub
hub.py, pub.py, sub.py for local test

python (or python3)
	hub.py -fwd  (start hub)
	pub.py [-loop] (start pub as loop or single action)
	sub.py [-loop] (start sub as loop or single action)

	sender.py (using pub.py)
	receiver.py (using sub.py)
	medium.py

shell script
./test.sh  (test case 0,1,2 in unittest format)

Dockerfile [hub] includes
ENTRYPOINT['python3', 'hub.py']
CMD ['-fwd']
Dockerfile [pubsub] includes
ENTRYPOINT['python3', 'hubtest.py']
CMD ['0']
CMD ['1']
CMD ['2']


Note: 
ENTRYPOINT['./test.sh'] should start unitttest ut.py and output all tests, but has issue when done within a container. One can rewrite test.sh to include: docker run testhub $1(0,1,2) instead

Note:pub, sub and hub modules in hub.py, all share the same Ip addreess)
Note:pub, sub in hubtest.py and hub modules may have different Ip addreesses)

Note: consistent AUOTGET (False, True) setting in Docker-Container or in Host
Communication between Docker-Container and Host

Scenario 1:
In docker: hub.py can have ip=0.0.0.0.0, container-ip=172.17.0.2
In host: hutest.py has ip=172.17.0.2 (works)

Scenario 2:
In host: hub.py has ip=0.0.0.0, while host-docker-bridge-ip: docker0=172.17.0.1
In docker: hubtest.py has ip=172.17.0.1, while container-ip=172.17.0.x for x>1

Remeber to remove container after usage: docker rm $(docker ps -aq)

