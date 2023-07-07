/network/different from test_hub

python (or python3)
	hub.py -fwd  (start hub)
	pub.py 
	sub.py (-ex using exernal buffer) 

	producer.py (use pub.py)
	consumer.py (use sub.py)
	media.py (pub.py -> sub.py)
test:

	1) 
	python3 hub.py -fwd

	2)
	python3 pub.py
	python3 sub.py

	3)
	python3 medium.py
	python3 producerm.py
	python3 consumerm.py

	4)
	python3 hub.py -fwd
	python3 consumer.py
	python3 producer.py
	python3 controller.py
	
github.com/dhuo-git/traffic-control


pubm.py, subm.py, producerm.py, consumerm.py, mediam.py 
contain multiple buffers in stead of single buffer, where mediam.py not quite work yet, cannot send.

git add *.py, git reset
git commit -m 'text'
git push
git pull

git remote rm <remote-name>

-----
producer.py, consumer.py, controller.py, medium.py
-----
https://www.mongodb.com/docs/compass/master/install/

wget https://downloads.mongodb.com/compass/mongodb-compass_1.36.4_amd64.deb
sudo dpkg -i mongodb-compass_1.36.4_amd64.deb
mongodb-compass
