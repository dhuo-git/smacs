/network/different from test_hub

python (or python3)
	hub.py -fwd  (start hub)
	pub.py 
	sub.py (-ex using exernal buffer) 

	producer.py (using pub.py)
	consumer.py (using sub.py)
	media.py (on pub.py, sub.py)
test:
	1) 
	python3 hub.py -fwd

	2)
	python3 pub.py
	python3 sub.py

	3)
	python3 media.py
	python3 producer.py
	python3 consumer.py

github.com/dhuo-git
