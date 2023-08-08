@8/8/2023

/smacs2/

prerequisite

sudo systemctl start mongod
python3 hub.py -fwd

dependency:

rrcons.py depends on cons.py
rrprod.py depends on prod.py

usage:

python3 rrcons.py mode
python3 rrprod.py mode
python3 rrcontr.py mode


tests:----------------------------

1) multicast
python3 rrcons.py -svr
python3 rrprod.py -svr
python3 rrcontr.py -clt

or 
python3 mcast.py (in 3 threads) or
python3 mcast.py -svr
python3 mcast.py -svr1
python3 mcast.py -clt

2) u-plane
python3 hub.py -fwd
python3 cons.py 2
python3 prod.py 2
or
python3 rrcons.py 2
python3 rrprod.py 2


3) c-plane
sudo systemctl start mongod
python3 hub.py -fwd
python3 rrcons.py 1
python3 rrprod.py 1
python3 rrcontr.py 1

4) u+c-plane
sudo systemctl start mongod
python3 hub.py -fwd
python3 rrcons.py 3
python3 rrprod.py 3
python3 rrcontr.py 3


5) analytics(view statistics)
mongosh

or

../analysis/
python3 analysis.py -clock
python3 analysis.py -all
python3 analysis.py -exp1
python3 analysis.py -exp2

6) extetrnal source/sink:
https://gist.github.com/140am/ca661b9a4fca550f9554


