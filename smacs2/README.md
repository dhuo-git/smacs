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


tests:

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
python3 cons.py 2
python3 prod.py 2



