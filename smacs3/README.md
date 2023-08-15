@8/15/2023

/smacs3/

prerequisite

sudo systemctl start mongod
sudo systemctl start rabbitmq-server (or docker container)

dependency:

rrRcons.py depends on Rcons.py
rrRprod.py depends on Rprod.py

usage:

python3 rrRcons.py mode
python3 rrRprod.py mode
python3 rrRcontr.py mode


tests:----------------------------

1) multicast
python3 rrRcons.py -svr
python3 rrRprod.py -svr
python3 rrRcontr.py -clt


2) u-plane
python3 Rcons.py 2/3
python3 Rprod.py 2/3


3) c-plane
python3 rrRcons.py 1/0
python3 rrRprod.py 1/0
python3 rrRcontr.py 1/0

4) u+c-plane
python3 rrRcons.py 3
python3 rrRprod.py 3
python3 rrRcontr.py 3


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


