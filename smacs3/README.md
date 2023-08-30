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

if external source/sink
python3 srcsnk.py -svr/clt/svrclt

3) c-plane
python3 rrRcons.py 1/0
python3 rrRprod.py 1/0
python3 rrRcontr.py 1/0

4) u+c-plane
python3 rrRcons.py 3
python3 rrRprod.py 3
python3 rrRcontr.py 3

if external source/sink
python3 srcsnk.py -svr/clt/svrclt


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


------Docker usage-----
docker run -d --rm --name rabbit dhuo/rabbitmq
docker run -d --rm --name mongo dhuo/mongo
docker run -it --rm --name mnode dhuo/smacs3
docker exec -it mnode bash
docker exec -it mnode bash

(in last 3 terminas do 
python3 rrRcons.py mode (0,1,2,3)
python3 rrRprod.py mode
python3 rrRcontr.py mode)

docker run --rm -it --user=$(id -u --env="DISPLAY" --volume="/etc/group:/etc/group:ro" --volume="/etc/passwd:/etc/passwd:ro" --volume="/etc/shadow:/etc/shadow:ro" --volume="/etc/sudoers.d:/etc/sudoers.d:ro" --volume="/tmp/.X11-unix:/tmp/.X11-unix:rw" dhuo/analytics python3 analysis.py -exp1

or in local
python3 analysis.py -exp1/2 (from mode 1/3)


