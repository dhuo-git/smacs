History:
 2005  pip -m install virtualenv --user virtualenv
or
python -m pip install --user virtualenv
virtualenv venv
source venv/bin/activate
deactivate
source venv/bin/activate

sudo apt-get install portaudio19-dev
sudo apt-get install python3-all-dev
[Do:
sudo apt install python3-pyaudio
=> cannot find pyaudio in python3 shell]

Preferred:

pip install pyaudio

python3 api-dev.py (to check it works)
----------
Check: https://people.csail.mit.edu/hubert/pyaudio/
it says to make sure that portaudio19-dev and python3-all-dev
are installed. Do:



Redo:
pip install pyaudio (complete and in python3 shell pyaudio can be imported)

[ 
Option in case of need: Get an audio recorder:

sudo add-apt-repository ppa:audio-recorder/ppa
sudo apt install audio-recorder
sudo add-apt-repository --remove ppa:audio-recorder/ppa
sudo apt remove --autoremove audio-recorder
]

Check examples: https://people.csail.mit.edu/hubert/pyaudio/
and explanation: https://people.csail.mit.edu/hubert/pyaudio/docs/
also related:
https://peps.python.org/pep-0572/
https://en.wikipedia.org/wiki/Callback_(computer_programming)
dpkg -l |grep alsa
https://alsa.opensrc.org/HowTo_Asynchronous_Playback

Check api and devices: api-dev.py

(venv) dhuo@system76-pc:~/Work/Audio$ ls
api-dev.py           play1.py          record1.py   wire-callback.py
audio-recording.wav  play-callback.py  record.py    wire.py
ChannelMaps.py       play.py           sample1.wav
output.wav           README.md         venv
(venv) dhuo@system76-pc:~/Work/Audio$ 

transport:
https://pyshine.com/How-to-send-audio-from-PyAudio-over-socket/

server.py and client.py should be on different computer, so that they don't compete with hardware resource

