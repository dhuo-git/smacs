# set base image (host OS)
FROM python:3.10

# set the working directory in the container
WORKDIR /code

# copy the dependencies file to the working directory

# install dependencies
#RUN pip install -r requirements.txt
RUN apt update
RUN apt install vim -y
RUN apt install net-tools -y
RUN apt install iputils-ping -y

RUN pip install pyzmq
RUN pip install pika
RUN python3 -m pip install pymongo
RUN mkdir tmp

# copy the content of the local src directory to the working directory
COPY ./src .

# command to run on container start
#CMD [ "python3", "/code/rrRcons.py", "0"] 
