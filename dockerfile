FROM ubuntu:latest
WORKDIR /code
ADD . /code

RUN apt update
RUN apt install python3 -y
RUN apt-get install -y python3-pip

COPY python_file.py ./
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt