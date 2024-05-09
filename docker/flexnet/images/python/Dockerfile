FROM python:3.12.2-slim

WORKDIR /monad

RUN apt update
RUN apt install -y build-essential

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
