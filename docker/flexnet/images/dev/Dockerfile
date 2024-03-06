# python dev environment
FROM python:3.12.2-slim

RUN apt update && apt install -y build-essential swig pkg-config

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
