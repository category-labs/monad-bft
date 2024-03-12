# python dev environment
FROM python:3.12.2-slim

WORKDIR /monad-dev

RUN apt update && apt install -y build-essential swig pkg-config sudo

ARG UID=1025
ARG GID=1025
ARG UNAME=

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME
RUN adduser $UNAME sudo

USER $UNAME

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
